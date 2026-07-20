#!/usr/bin/env python3
"""
Dallas Intel â€” Motivated Seller Lead Scraper
=============================================
Sources
  NOFC     â€” Trustee sale / foreclosure notices  (Dallas County Clerk PublicSearch)
  CODE     â€” Code violations                      (Dallas OpenData Socrata API)
  TAXSALE  â€” Tax-delinquent sheriff sale list     (LGBS / dallascounty.org)
  LP       â€” Lis pendens PDF upload               (drag-and-drop on dashboard)
  BK       â€” Bankruptcy filings                   (CourtListener, Northern Dist TX)

Parcel enrichment via DCAD ArcGIS MapServer
Daily run via GitHub Actions â†’ GitHub Pages dashboard

FIXES APPLIED
  1. DCAD address mismatch  â€” streets now uppercased + common suffixes normalized
                              (e.g. "Boulevard" â†’ "BLVD") before querying DCAD.
                              Also removed the [:20] slice that cut street names mid-word.
  2. BK infinite loop guard â€” date-chunking restart now stops if oldest date already
                              reached the lookback cutoff, preventing an endless loop.
  3. BK speed (2hr â†’ ~25min)â€” page_size raised from 10 â†’ 50; docket sub-fetches run
                              4 at a time with ThreadPoolExecutor instead of one-by-one.
  4. CODE sort field typo   â€” violations dataset sort changed from "updated DESC"
                              (doesn't exist) to "created DESC" (the real field).
  5. Scorer WEEK_AGO freeze â€” was computed once at import time; now computed fresh
                              inside score() so it's always accurate.
  6. Scorer O(nÂ²) fix       â€” ownerâ†’categories index built once before scoring loop
                              instead of scanning all records for every single record.
"""

import csv
import json
import logging
import os
import re
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS       = int(os.getenv("LOOKBACK_DAYS", "30"))
LOOKBACK_DATE       = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
COURTLISTENER_TOKEN = os.getenv("COURTLISTENER_TOKEN", "")

# ---------------------------------------------------------------------------
# Data source URLs
# ---------------------------------------------------------------------------

DALLAS_CODE_URL     = "https://www.dallasopendata.com/resource/gc4d-8a49.json"
DALLAS_311_URL      = "https://www.dallasopendata.com/resource/dkp4-ix7s.json"
DALLAS_PUBLICSEARCH = "https://dallas.tx.publicsearch.us"
LGBS_TAXSALE_URL    = "http://taxsales.lgbs.com/dallas/list"
DCAD_URL            = ("https://maps.dcad.org/prdwa/rest/services/"
                       "Property/ParcelQuery/MapServer/4/query")
CL_BK_URL           = "https://www.courtlistener.com/api/rest/v4/bankruptcy-information/"
CL_BK_COURT         = "txnb"

# ---------------------------------------------------------------------------
# LP PDF column layout
# ---------------------------------------------------------------------------
_LP_COLS = {
    "grantor": (0,   102),
    "grantee": (102, 205),
    "date":    (269, 328),
    "docnum":  (328, 407),
    "parcel":  (495, 582),
    "address": (582, 693),
}
_LP_CUTOFF = datetime(2026, 1, 1)

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
RECORDS_JSON  = DATA_DIR / "records.json"
DASH_JSON     = DASHBOARD_DIR / "records.json"
GHL_CSV       = DATA_DIR / "ghl_export.csv"
LP_PDF_PATH   = DATA_DIR / "lp_export.pdf"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": "DallasLeadScraper/1.0"})
    return s


# FIX 1 â€” street suffix normalization for DCAD address matching.
# DCAD stores streets like "ELM BLVD" but our addresses often say "Elm Boulevard".
# This map converts the long form to the short form DCAD uses.
_STREET_SUFFIX_MAP = {
    "BOULEVARD": "BLVD", "AVENUE": "AVE",   "STREET": "ST",
    "DRIVE":     "DR",   "ROAD":   "RD",    "LANE":   "LN",
    "COURT":     "CT",   "PLACE":  "PL",    "CIRCLE": "CIR",
    "TRAIL":     "TRL",  "PARKWAY":"PKWY",  "HIGHWAY":"HWY",
    "TERRACE":   "TER",  "SQUARE": "SQ",    "LOOP":   "LOOP",
}

def _normalize_street_for_dcad(street: str) -> str:
    """Uppercase the street and replace any long suffix with DCAD's short form."""
    parts = street.upper().split()
    if parts:
        parts[-1] = _STREET_SUFFIX_MAP.get(parts[-1], parts[-1])
    return " ".join(parts)


# ===========================================================================
# Source: Code Violations â€” Dallas OpenData Socrata
# ===========================================================================

class DallasCodeScraper:
    """
    Pulls code violations from Dallas OpenData Socrata API.
    Dataset x9pz-kdq9 â€” updated daily, ~80k total records.
    """

    SEVERITY = {
        "Structural": "high",
        "Nuisance Abatement": "high",
        "Substandard Structure": "high",
        "Zoning": "medium",
        "Other": "low",
    }

    HIGH_PRIORITY = {"Emergency", "Priority", "High"}
    INCLUDE_TYPES = (
        "Code Concern - CCS",
        "Single Family Rental Needs Registration - CCS",
        "Boarding Home Complaint - CCS",
        "Boarding Home Facilities - CCS",
        "Emergency Regulations Violation - CCS",
        "Illegal Dumping Sign - CCS",
    )

    def fetch(self) -> list:
        log.info("Scraping Dallas code violations (Socrata)...")
        session  = make_session()
        all_rows = []
        offset   = 0
        limit    = 1000
        where    = (
            "service_request_type in("
            + ",".join(f"\'{t}\'" for t in self.INCLUDE_TYPES)
            + ")"
        )

        while True:
            try:
                params = {
                    "$limit":  limit,
                    "$offset": offset,
                    "$order":  "created_date DESC",
                    "$where":  where,
                }
                r = session.get(DALLAS_CODE_URL, params=params, timeout=30)
                if r.status_code != 200:
                    log.warning("Socrata CODE HTTP %d - %s", r.status_code, r.text[:200])
                    break
                batch = r.json()
                if not batch:
                    break
                all_rows.extend(batch)
                if len(batch) < limit or offset >= 20000:
                    break
                offset += limit
                time.sleep(0.3)
            except Exception as e:
                log.warning("Socrata CODE error: %s", e)
                break

        log.info("Code violations fetched: %d rows", len(all_rows))

        from collections import Counter
        addr_counts = Counter(
            (row.get("address") or "").strip().upper()
            for row in all_rows if row.get("address")
        )

        records = []
        for row in all_rows:
            priority = (row.get("priority") or "Standard").strip()
            addr     = (row.get("address") or "").strip().upper()
            repeat   = addr_counts.get(addr, 0) >= 2

            if priority == "Standard" and not repeat:
                continue

            rec = self._to_record(row, priority, repeat)
            if rec:
                records.append(rec)

        log.info("Code violations total: %d records (after distress filtering)", len(records))
        return records

    def _to_record(self, row: dict, priority: str = "Standard", repeat: bool = False) -> Optional[dict]:
        address = (row.get("address") or "").strip().title()
        if not address:
            return None

        stype   = (row.get("service_request_type") or "Other").strip()
        status  = (row.get("status") or "").strip()

        # Determine severity from priority field
        if priority in self.HIGH_PRIORITY:
            sev = "high"
            cat = "CODE_STRUCT"
            cat_label = "Substandard Structure"
        elif repeat:
            sev = "medium"
            cat = "CODE_STRUCT"
            cat_label = "Repeat Violation"
        else:
            sev = "low"
            cat = "CODE"
            cat_label = "Code Violation"

        # Build distress flags
        flags = []
        if priority == "Emergency":
            flags.append("Emergency priority")
        elif priority == "Priority":
            flags.append("High priority")
        if repeat:
            flags.append("Repeat violation")
        if "rental" in stype.lower():
            flags.append("Unregistered rental")
        if "boarding" in stype.lower():
            flags.append("Boarding home")
        if "dumping" in stype.lower():
            flags.append("Illegal dumping")

        filed_raw = (row.get("created_date") or row.get("update_date") or "")
        filed = ""
        if filed_raw:
            try:
                filed = __import__("datetime").datetime.fromisoformat(filed_raw[:10]).strftime("%m/%d/%Y")
            except Exception:
                filed = filed_raw[:10]

        # Parse city/zip from address field e.g. "1234 MAIN ST, DALLAS, TX, 75201"
        prop_city = "Dallas"
        prop_zip  = ""
        parts = address.split(",")
        if len(parts) >= 3:
            prop_city = parts[1].strip().title()
        if len(parts) >= 4:
            prop_zip = parts[3].strip()

        return {
            "doc_num":       str(row.get("service_request_number") or row.get("unique_key") or ""),
            "doc_type":      f"CODE-{priority.upper()[:4]}",
            "cat":           cat,
            "cat_label":     cat_label,
            "filed":         filed,
            "owner":         "",
            "grantee":       "",
            "amount":        None,
            "legal":         "",
            "clerk_url":     "",
            "prop_address":  parts[0].strip().title() if parts else address,
            "prop_city":     prop_city,
            "prop_state":    "TX",
            "prop_zip":      prop_zip,
            "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":        "Dallas Code Enforcement",
            "neighborhood":  (row.get("city_council_district") or ""),
            "viol_status":   status,
            "viol_desc":     f"{stype} [{priority}]{'  REPEAT' if repeat else ''}",
            "viol_severity": sev,
            "delinquent":    False, "delinq_amt": "",
            "homestead":     None,
            "appraised":     "",
            "out_of_state":  False,
            "luc":           "", "luc_desc": "",
            "score":         0,
            "flags":         flags,
        }


# ===========================================================================
# Source: NOFC + LP -- Dallas County Clerk PublicSearch
# ===========================================================================

class DallasPublicSearchScraper:
    BASE       = "https://dallas.tx.publicsearch.us"
    SEARCH_URL = "https://dallas.tx.publicsearch.us/results"
    DOC_URL    = "https://dallas.tx.publicsearch.us/doc/{}"

    SEARCHES = [
        ("trustee sale", "NOFC", "Notice of Foreclosure"),
        ("foreclosure",  "NOFC", "Notice of Foreclosure"),
        ("lis pendens",  "LP",   "Lis Pendens"),
    ]

    def _make_session(self):
        s = make_session()
        s.headers.update({
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://dallas.tx.publicsearch.us/",
        })
        return s

    def fetch(self) -> list:
        log.info("Scraping Dallas County PublicSearch via Playwright (NOFC + LP)...")
        try:
            import asyncio as _asyncio
            from playwright.async_api import async_playwright as _apw
        except ImportError:
            log.error("Playwright not installed â€” run: pip install playwright && python -m playwright install chromium")
            return []

        cutoff     = LOOKBACK_DATE.date()
        start_str  = cutoff.strftime("%Y%m%d")
        end_str    = date.today().strftime("%Y%m%d")
        LIMIT      = 50
        BARGS      = ["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu"]

        async def _get_rows(page):
            return await page.evaluate("""() => {
                const out = [];
                for (const tr of document.querySelectorAll('tbody tr')) {
                    const cb = tr.querySelector('input[id^="table-checkbox-"]');
                    if (!cb) continue;
                    const id = cb.id.replace('table-checkbox-', '');
                    const c = i => { const e = tr.querySelector('.col-'+i+' span'); return e ? e.innerText.trim() : ''; };
                    out.push({id, g3:c(3), g4:c(4), g5:c(5), g6:c(6), g7:c(7), g9:c(9), g10:c(10)});
                }
                const m = document.body.innerText.match(/([0-9][0-9,]+)\s+results/);
                return {rows: out, total: m ? parseInt(m[1].replace(/,/g,'')) : 0};
            }""")

        async def _load_page(pw, url):
            for attempt in range(3):
                browser = await pw.chromium.launch(headless=True, args=BARGS)
                try:
                    ctx  = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                    page = await ctx.new_page()
                    await page.goto("https://dallas.tx.publicsearch.us/", wait_until="domcontentloaded", timeout=60000)
                    await _asyncio.sleep(2)
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await _asyncio.sleep(3)
                    for _ in range(27):
                        data = await _get_rows(page)
                        if data["rows"]: return data
                        await _asyncio.sleep(1)
                    return await _get_rows(page)
                except Exception as e:
                    log.warning("PublicSearch load attempt %d: %s", attempt+1, e)
                    await _asyncio.sleep(3)
                finally:
                    await browser.close()
            return {"rows": [], "total": 0}

        async def _scrape_one(pw, search_val, cat, cat_label):
            base = (f"https://dallas.tx.publicsearch.us/results"
                    f"?department=RP&searchType=quickSearch"
                    f"&searchValue={search_val.replace(' ', '+')}"
                    f"&recordedDateRange={start_str},{end_str}&limit={LIMIT}")
            seen, recs = set(), []
            total, offset, pnum = 9999, 0, 1
            while offset < total:
                data = await _load_page(pw, f"{base}&offset={offset}")
                if pnum == 1:
                    total = data["total"] or 9999
                    log.info("PublicSearch '%s' total=%d", search_val, total)
                batch = data["rows"]
                log.info("PublicSearch '%s' page %d rows=%d", search_val, pnum, len(batch))
                if not batch: break
                for r in batch:
                    key = r["g7"] or r["id"]
                    if key in seen: continue
                    seen.add(key)
                    # parse date
                    filed = ""
                    try: filed = datetime.strptime(r["g6"].strip(), "%m/%d/%Y").strftime("%m/%d/%Y")
                    except Exception: filed = r["g6"][:10] if r["g6"] else ""
                    # extract address from legal desc
                    address = ""
                    m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                                  r["g10"], re.I)
                    if m: address = m.group(1).strip().title()
                    recs.append({
                        "doc_num":      r["g7"] or r["id"],
                        "doc_type":     r["g5"],
                        "cat":          cat,
                        "cat_label":    cat_label,
                        "filed":        filed,
                        "owner":        r["g3"].strip().title(),
                        "grantee":      r["g4"].strip().title(),
                        "amount":       None,
                        "legal":        r["g10"][:200],
                        "clerk_url":    f"https://dallas.tx.publicsearch.us/doc/{r['id']}",
                        "prop_address": address,
                        "prop_city":    r["g9"].strip().title() or "Dallas",
                        "prop_state":   "TX",
                        "prop_zip":     "",
                        "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                        "source":       "Dallas County Clerk",
                        "neighborhood": "", "viol_status": r["g5"], "viol_desc": r["g5"],
                        "viol_severity": "high",
                        "delinquent":   False, "delinq_amt": "",
                        "homestead":    None,  "appraised":  "",
                        "out_of_state": False,
                        "luc":          "", "luc_desc": "",
                        "score":        0,  "flags":    [],
                    })
                offset += LIMIT; pnum += 1
                if offset < total: await _asyncio.sleep(2)
            return recs

        async def _run():
            all_recs = []
            async with _apw() as pw:
                for sv, cat, label in self.SEARCHES:
                    recs = await _scrape_one(pw, sv, cat, label)
                    log.info("PublicSearch '%s': %d records", sv, len(recs))
                    all_recs.extend(recs)
            # dedup by doc_num
            seen, out = set(), []
            for r in all_recs:
                if r["doc_num"] not in seen:
                    seen.add(r["doc_num"]); out.append(r)
            log.info("PublicSearch: %d records parsed from table", len(out))
            return out

        return _asyncio.run(_run())

    def _parse_api_item(self, item: dict, cat: str, cat_label: str) -> Optional[dict]:
        """Parse a JSON item from the PublicSearch API."""
        try:
            doc_id   = str(item.get("id") or item.get("docId") or item.get("documentId") or "")
            doc_num  = str(item.get("documentNumber") or item.get("doc_number") or item.get("docNumber") or doc_id)
            doc_type = (item.get("documentType") or item.get("docType") or cat).strip()
            recorded = (item.get("recordedDate") or item.get("recorded_date") or "").strip()
            grantor  = (item.get("grantor") or item.get("grantorName") or "").strip()
            grantee  = (item.get("grantee") or item.get("granteeName") or "").strip()
            legal    = (item.get("legalDescription") or item.get("legal") or "").strip()
            town     = (item.get("city") or item.get("town") or "Dallas").strip()

            filed = ""
            try:
                filed = datetime.strptime(recorded[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                try:
                    filed = datetime.strptime(recorded[:10], "%m/%d/%Y").strftime("%m/%d/%Y")
                except Exception:
                    filed = recorded[:10]

            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            clerk_url = f"https://dallas.tx.publicsearch.us/doc/{doc_id}" if doc_id else ""

            if not grantor and not doc_num:
                return None

            return {
                "doc_num":       doc_num,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.title(),
                "grantee":       grantee.title(),
                "amount":        None,
                "legal":         legal[:200],
                "clerk_url":     clerk_url,
                "prop_address":  address,
                "prop_city":     town.title() or "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch API item parse error: %s", e)
            return None

    def _parse_row(self, row, cat: str, cat_label: str) -> Optional[dict]:
        """Parse a table row directly â€” all data is in col-0 through col-10."""
        try:
            # Doc ID from checkbox id: "table-checkbox-314313835"
            chk = row.select_one("input[id^='table-checkbox-']")
            if not chk:
                return None
            internal_id = chk["id"].replace("table-checkbox-", "").strip()

            def col(n):
                el = row.select_one(f".col-{n} span")
                return el.get_text(strip=True) if el else ""

            grantor  = col(3)
            grantee  = col(4)
            doc_type = col(5)
            recorded = col(6)
            doc_num  = col(7)
            town     = col(9)
            legal    = col(10)

            if not grantor and not doc_num:
                return None

            # Parse date
            filed = ""
            try:
                filed = datetime.strptime(recorded.strip(), "%m/%d/%Y").strftime("%m/%d/%Y")
            except Exception:
                filed = recorded[:10] if recorded else ""

            # Extract address from legal description
            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            clerk_url = f"https://dallas.tx.publicsearch.us/doc/{internal_id}"

            return {
                "doc_num":       doc_num or internal_id,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.strip().title(),
                "grantee":       grantee.strip().title(),
                "amount":        None,
                "legal":         legal[:200],
                "clerk_url":     clerk_url,
                "prop_address":  address,
                "prop_city":     town.strip().title() or "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch row parse error: %s", e)
            return None

    def _fetch_doc(self, session, doc_id: str, cat: str, cat_label: str) -> Optional[dict]:
        try:
            url = self.DOC_URL.format(doc_id)
            r   = session.get(url, timeout=20)
            if r.status_code != 200:
                return None
            soup = BeautifulSoup(r.text, "lxml")

            def get_field(label):
                for el in soup.find_all(string=re.compile(label, re.I)):
                    parent = el.find_parent()
                    if parent:
                        nxt = parent.find_next_sibling()
                        if nxt:
                            return nxt.get_text(strip=True)
                return ""

            doc_num  = get_field("Document Number") or doc_id
            doc_type = get_field("Document Type") or cat
            recorded = get_field("Recorded Date") or ""
            grantor  = get_field("GRANTOR") or get_field("Grantor") or ""
            grantee  = get_field("GRANTEE") or get_field("Grantee") or ""
            legal    = get_field("Legal Description") or ""
            consider = get_field("Consideration") or ""

            filed = ""
            try:
                filed = datetime.strptime(recorded.strip()[:10], "%m/%d/%Y").strftime("%m/%d/%Y")
            except Exception:
                filed = recorded[:10] if recorded else ""

            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))\b",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            amount = None
            if consider:
                m2 = re.search(r"[\d,]+", consider.replace("$", ""))
                if m2:
                    try:
                        amount = float(m2.group().replace(",", ""))
                    except Exception:
                        pass

            if not grantor and not doc_num:
                return None

            return {
                "doc_num":       doc_num,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.strip().title(),
                "grantee":       grantee.strip().title(),
                "amount":        amount,
                "legal":         legal[:200],
                "clerk_url":     url,
                "prop_address":  address,
                "prop_city":     "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch doc %s error: %s", doc_id, e)
            return None

class DallasPublicSearchScraper:
    BASE       = "https://dallas.tx.publicsearch.us"
    SEARCH_URL = "https://dallas.tx.publicsearch.us/results"
    DOC_URL    = "https://dallas.tx.publicsearch.us/doc/{}"

    SEARCHES = [
        ("trustee sale", "NOFC", "Notice of Foreclosure"),
        ("foreclosure",  "NOFC", "Notice of Foreclosure"),
        ("lis pendens",  "LP",   "Lis Pendens"),
    ]

    def _make_session(self):
        s = make_session()
        s.headers.update({
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://dallas.tx.publicsearch.us/",
        })
        return s

    def fetch(self) -> list:
        log.info("Scraping Dallas County PublicSearch via Playwright (NOFC + LP)...")
        try:
            import asyncio as _asyncio
            from playwright.async_api import async_playwright as _apw
        except ImportError:
            log.error("Playwright not installed â€” run: pip install playwright && python -m playwright install chromium")
            return []

        cutoff     = LOOKBACK_DATE.date()
        start_str  = cutoff.strftime("%Y%m%d")
        end_str    = date.today().strftime("%Y%m%d")
        LIMIT      = 50
        BARGS      = ["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu"]

        async def _get_rows(page):
            return await page.evaluate("""() => {
                const out = [];
                for (const tr of document.querySelectorAll('tbody tr')) {
                    const cb = tr.querySelector('input[id^="table-checkbox-"]');
                    if (!cb) continue;
                    const id = cb.id.replace('table-checkbox-', '');
                    const c = i => { const e = tr.querySelector('.col-'+i+' span'); return e ? e.innerText.trim() : ''; };
                    out.push({id, g3:c(3), g4:c(4), g5:c(5), g6:c(6), g7:c(7), g9:c(9), g10:c(10)});
                }
                const m = document.body.innerText.match(/([0-9][0-9,]+)\s+results/);
                return {rows: out, total: m ? parseInt(m[1].replace(/,/g,'')) : 0};
            }""")

        async def _load_page(pw, url):
            for attempt in range(3):
                browser = await pw.chromium.launch(headless=True, args=BARGS)
                try:
                    ctx  = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                    page = await ctx.new_page()
                    await page.goto("https://dallas.tx.publicsearch.us/", wait_until="domcontentloaded", timeout=60000)
                    await _asyncio.sleep(2)
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await _asyncio.sleep(3)
                    for _ in range(27):
                        data = await _get_rows(page)
                        if data["rows"]: return data
                        await _asyncio.sleep(1)
                    return await _get_rows(page)
                except Exception as e:
                    log.warning("PublicSearch load attempt %d: %s", attempt+1, e)
                    await _asyncio.sleep(3)
                finally:
                    await browser.close()
            return {"rows": [], "total": 0}

        async def _scrape_one(pw, search_val, cat, cat_label):
            base = (f"https://dallas.tx.publicsearch.us/results"
                    f"?department=RP&searchType=quickSearch"
                    f"&searchValue={search_val.replace(' ', '+')}"
                    f"&recordedDateRange={start_str},{end_str}&limit={LIMIT}")
            seen, recs = set(), []
            total, offset, pnum = 9999, 0, 1
            while offset < total:
                data = await _load_page(pw, f"{base}&offset={offset}")
                if pnum == 1:
                    total = data["total"] or 9999
                    log.info("PublicSearch '%s' total=%d", search_val, total)
                batch = data["rows"]
                log.info("PublicSearch '%s' page %d rows=%d", search_val, pnum, len(batch))
                if not batch: break
                for r in batch:
                    key = r["g7"] or r["id"]
                    if key in seen: continue
                    seen.add(key)
                    # parse date
                    filed = ""
                    try: filed = datetime.strptime(r["g6"].strip(), "%m/%d/%Y").strftime("%m/%d/%Y")
                    except Exception: filed = r["g6"][:10] if r["g6"] else ""
                    # extract address from legal desc
                    address = ""
                    m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                                  r["g10"], re.I)
                    if m: address = m.group(1).strip().title()
                    recs.append({
                        "doc_num":      r["g7"] or r["id"],
                        "doc_type":     r["g5"],
                        "cat":          cat,
                        "cat_label":    cat_label,
                        "filed":        filed,
                        "owner":        r["g3"].strip().title(),
                        "grantee":      r["g4"].strip().title(),
                        "amount":       None,
                        "legal":        r["g10"][:200],
                        "clerk_url":    f"https://dallas.tx.publicsearch.us/doc/{r['id']}",
                        "prop_address": address,
                        "prop_city":    r["g9"].strip().title() or "Dallas",
                        "prop_state":   "TX",
                        "prop_zip":     "",
                        "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                        "source":       "Dallas County Clerk",
                        "neighborhood": "", "viol_status": r["g5"], "viol_desc": r["g5"],
                        "viol_severity": "high",
                        "delinquent":   False, "delinq_amt": "",
                        "homestead":    None,  "appraised":  "",
                        "out_of_state": False,
                        "luc":          "", "luc_desc": "",
                        "score":        0,  "flags":    [],
                    })
                offset += LIMIT; pnum += 1
                if offset < total: await _asyncio.sleep(2)
            return recs

        async def _run():
            all_recs = []
            async with _apw() as pw:
                for sv, cat, label in self.SEARCHES:
                    recs = await _scrape_one(pw, sv, cat, label)
                    log.info("PublicSearch '%s': %d records", sv, len(recs))
                    all_recs.extend(recs)
            # dedup by doc_num
            seen, out = set(), []
            for r in all_recs:
                if r["doc_num"] not in seen:
                    seen.add(r["doc_num"]); out.append(r)
            log.info("PublicSearch: %d records parsed from table", len(out))
            return out

        return _asyncio.run(_run())

    def _parse_api_item(self, item: dict, cat: str, cat_label: str) -> Optional[dict]:
        """Parse a JSON item from the PublicSearch API."""
        try:
            doc_id   = str(item.get("id") or item.get("docId") or item.get("documentId") or "")
            doc_num  = str(item.get("documentNumber") or item.get("doc_number") or item.get("docNumber") or doc_id)
            doc_type = (item.get("documentType") or item.get("docType") or cat).strip()
            recorded = (item.get("recordedDate") or item.get("recorded_date") or "").strip()
            grantor  = (item.get("grantor") or item.get("grantorName") or "").strip()
            grantee  = (item.get("grantee") or item.get("granteeName") or "").strip()
            legal    = (item.get("legalDescription") or item.get("legal") or "").strip()
            town     = (item.get("city") or item.get("town") or "Dallas").strip()

            filed = ""
            try:
                filed = datetime.strptime(recorded[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                try:
                    filed = datetime.strptime(recorded[:10], "%m/%d/%Y").strftime("%m/%d/%Y")
                except Exception:
                    filed = recorded[:10]

            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            clerk_url = f"https://dallas.tx.publicsearch.us/doc/{doc_id}" if doc_id else ""

            if not grantor and not doc_num:
                return None

            return {
                "doc_num":       doc_num,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.title(),
                "grantee":       grantee.title(),
                "amount":        None,
                "legal":         legal[:200],
                "clerk_url":     clerk_url,
                "prop_address":  address,
                "prop_city":     town.title() or "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch API item parse error: %s", e)
            return None

    def _parse_row(self, row, cat: str, cat_label: str) -> Optional[dict]:
        """Parse a table row directly â€” all data is in col-0 through col-10."""
        try:
            # Doc ID from checkbox id: "table-checkbox-314313835"
            chk = row.select_one("input[id^='table-checkbox-']")
            if not chk:
                return None
            internal_id = chk["id"].replace("table-checkbox-", "").strip()

            def col(n):
                el = row.select_one(f".col-{n} span")
                return el.get_text(strip=True) if el else ""

            grantor  = col(3)
            grantee  = col(4)
            doc_type = col(5)
            recorded = col(6)
            doc_num  = col(7)
            town     = col(9)
            legal    = col(10)

            if not grantor and not doc_num:
                return None

            # Parse date
            filed = ""
            try:
                filed = datetime.strptime(recorded.strip(), "%m/%d/%Y").strftime("%m/%d/%Y")
            except Exception:
                filed = recorded[:10] if recorded else ""

            # Extract address from legal description
            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            clerk_url = f"https://dallas.tx.publicsearch.us/doc/{internal_id}"

            return {
                "doc_num":       doc_num or internal_id,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.strip().title(),
                "grantee":       grantee.strip().title(),
                "amount":        None,
                "legal":         legal[:200],
                "clerk_url":     clerk_url,
                "prop_address":  address,
                "prop_city":     town.strip().title() or "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch row parse error: %s", e)
            return None

    def _fetch_doc(self, session, doc_id: str, cat: str, cat_label: str) -> Optional[dict]:
        try:
            url = self.DOC_URL.format(doc_id)
            r   = session.get(url, timeout=20)
            if r.status_code != 200:
                return None
            soup = BeautifulSoup(r.text, "lxml")

            def get_field(label):
                for el in soup.find_all(string=re.compile(label, re.I)):
                    parent = el.find_parent()
                    if parent:
                        nxt = parent.find_next_sibling()
                        if nxt:
                            return nxt.get_text(strip=True)
                return ""

            doc_num  = get_field("Document Number") or doc_id
            doc_type = get_field("Document Type") or cat
            recorded = get_field("Recorded Date") or ""
            grantor  = get_field("GRANTOR") or get_field("Grantor") or ""
            grantee  = get_field("GRANTEE") or get_field("Grantee") or ""
            legal    = get_field("Legal Description") or ""
            consider = get_field("Consideration") or ""

            filed = ""
            try:
                filed = datetime.strptime(recorded.strip()[:10], "%m/%d/%Y").strftime("%m/%d/%Y")
            except Exception:
                filed = recorded[:10] if recorded else ""

            address = ""
            m = re.search(r"(\d+\s+\S+(?:\s+\S+){1,5}(?:ST|AVE|BLVD|DR|RD|LN|CT|WAY|PL|TRL|PKWY))\b",
                          legal, re.I)
            if m:
                address = m.group(1).strip().title()

            amount = None
            if consider:
                m2 = re.search(r"[\d,]+", consider.replace("$", ""))
                if m2:
                    try:
                        amount = float(m2.group().replace(",", ""))
                    except Exception:
                        pass

            if not grantor and not doc_num:
                return None

            return {
                "doc_num":       doc_num,
                "doc_type":      doc_type,
                "cat":           cat,
                "cat_label":     cat_label,
                "filed":         filed,
                "owner":         grantor.strip().title(),
                "grantee":       grantee.strip().title(),
                "amount":        amount,
                "legal":         legal[:200],
                "clerk_url":     url,
                "prop_address":  address,
                "prop_city":     "Dallas",
                "prop_state":    "TX",
                "prop_zip":      "",
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Dallas County Clerk",
                "neighborhood":  "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":    False, "delinq_amt": "",
                "homestead":     None, "appraised":  "",
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,  "flags":    [],
            }
        except Exception as e:
            log.debug("PublicSearch doc %s error: %s", doc_id, e)
            return None

class DallasTaxSaleScraper:
    """
    Pulls Dallas-area tax sale properties from the LGBS map API.
    The site is a JS app but loads property data from a /map endpoint
    discovered via Network tab â€” returns JSON with full property details.
    Paginates using offset in steps of 50 until all records are fetched.
    """

    # Real API endpoint discovered via Network tab â€” /api/property_sales/
    API_URL  = "https://taxsales.lgbs.com/api/property_sales/"
    PAGE     = 50
    IN_BBOX  = "-97.40638188867187,32.46198950912711,-96.05781011132812,33.174066019764034"

    # Dallas-area county names to filter for (site covers all of Texas)
    DALLAS_COUNTIES = {"dallas", "collin", "denton", "tarrant", "rockwall",
                       "kaufman", "ellis", "johnson"}

    def fetch(self) -> list:
        log.info("Scraping LGBS tax sale list via map APIâ€¦")
        session = make_session()
        # Mimic a real browser â€” the site checks headers
        session.headers.update({
            "Accept":          "application/json, text/plain, */*",
            "Referer":         "https://taxsales.lgbs.com/",
            "Origin":          "https://taxsales.lgbs.com",
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        })
        records = []
        offset  = 0

        while True:
            try:
                params = {
                    "in_bbox":   self.IN_BBOX,
                    "offset":    offset,
                    "ordering":  "precinct,sale_nbr,uid",
                    "sale_type": "SALE,RESALE,STRUCK OFF,FUTURE SALE",
                    "limit":     self.PAGE,
                }
                r = session.get(self.API_URL, params=params, timeout=30)
                if r.status_code != 200:
                    log.warning("LGBS API HTTP %d â€” %s", r.status_code, r.text[:200])
                    break

                # Response is JSON with a results array
                try:
                    data = r.json()
                except Exception:
                    log.warning("LGBS API returned non-JSON - site may be blocking")
                    break

                # Handle both {"results": [...]} and bare list responses
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = data.get("results") or data.get("properties") or []
                else:
                    break

                if not items:
                    break

                log.info("LGBS offset %d: %d items", offset, len(items))
                for item in items:
                    # Filter to Dallas metro only
                    county = (item.get("county") or "").lower().strip()
                    # LGBS returns "dallas county" or "dallas" â€” check if any of our
                    # target county names appear anywhere in the county field
                    if county and not any(c in county for c in self.DALLAS_COUNTIES):
                        continue
                    rec = self._to_record(item)
                    if rec:
                        records.append(rec)

                if len(items) < self.PAGE:
                    break  # last page
                offset += self.PAGE
                time.sleep(0.5)

            except Exception as e:
                log.warning("LGBS fetch error at offset %d: %s", offset, e)
                break

        log.info("Tax sale: %d Dallas-area records", len(records))
        return records

    def _to_record(self, item: dict) -> Optional[dict]:
        try:
            # prop_address_one contains the FULL address e.g. "2811 S Story Rd"
            # street_name is street-only fallback e.g. "S Story Rd"
            address_full = (item.get("prop_address_one") or
                            item.get("address_full") or
                            item.get("address") or "").strip().title()
            street       = (item.get("street_name") or "").strip().title()
            city         = (item.get("prop_city") or item.get("city") or
                            item.get("municipality") or "Dallas").strip().title()
            state        = (item.get("prop_state") or item.get("state") or "TX").strip().upper()
            zipcode      = str(item.get("prop_zipcode") or item.get("zip") or
                               item.get("zip_code") or "").strip()
            cause_num    = str(item.get("cause_nbr") or item.get("cause_number") or
                               item.get("uid") or "").strip()

            # Coordinates from LGBS geometry â€” used for precise DCAD spatial lookup
            geo      = item.get("geometry") or {}
            coords   = geo.get("coordinates") or []
            prop_lng = float(coords[0]) if len(coords) >= 2 else None
            prop_lat = float(coords[1]) if len(coords) >= 2 else None
            sale_date    = (item.get("sale_date") or "").strip()
            sale_type    = (item.get("sale_type") or "SALE").strip()
            adj_value    = item.get("adjudged_value") or item.get("adjudged_val")
            min_bid      = item.get("minimum_bid") or item.get("min_bid")
            geo_id       = str(item.get("geo_id") or
                               item.get("account_number") or "").strip()
            precinct     = str(item.get("precinct") or "").strip()
            status       = (item.get("status") or "").strip()

            # Parse sale date
            filed = ""
            if sale_date:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
                    try:
                        filed = datetime.strptime(sale_date[:19], fmt).strftime("%m/%d/%Y")
                        break
                    except Exception:
                        continue

            # Use minimum bid as the amount (most relevant for investors)
            amount = None
            for val in (min_bid, adj_value):
                if val:
                    try:
                        amount = float(str(val).replace(",", "").replace("$", ""))
                        break
                    except Exception:
                        pass

            appraised = ""
            if adj_value:
                try:
                    appraised = f"${float(str(adj_value).replace(',','').replace('$','')):,.0f}"
                except Exception:
                    appraised = str(adj_value)

            if not address_full and not street and not cause_num:
                return None

            clerk_url = (f"https://taxsales.lgbs.com/?cause={cause_num}"
                         if cause_num else "https://taxsales.lgbs.com/")

            return {
                "doc_num":       f"TAXSALE-{cause_num}" if cause_num else "TAXSALE",
                "doc_type":      f"TAXSALE-{sale_type}",
                "cat":           "TAXSALE",
                "cat_label":     "Tax Sale",
                "filed":         filed,
                "owner":         "",   # LGBS doesn't expose owner name in list view
                "grantee":       "Dallas County",
                "amount":        amount,
                "legal":         geo_id,
                "clerk_url":     clerk_url,
                "prop_address":  address_full or (f"{street}, {city}" if street else ""),
                "prop_city":     city,
                "prop_state":    state,
                "prop_zip":      zipcode,
                "prop_lng":      prop_lng,
                "prop_lat":      prop_lat,
                "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":        "Tax Sale List",
                "neighborhood":  precinct,
                "viol_status":   status,
                "viol_desc":     f"Tax sale â€” {sale_type}",
                "viol_severity": "high",
                "delinquent":    True,
                "delinq_amt":    f"{amount:,.2f}" if amount else "",
                "homestead":     None,
                "appraised":     appraised,
                "out_of_state":  False,
                "luc":           "", "luc_desc": "",
                "score":         0,
                "flags":         [],
            }
        except Exception as e:
            log.debug("LGBS record parse error: %s", e)
            return None


# ===========================================================================
# Source: LP â€” Lis Pendens PDF (manual upload)
# Drop the PDF at data/lp_export.pdf before running.
# ===========================================================================

class LPScraper:
    def fetch(self) -> list:
        if not LP_PDF_PATH.exists():
            log.info("No LP PDF found at %s â€” skipping", LP_PDF_PATH)
            return []
        try:
            import pdfplumber as _pp
        except ImportError:
            log.warning("pdfplumber not installed â€” LP PDF skipped. pip install pdfplumber")
            return []

        log.info("Found LP PDF at %s â€” parsingâ€¦", LP_PDF_PATH)
        records = []
        try:
            with _pp.open(str(LP_PDF_PATH)) as pdf:
                for page in pdf.pages:
                    words = page.extract_words(x_tolerance=4, y_tolerance=4)
                    rows  = self._words_to_rows(words, page.height)
                    for row in rows:
                        rec = self._row_to_record(row)
                        if rec:
                            records.append(rec)
        except Exception as e:
            log.warning("LP PDF parse error: %s", e)

        log.info("LP PDF: %d lis pendens records", len(records))
        return records

    def _words_to_rows(self, words: list, page_height: float) -> list:
        if not words:
            return []
        lines: dict = defaultdict(list)
        for w in words:
            key = round(w["top"] / 8) * 8
            lines[key].append(w)
        rows = []
        for top in sorted(lines):
            row_words = sorted(lines[top], key=lambda w: w["x0"])
            row = {}
            for col, (x0, x1) in _LP_COLS.items():
                col_words = [w["text"] for w in row_words if x0 <= w["x0"] < x1]
                row[col] = " ".join(col_words).strip()
            rows.append(row)
        return rows

    def _row_to_record(self, row: dict) -> Optional[dict]:
        grantor  = row.get("grantor", "").strip()
        address  = row.get("address", "").strip().title()
        date_str = row.get("date", "").strip()
        docnum   = row.get("docnum", "").strip()
        parcel   = row.get("parcel", "").strip()

        if not grantor or not date_str:
            return None
        if re.search(r"grantor|grantee|date|address|parcel", grantor, re.I):
            return None  # header row

        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
            if dt < _LP_CUTOFF:
                return None
            filed = date_str
        except Exception:
            return None

        city, state, zipcode = "Dallas", "TX", ""
        m = re.search(r",\s*([^,]+),\s*(TX|TEXAS)\s+(\d{5})", address, re.I)
        if m:
            city    = m.group(1).strip().title()
            zipcode = m.group(3)

        return {
            "doc_num":       docnum or f"LP-{grantor[:15].replace(' ', '')}",
            "doc_type":      "LP",
            "cat":           "LP",
            "cat_label":     "Lis Pendens",
            "filed":         filed,
            "owner":         grantor.title(),
            "grantee":       row.get("grantee", "").strip().title(),
            "amount":        None,
            "legal":         parcel,
            "clerk_url":     "",
            "prop_address":  address,
            "prop_city":     city,
            "prop_state":    state,
            "prop_zip":      zipcode,
            "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":        "Manual (PDF)",
            "neighborhood":  "", "viol_status": "", "viol_desc": "", "viol_severity": "",
            "delinquent":    False, "delinq_amt": "",
            "homestead":     None,
            "appraised":     "",
            "out_of_state":  False,
            "luc":           "", "luc_desc": "",
            "score":         0,
            "flags":         [],
        }


# ===========================================================================
# Source: BK â€” Bankruptcy (CourtListener, Northern District TX)
# ===========================================================================

class BankruptcyScraper:
    """
    Pulls Chapter 7 & 13 bankruptcy filings from CourtListener for txnb.
    FIX 2: Date-chunking loop now stops if we've already reached the cutoff,
            preventing an infinite loop.
    FIX 3: page_size raised to 50 and docket sub-fetches run 4 at a time,
            cutting runtime from ~2hr to ~25min.
    """

    def fetch(self) -> list:
        if not COURTLISTENER_TOKEN:
            log.info("No COURTLISTENER_TOKEN â€” skipping BK")
            return []
        log.info("Scraping Northern District Texas bankruptcy filingsâ€¦")
        session = make_session()
        session.headers.update({
            "Authorization": f"Token {COURTLISTENER_TOKEN}",
            "Accept":        "application/json",
        })
        records = []
        cutoff  = LOOKBACK_DATE.strftime("%Y-%m-%d")

        for chapter in ["7", "13"]:
            fetched = self._fetch_chapter(session, chapter, cutoff, records)
            log.info("BK chapter %s: %d records", chapter, fetched)

        log.info("BK: %d total records", len(records))
        return records

    def _fetch_chapter(self, session, chapter: str, cutoff: str, records: list) -> int:
        params = {
            "docket__court":           CL_BK_COURT,
            "chapter":                 chapter,
            "docket__date_filed__gte": cutoff,
            "order_by":                "-docket__date_filed",
            # FIX 3 â€” was 10; larger pages = fewer list requests before sub-fetches
            "page_size":               100,
            "format":                  "json",
            "fields":                  "docket,chapter,date_filed,debtor_name,docket_number",
        }
        next_url  = CL_BK_URL
        page      = 1
        fetched   = 0
        MAX_PAGES = 50  # cap to prevent timeout

        while next_url:
            try:
                resp = None
                for attempt in range(2):
                    try:
                        resp = session.get(
                            next_url,
                            params=params if page == 1 else None,
                            timeout=15,
                        )
                        break
                    except Exception as te:
                        log.warning("BK Ch.%s page %d attempt %d timeout: %s",
                                    chapter, page, attempt + 1, te)
                        time.sleep(3)
                if resp is None:
                    log.warning("BK Ch.%s page %d â€” all retries failed, stopping", chapter, page)
                    break
                if resp.status_code == 429:
                    log.warning("CourtListener rate limited â€” stopping BK")
                    break
                if resp.status_code != 200:
                    log.warning("BK Ch.%s HTTP %d: %s", chapter, resp.status_code, resp.text[:200])
                    break

                data    = resp.json()
                results = data.get("results", [])
                if not results:
                    break
                log.info("BK chapter %s page %d: %d results", chapter, page, len(results))

                # FIX 3 â€” fetch all dockets in this page concurrently (4 workers)
                new_recs = self._fetch_dockets_parallel(session, results, chapter)
                for rec in new_recs:
                    records.append(rec)
                    fetched += 1

                next_link = data.get("next")
                if not next_link:
                    break

                time.sleep(0.3)
                # CourtListener blocks past page 100 â€” date-chunk workaround
                if page >= 99:
                    dates = [r.get("date_filed", "") for r in results if r.get("date_filed")]
                    if dates:
                        oldest = min(dates)
                        # FIX 2 â€” stop if we've already reached or passed the cutoff
                        if oldest <= cutoff:
                            log.info("BK Ch.%s reached cutoff at page 99 â€” stopping", chapter)
                            break
                        log.info("BK Ch.%s hit page 99 â€” restarting from %s", chapter, oldest)
                        params["docket__date_filed__lte"] = oldest
                        params["page"] = 1
                        next_url = CL_BK_URL
                        page     = 1
                    else:
                        break
                else:
                    next_url = next_link
                    page    += 1
                    if page > MAX_PAGES:
                        log.info("BK Ch.%s hit MAX_PAGES %d â€” stopping", chapter, MAX_PAGES)
                        break

                time.sleep(0.3)
            except Exception as e:
                log.warning("BK Ch.%s page %d error: %s", chapter, page, e)
                break

        return fetched

    def _fetch_dockets_parallel(self, session, items: list, chapter: str) -> list:
        """Fetch docket details for a batch of items using 4 parallel threads."""
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self._to_record, session, item, chapter): item
                for item in items
            }
            for future in as_completed(futures):
                try:
                    rec = future.result()
                    if rec:
                        results.append(rec)
                except Exception as e:
                    log.debug("BK docket fetch error: %s", e)
        return results

    def _to_record(self, session, item: dict, chapter: str) -> Optional[dict]:
        docket_ref  = item.get("docket") or ""
        docket_data = {}
        docket_url  = ""

        if isinstance(docket_ref, str) and docket_ref:
            docket_url = (docket_ref if docket_ref.startswith("http")
                          else "https://www.courtlistener.com" + docket_ref)
            try:
                r = session.get(
                    docket_url,
                    params={"format": "json",
                            "fields": "case_name,docket_number,date_filed,absolute_url"},
                    timeout=20,
                )
                if r.status_code == 200:
                    docket_data = r.json()
            except Exception:
                pass
        elif isinstance(docket_ref, dict):
            docket_data = docket_ref
            docket_url  = docket_data.get("absolute_url", "")

        case_name = (docket_data.get("case_name") or "").strip()
        case_num  = (docket_data.get("docket_number") or "").strip()
        filed_raw = docket_data.get("date_filed") or item.get("date_filed") or ""
        abs_url   = docket_data.get("absolute_url", "")
        clerk_url = ("https://www.courtlistener.com" + abs_url if abs_url
                     else "https://www.courtlistener.com/recap/")

        if not case_name:
            return None

        name = re.sub(r"(?i)^in\s+re:?\s*", "", case_name).strip()
        name = re.sub(r"\s*\(.*?\)", "", name).strip()

        filed_fmt = ""
        if filed_raw:
            try:
                filed_fmt = datetime.strptime(filed_raw[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                filed_fmt = filed_raw[:10]

        return {
            "doc_num":       case_num or f"BK-{name[:20]}",
            "doc_type":      f"BK{chapter}",
            "cat":           "BK",
            "cat_label":     f"Bankruptcy Ch.{chapter}",
            "filed":         filed_fmt,
            "owner":         name,
            "grantee":       "",
            "amount":        None,
            "legal":         "",
            "clerk_url":     clerk_url,
            "prop_address":  "",
            "prop_city":     "Dallas",
            "prop_state":    "TX",
            "prop_zip":      "",
            "mail_address":  "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":        "Court Docket",
            "neighborhood":  "", "viol_status": "", "viol_desc": "", "viol_severity": "",
            "delinquent":    False, "delinq_amt": "",
            "homestead":     None,
            "appraised":     "",
            "out_of_state":  False,
            "luc":           "", "luc_desc": "",
            "bk_chapter":    chapter,
            "score":         0,
            "flags":         [],
        }


# ===========================================================================
# Parcel Enrichment â€” DCAD ArcGIS
# ===========================================================================

class ParcelLookup:
    """
    Enriches records with owner, mailing address, homestead flag, and
    appraised value from the Dallas Central Appraisal District ArcGIS API.

    FIX 1 â€” addresses are now normalized to match DCAD's ALL-CAPS format
    and common suffix variants (e.g. Boulevard â†’ BLVD) before querying.
    The [:20] street slice that cut names mid-word is also removed.
    """

    def __init__(self):
        self._session = None

    def load(self):
        self._session = make_session()
        try:
            r = self._session.get(DCAD_URL, params={
                "where":             "ACCOUNT_NUM='00832550000000000'",
                "outFields":         "SITEADDRESS,OWNERNME1,CNVYNAME,PSTLADDRESS,PSTLCITY,PSTLSTATE,PSTLZIP5",
                "f":                 "json",
                "resultRecordCount": 1,
                "returnGeometry":    "false",
            }, timeout=15)
            if r.status_code == 200:
                log.info("DCAD ArcGIS: reachable")
            else:
                log.warning("DCAD ArcGIS: HTTP %d", r.status_code)
        except Exception as e:
            log.warning("DCAD ArcGIS load error: %s", e)

    def enrich(self, records: list) -> list:
        if not self._session:
            self.load()

        enriched = 0
        for rec in records:
            address = rec.get("prop_address", "")
            parcel  = rec.get("legal", "")
            lat     = rec.get("prop_lat")
            lng     = rec.get("prop_lng")
            if not address and not parcel and not lat:
                continue
            match = self._lookup(address, parcel, lat, lng)
            if match:
                self._apply(rec, match)
                enriched += 1

        log.info("Parcel enrichment: %d/%d records matched", enriched, len(records))
        return records

    def _lookup(self, address: str, parcel: str,
                lat: float = None, lng: float = None) -> Optional[dict]:

        OUT_FIELDS = (
            "SITEADDRESS,OWNERNME1,CNVYNAME,PSTLADDRESS,PSTLCITY,PSTLSTATE,PSTLZIP5,CNTASSDVAL,USECD,USEDSCRP"
        )

        # --- 1. Spatial lookup (most precise â€” uses GPS coordinates from LGBS) ---
        if lat is not None and lng is not None:
            try:
                params = {
                    "geometry":      f"{lng},{lat}",
                    "geometryType":  "esriGeometryPoint",
                    "inSR":          "4326",
                    "spatialRel":    "esriSpatialRelIntersects",
                    "outFields":     OUT_FIELDS,
                    "f":             "json",
                    "resultRecordCount": 1,
                    "returnGeometry": "false",
                }
                r = self._session.get(DCAD_URL, params=params, timeout=15)
                if r.status_code == 200:
                    features = r.json().get("features") or []
                    if features:
                        attrs = features[0].get("attributes", {})
                        log.debug("DCAD spatial hit: %s", dict(list(attrs.items())[:6]))
                        return attrs
            except Exception as e:
                log.debug("DCAD spatial lookup error: %s", e)

        # --- 2. Attribute queries (fallback when no coordinates) ---
        queries = []

        if parcel:
            subdiv_m = re.search(r"Name:\s*((?:[A-Z0-9]+\s+){1,2}[A-Z0-9]+)", parcel, re.I)
            lot_m    = re.search(r"Lot:\s*(\S+)", parcel, re.I)
            block_m  = re.search(r"Block:\s*(\S+)", parcel, re.I)
            if subdiv_m and lot_m and block_m:
                subdiv = subdiv_m.group(1).strip()
                lot    = lot_m.group(1).strip()
                block  = block_m.group(1).strip()
                queries.append(
                    f"CNVYNAME LIKE '%{subdiv}%' AND "
                    f"PRPRTYDSCRP LIKE '%BLK%{block}%LT {lot} %'"
                )
                queries.append(
                    f"CNVYNAME LIKE '%{subdiv}%' AND "
                    f"PRPRTYDSCRP LIKE '%BLK%{block}%LOT {lot} %'"
                )
            elif subdiv_m:
                queries.append(f"CNVYNAME LIKE '%{subdiv_m.group(1).strip()}%'")
            elif not re.search(r"Subdivision|Lot|Block", parcel, re.I):
                clean_parcel = re.sub(r"[\s\-]", "", parcel)
                queries.append(f"ACCOUNT_NUM='{clean_parcel}'")

        if address:
            num_match = re.match(r"^(\d+)\s+(.+?)(?:,|$)", address)
            if num_match:
                num    = num_match.group(1)
                street = _normalize_street_for_dcad(num_match.group(2).strip().split(",")[0])
                queries.append(f"SITEADDRESS LIKE '{num} {street}%'")
            else:
                street_raw = address.split(",")[0].strip()
                if street_raw:
                    street = _normalize_street_for_dcad(street_raw)
                    queries.append(f"SITEADDRESS LIKE '%{street}%'")

        for where in queries:
            try:
                params = {
                    "where":             where,
                    "outFields":         OUT_FIELDS,
                    "f":                 "json",
                    "resultRecordCount": 1,
                    "returnGeometry":    "false",
                }
                r = self._session.get(DCAD_URL, params=params, timeout=15)
                if r.status_code != 200:
                    continue
                data     = r.json()
                features = data.get("features") or []
                if not features:
                    continue
                attrs = features[0].get("attributes", {})
                log.debug("DCAD attr hit: %s", dict(list(attrs.items())[:6]))
                return attrs
            except Exception as e:
                log.debug("DCAD lookup error: %s", e)

        return None

    def _apply(self, rec: dict, attrs: dict):
        situs_num    = str(attrs.get("SITUS_NUM") or "").strip()
        situs_street = (attrs.get("SITUS_STREET") or "").strip().title()
        if situs_num and situs_street:
            # Always prefer DCAD's full address (number + street) over partial
            # addresses from source data (e.g. LGBS only returns street name).
            rec["prop_address"] = f"{situs_num} {situs_street}"
            rec["prop_city"]    = (attrs.get("SITUS_CITY") or "Dallas").strip().title()
            rec["prop_zip"]     = str(attrs.get("SITUS_ZIP") or "").strip()
        elif situs_street and not rec.get("prop_address"):
            # DCAD has street but no number â€” only fill if record has nothing yet
            rec["prop_address"] = situs_street
            rec["prop_city"]    = (attrs.get("SITUS_CITY") or "Dallas").strip().title()
            rec["prop_zip"]     = str(attrs.get("SITUS_ZIP") or "").strip()

        if not rec.get("owner"):
            rec["owner"] = (attrs.get("OWNERNME1") or "").strip().title()

        mail_addr  = (attrs.get("PSTLADDRESS") or "").strip().title()
        mail_city  = (attrs.get("PSTLCITY")    or "").strip().title()
        mail_state = (attrs.get("PSTLSTATE")   or "TX").strip().upper()
        mail_zip   = str(attrs.get("PSTLZIP5") or "").strip()
        if mail_addr:
            rec["mail_address"] = mail_addr
            rec["mail_city"]    = mail_city
            rec["mail_state"]   = mail_state
            rec["mail_zip"]     = mail_zip
            rec["out_of_state"] = mail_state not in ("", "TX")

        rec["homestead"] = None
        appr = attrs.get("CNTASSDVAL")
        if appr:
            try:
                rec["appraised"] = f"${float(appr):,.0f}"
            except Exception:
                rec["appraised"] = str(appr)

        rec["luc"]      = str(attrs.get("USECD") or "").strip()
        rec["luc_desc"] = (attrs.get("USEDSCRP") or "").strip()

        acct = str(attrs.get("ACCOUNT_NUM") or "").strip()
        if acct and not rec.get("legal"):
            rec["legal"] = acct



# ===========================================================================
# Nominatim reverse geocoder (free, no API key, covers all DFW)
# ===========================================================================

class NominatimGeocoder:
    """
    Reverse-geocodes records that have coordinates but no house number.
    Uses OpenStreetMap Nominatim â€” free, no key, 1 req/sec limit.
    Only called for TAXSALE records where DCAD returned no SITUS_NUM.
    """

    BASE_URL = "https://nominatim.openstreetmap.org/reverse"
    HEADERS  = {"User-Agent": "DallasIntel/1.0 (github.com/10xresourcesinc-ai/Dallas-intel)"}

    def _needs_geocode(self, rec: dict) -> bool:
        if not rec.get("prop_lat") or not rec.get("prop_lng"):
            return False
        address = rec.get("prop_address", "")
        return not re.match(r"^\d+\s", address)

    def geocode(self, records: list) -> list:
        targets = [r for r in records if self._needs_geocode(r)]
        if not targets:
            log.info("Nominatim: no records need geocoding")
            return records

        log.info("Nominatim: reverse-geocoding %d records (1 req/sec)...", len(targets))
        fixed = 0
        for rec in targets:
            try:
                params = {
                    "lat":            rec["prop_lat"],
                    "lon":            rec["prop_lng"],
                    "format":         "jsonv2",
                    "addressdetails": 1,
                }
                r = requests.get(
                    self.BASE_URL, params=params,
                    headers=self.HEADERS, timeout=10
                )
                if r.status_code != 200:
                    time.sleep(1)
                    continue

                data    = r.json()
                addr    = data.get("address", {})
                house   = addr.get("house_number", "")
                road    = addr.get("road", "")
                city    = (addr.get("city") or addr.get("town") or
                           addr.get("village") or addr.get("county") or "")
                zipcode = addr.get("postcode", "")

                if house and road:
                    rec["prop_address"] = f"{house} {road.title()}"
                    if city:
                        rec["prop_city"] = city.title()
                    if zipcode:
                        rec["prop_zip"] = zipcode
                    fixed += 1
                    log.debug("Nominatim fixed: %s", rec["prop_address"])

            except Exception as e:
                log.debug("Nominatim error for %s: %s", rec.get("doc_num"), e)
            finally:
                time.sleep(1)

        log.info("Nominatim: fixed %d/%d records", fixed, len(targets))
        return records

# ===========================================================================
# Scoring
# ===========================================================================

class LeadScorer:

    @staticmethod
    def score(rec: dict, owner_cats_index: dict) -> tuple:
        """
        Score a single record.
        owner_cats_index: pre-built dict of normalized_owner â†’ set of cats
                          (built once in main() to avoid O(nÂ²) scanning).
        FIX 5 â€” WEEK_AGO now computed fresh inside score() instead of
                 being frozen at import time.
        FIX 6 â€” cross-signal stacking uses the pre-built index instead of
                 scanning all_recs on every call.
        """
        week_ago = datetime.now() - timedelta(days=7)  # FIX 5

        flags, points = [], 15
        cat   = rec.get("cat", "")
        owner = rec.get("owner", "")
        amt   = rec.get("amount")

        # Primary signal
        if cat == "LP":
            flags.append("Lis pendens");         points += 20
        if cat == "NOFC":
            flags.append("Pre-foreclosure");     points += 25
        if cat == "TAXSALE":
            flags.append("Tax sale scheduled");  points += 30
        if cat == "JUD":
            flags.append("Judgment lien");       points += 15
        if cat == "CODE":
            flags.append("Code violation");      points += 10
            sev = rec.get("viol_severity", "")
            if sev == "high":
                flags.append("Structural violation"); points += 15
            elif sev == "medium":
                points += 7
        if cat == "CODE_STRUCT":
            flags.append("Code violation");      points += 25
        if cat == "BK":
            chapter = rec.get("bk_chapter", "")
            flags.append(f"Bankruptcy Ch.{chapter}" if chapter else "Bankruptcy")
            points += 20

        # Cross-signal stacking â€” FIX 6: use pre-built index, not a full scan
        norm = normalize_name(owner)
        if norm:
            owner_cats = owner_cats_index.get(norm, set())
            if "LP" in owner_cats and owner_cats & {"NOFC", "TAXSALE"}:
                points += 20
            if owner_cats & {"NOFC", "LP", "TAXSALE"} and owner_cats & {"CODE", "CODE_STRUCT"}:
                flags.append("Foreclosure + violation"); points += 25
            if len(owner_cats) >= 3:
                flags.append("Multi-hit owner"); points += 15

        # Debt size
        if amt:
            if   amt > 100_000: flags.append("High debt (>$100k)"); points += 15
            elif amt >  50_000: points += 8

        # Recency â€” FIX 5: uses local week_ago computed above
        try:
            dt = datetime.strptime(rec.get("filed", "").strip(), "%m/%d/%Y")
            if dt >= week_ago:
                flags.append("New this week"); points += 5
        except Exception:
            pass

        # Enrichment signals
        if rec.get("prop_address") or rec.get("mail_address"):
            flags.append("Address found"); points += 3

        if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner, re.I):
            flags.append("LLC / corp owner"); points += 5

        if rec.get("delinquent"):
            flags.append("Delinquent taxes"); points += 20
        if rec.get("out_of_state"):
            flags.append("Out-of-state owner"); points += 15
        if rec.get("homestead") is False and rec.get("prop_address"):
            flags.append("No homestead exemption"); points += 3

        return min(100, max(0, points)), list(dict.fromkeys(flags))


# ===========================================================================
# Export
# ===========================================================================

GHL_FIELDS = [
    "Score", "Type", "Category", "Filed Date", "Owner Name",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Amount", "Appraised Value", "Delinquent", "Delinquency Amount",
    "Homestead", "Out of State", "Flags", "Source", "Doc Number", "Link",
]


def export_ghl_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow({
                "Score":              r.get("score", 0),
                "Type":               r.get("cat_label", r.get("cat", "")),
                "Category":           r.get("cat", ""),
                "Filed Date":         r.get("filed", ""),
                "Owner Name":         r.get("owner", ""),
                "Property Address":   r.get("prop_address", ""),
                "Property City":      r.get("prop_city", ""),
                "Property State":     r.get("prop_state", "TX"),
                "Property Zip":       r.get("prop_zip", ""),
                "Mailing Address":    r.get("mail_address", ""),
                "Mailing City":       r.get("mail_city", ""),
                "Mailing State":      r.get("mail_state", "TX"),
                "Mailing Zip":        r.get("mail_zip", ""),
                "Amount":             r.get("amount") or "",
                "Appraised Value":    r.get("appraised", ""),
                "Delinquent":         "Yes" if r.get("delinquent") else "No",
                "Delinquency Amount": r.get("delinq_amt", ""),
                "Homestead":          "Yes" if r.get("homestead") else "No",
                "Out of State":       "Yes" if r.get("out_of_state") else "No",
                "Flags":              ", ".join(r.get("flags") or []),
                "Source":             r.get("source", ""),
                "Doc Number":         r.get("doc_num", ""),
                "Link":               r.get("clerk_url", ""),
            })
    log.info("GHL CSV exported: %s (%d rows)", path, len(records))


# ===========================================================================
# Main pipeline
# ===========================================================================

def main():
    log.info("Dallas Intel scraper starting â€” lookback %d days", LOOKBACK_DAYS)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list = []

    # 1. Code violations
    all_records.extend(DallasCodeScraper().fetch())
    log.info("Total after code violations: %d", len(all_records))

    # 2. Foreclosure / trustee sale notices
    all_records.extend(DallasPublicSearchScraper().fetch())
    log.info("Total after NOFC: %d", len(all_records))

    # 3. Tax sale list
    all_records.extend(DallasTaxSaleScraper().fetch())
    log.info("Total after tax sales: %d", len(all_records))

    # 4. LP PDF
    all_records.extend(LPScraper().fetch())
    log.info("Total after LP: %d", len(all_records))

    # 5. Bankruptcy
    # BK disabled - low quality leads, CourtListener blocked from GitHub Actions IPs
    # all_records.extend(BankruptcyScraper().fetch())
    log.info("Total after BK: %d", len(all_records))

    # 6. Parcel enrichment
    lookup = ParcelLookup()
    lookup.load()
    all_records = lookup.enrich(all_records)

    # 7. Score
    # FIX 6 â€” build ownerâ†’categories index once here so scoring is O(n) not O(nÂ²)
    owner_cats_index: dict = defaultdict(set)
    for r in all_records:
        n = normalize_name(r.get("owner", ""))
        if n:
            owner_cats_index[n].add(r["cat"])

    scorer = LeadScorer()
    for rec in all_records:
        rec["score"], rec["flags"] = scorer.score(rec, owner_cats_index)

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    hot = sum(1 for r in all_records if r.get("score", 0) >= 70)
    log.info("Scoring complete. Total=%d  Hot(â‰¥70)=%d  Max=%d",
             len(all_records), hot,
             max((r.get("score", 0) for r in all_records), default=0))

    # 8. Write outputs
    payload = json.dumps(all_records, indent=2, default=str)
    RECORDS_JSON.write_text(payload, encoding="utf-8")
    DASH_JSON.write_text(payload, encoding="utf-8")
    export_ghl_csv(all_records, GHL_CSV)

    log.info("Dallas Intel complete. %d leads written.", len(all_records))


if __name__ == "__main__":
    main()


