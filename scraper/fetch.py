#!/usr/bin/env python3
"""
Dallas Intel — Motivated Seller Lead Scraper
=============================================
Sources
  NOFC     — Trustee sale / foreclosure notices  (Dallas County Clerk PublicSearch)
  CODE     — Code violations                      (Dallas OpenData Socrata API)
  TAXSALE  — Tax-delinquent sheriff sale list     (LGBS / dallascounty.org)
  LP       — Lis pendens PDF upload               (drag-and-drop on dashboard)
  BK       — Bankruptcy filings                   (CourtListener, Northern Dist TX)

Parcel enrichment via DCAD ArcGIS MapServer
Daily run via GitHub Actions → GitHub Pages dashboard
"""

import csv
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
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

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "30"))
LOOKBACK_DATE   = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
COURTLISTENER_TOKEN = os.getenv("COURTLISTENER_TOKEN", "")

# ---------------------------------------------------------------------------
# Data source URLs
# ---------------------------------------------------------------------------

# Dallas OpenData Socrata — Code Violations (dataset x9pz-kdq9)
DALLAS_CODE_URL = "https://www.dallasopendata.com/resource/x9pz-kdq9.json"

# Dallas County Clerk PublicSearch (same Neumo platform as Cuyahoga)
DALLAS_PUBLICSEARCH = "https://dallas.tx.publicsearch.us"

# LGBS tax sale list
LGBS_TAXSALE_URL = "http://taxsales.lgbs.com/dallas/list"

# DCAD parcel lookup (Dallas Central Appraisal District)
DCAD_URL = ("https://maps.dcad.org/prdwa/rest/services/"
            "Property/PropertySearch/MapServer/0/query")

# CourtListener — Northern District of Texas bankruptcy
CL_BK_URL   = "https://www.courtlistener.com/api/rest/v4/bankruptcy-information/"
CL_BK_COURT = "txnb"

# ---------------------------------------------------------------------------
# LP PDF column layout  (Dallas Recorder PDF — same Neumo platform as Cuyahoga)
# Columns: Grantor | Grantee | DocType | Date | DocNum | Legal | ParcelID | Address
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


# ===========================================================================
# Source: Code Violations — Dallas OpenData Socrata
# ===========================================================================

class DallasCodeScraper:
    """
    Pulls code violations from Dallas OpenData Socrata API.
    Dataset x9pz-kdq9 — updated daily, ~80k total records.
    We filter to the lookback window and structural/nuisance types.
    """

    # Violation type → severity mapping
    SEVERITY = {
        "Structural": "high",
        "Nuisance Abatement": "high",
        "Substandard Structure": "high",
        "Zoning": "medium",
        "Other": "low",
    }

    def fetch(self) -> list:
        log.info("Scraping Dallas code violations (Socrata)…")
        session = make_session()
        cutoff_str = LOOKBACK_DATE.strftime("%Y-%m-%dT%H:%M:%S")
        records = []
        offset = 0
        limit  = 1000

        while True:
            try:
                params = {
                    "$where":  f"created >= '{cutoff_str}'",
                    "$limit":  limit,
                    "$offset": offset,
                    "$order":  "created DESC",
                }
                r = session.get(DALLAS_CODE_URL, params=params, timeout=30)
                if r.status_code != 200:
                    log.warning("Socrata HTTP %d — %s", r.status_code, r.text[:200])
                    break
                batch = r.json()
                if not batch:
                    break
                log.info("Code violations page offset=%d → %d rows", offset, len(batch))
                for row in batch:
                    rec = self._to_record(row)
                    if rec:
                        records.append(rec)
                if len(batch) < limit:
                    break
                offset += limit
                time.sleep(0.3)
            except Exception as e:
                log.warning("Socrata error: %s", e)
                break

        log.info("Code violations: %d records", len(records))
        return records

    def _to_record(self, row: dict) -> Optional[dict]:
        # Build address from split columns: str_num + str_prefix + str_nam + str_suffix
        str_num    = str(row.get("str_num") or "").strip()
        str_prefix = (row.get("str_prefix") or "").strip()
        str_nam    = (row.get("str_nam") or "").strip()
        str_suffix = (row.get("str_suffix") or "").strip()
        address    = " ".join(filter(None, [str_num, str_prefix, str_nam, str_suffix])).title()
        if not address:
            return None

        # Type / severity — dataset uses "nuisance" field + "type" field
        nuisance = (row.get("nuisance") or "").strip()
        vtype    = (row.get("type") or nuisance or "Other").strip()
        # Structural types get high severity
        structural_kws = ("struct", "foundation", "roof", "wall", "unsafe", "substandard",
                          "demolish", "board", "vacant")
        if any(k in vtype.lower() for k in structural_kws):
            sev = "high"
        elif any(k in vtype.lower() for k in ("zoning", "permit")):
            sev = "medium"
        else:
            sev = "low"
        cat = "CODE_STRUCT" if sev == "high" else "CODE"

        filed_raw = (row.get("created") or row.get("created_date") or "")
        filed = ""
        if filed_raw:
            try:
                filed = datetime.fromisoformat(filed_raw[:10]).strftime("%m/%d/%Y")
            except Exception:
                filed = filed_raw[:10]

        # Zone column contains the zip code in this dataset
        zipcode = str(row.get("zone") or "").strip()
        city = "Dallas"

        return {
            "doc_num":      str(row.get("service_request_id") or row.get("service_request") or ""),
            "doc_type":     vtype.upper().replace(" ", "_")[:20],
            "cat":          cat,
            "cat_label":    "Substandard Structure" if sev == "high" else "Code Violation",
            "filed":        filed,
            "owner":        (row.get("owner_name") or "").strip().title(),
            "grantee":      "",
            "amount":       None,
            "legal":        (row.get("parcel_id") or row.get("account_number") or "").strip(),
            "clerk_url":    "",
            "prop_address": address,
            "prop_city":    city,
            "prop_state":   "TX",
            "prop_zip":     zipcode,
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":       "Dallas Code Enforcement",
            "neighborhood": (row.get("department") or ""),
            "viol_status":  (row.get("status") or "").strip(),
            "viol_desc":    nuisance or vtype,
            "viol_severity": sev,
            "delinquent":   False, "delinq_amt": "",
            "homestead":    None,
            "appraised":    "",
            "out_of_state": False,
            "luc":          "",
            "luc_desc":     "",
            "score":        0,
            "flags":        [],
        }


# ===========================================================================
# Source: NOFC — Trustee Sale / Foreclosure Notices
# Dallas County Clerk PublicSearch (same Neumo platform as Cuyahoga)
# ===========================================================================

class DallasNOFCScraper:
    """
    Scrapes trustee sale notices from dallas.tx.publicsearch.us.
    In Texas these are filed by substitute trustees, not the court,
    so they appear as doc_type NOFC (Notice of Foreclosure / Trustee Sale).
    """

    SEARCH_URL  = f"{DALLAS_PUBLICSEARCH}/results"
    # Doc type codes for Dallas — trustee sale notices
    DOC_TYPES   = ["TSN", "NOFC", "TS", "TRUSTSALE", "FORECLOSURE"]

    def fetch(self) -> list:
        log.info("Scraping Dallas foreclosure/trustee sale notices…")
        session = make_session()
        records = []
        cutoff  = LOOKBACK_DATE.date()

        for doc_type in self.DOC_TYPES:
            try:
                params = {
                    "documentType": doc_type,
                    "dateFrom":     cutoff.strftime("%m/%d/%Y"),
                    "dateTo":       date.today().strftime("%m/%d/%Y"),
                    "county":       "dallas",
                    "state":        "TX",
                }
                r = session.get(self.SEARCH_URL, params=params, timeout=30)
                if r.status_code != 200:
                    log.debug("NOFC %s HTTP %d", doc_type, r.status_code)
                    continue
                soup = BeautifulSoup(r.text, "lxml")
                rows = soup.select("table.results-table tr, div.result-row, tr[data-id]")
                log.info("NOFC %s → %d rows", doc_type, len(rows))
                for row in rows:
                    rec = self._parse_row(row, doc_type)
                    if rec:
                        records.append(rec)
                time.sleep(0.5)
            except Exception as e:
                log.warning("NOFC %s error: %s", doc_type, e)

        # Deduplicate by doc_num
        seen = set()
        deduped = []
        for r in records:
            key = r["doc_num"]
            if key not in seen:
                seen.add(key)
                deduped.append(r)

        log.info("NOFC: %d records (deduped)", len(deduped))
        return deduped

    def _parse_row(self, row, doc_type: str) -> Optional[dict]:
        try:
            cells = row.find_all(["td", "div"])
            if len(cells) < 3:
                return None
            texts = [c.get_text(strip=True) for c in cells]
            doc_num  = texts[0] if texts else ""
            filed    = texts[1] if len(texts) > 1 else ""
            grantor  = texts[2] if len(texts) > 2 else ""
            grantee  = texts[3] if len(texts) > 3 else ""
            amount   = None
            for t in texts:
                m = re.search(r"\$([\d,]+)", t)
                if m:
                    try:
                        amount = float(m.group(1).replace(",", ""))
                    except Exception:
                        pass
                    break
            address = ""
            for t in texts:
                if re.search(r"\d+\s+\w+\s+(st|ave|blvd|dr|rd|ln|ct|way|pl)\b", t, re.I):
                    address = t.strip().title()
                    break
            link = ""
            a = row.find("a", href=True)
            if a:
                link = DALLAS_PUBLICSEARCH + a["href"] if a["href"].startswith("/") else a["href"]

            if not doc_num and not grantor:
                return None

            return {
                "doc_num":      doc_num,
                "doc_type":     doc_type,
                "cat":          "NOFC",
                "cat_label":    "Notice of Foreclosure",
                "filed":        filed,
                "owner":        grantor.title(),
                "grantee":      grantee.title(),
                "amount":       amount,
                "legal":        "",
                "clerk_url":    link,
                "prop_address": address,
                "prop_city":    "Dallas",
                "prop_state":   "TX",
                "prop_zip":     "",
                "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":       "Court Docket",
                "neighborhood": "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":   False, "delinq_amt": "",
                "homestead":    None,
                "appraised":    "",
                "out_of_state": False,
                "luc":          "", "luc_desc":   "",
                "score":        0,
                "flags":        [],
            }
        except Exception as e:
            log.debug("NOFC row parse error: %s", e)
            return None


# ===========================================================================
# Source: TAXSALE — Delinquent Tax Sale List (LGBS)
# ===========================================================================

class DallasTaxSaleScraper:
    """
    Dallas County delinquent tax sale list published by LGBS.
    These are properties already headed to sheriff auction — very motivated.
    """

    def fetch(self) -> list:
        log.info("Scraping Dallas tax sale list (LGBS)…")
        session = make_session()
        records = []

        try:
            r = session.get(LGBS_TAXSALE_URL, timeout=30)
            if r.status_code != 200:
                log.warning("LGBS HTTP %d", r.status_code)
                return []
            soup = BeautifulSoup(r.text, "lxml")
            rows = soup.select("table tr, .property-row")
            log.info("Tax sale rows: %d", len(rows))
            for row in rows:
                rec = self._parse_row(row)
                if rec:
                    records.append(rec)
        except Exception as e:
            log.warning("Tax sale error: %s", e)

        log.info("Tax sale: %d records", len(records))
        return records

    def _parse_row(self, row) -> Optional[dict]:
        try:
            cells = row.find_all(["td", "div"])
            if len(cells) < 3:
                return None
            texts = [c.get_text(strip=True) for c in cells]
            suit_no  = texts[0] if texts else ""
            owner    = texts[1] if len(texts) > 1 else ""
            address  = texts[2] if len(texts) > 2 else ""
            amount   = None
            for t in texts:
                m = re.search(r"\$([\d,]+)", t)
                if m:
                    try:
                        amount = float(m.group(1).replace(",", ""))
                    except Exception:
                        pass
                    break
            if not suit_no and not owner:
                return None
            return {
                "doc_num":      f"TAXSALE-{suit_no}",
                "doc_type":     "TAXSALE",
                "cat":          "TAXSALE",
                "cat_label":    "Tax Sale",
                "filed":        date.today().strftime("%m/%d/%Y"),
                "owner":        owner.strip().title(),
                "grantee":      "Dallas County",
                "amount":       amount,
                "legal":        "",
                "clerk_url":    LGBS_TAXSALE_URL,
                "prop_address": address.strip().title(),
                "prop_city":    "Dallas",
                "prop_state":   "TX",
                "prop_zip":     "",
                "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "source":       "Tax Sale List",
                "neighborhood": "", "viol_status": "", "viol_desc": "",
                "viol_severity": "",
                "delinquent":   True,
                "delinq_amt":   f"{amount:,.2f}" if amount else "",
                "homestead":    None,
                "appraised":    "",
                "out_of_state": False,
                "luc":          "", "luc_desc":   "",
                "score":        0,
                "flags":        [],
            }
        except Exception as e:
            log.debug("Tax sale row parse: %s", e)
            return None


# ===========================================================================
# Source: LP — Lis Pendens PDF (manual upload, same as Cuyahoga)
# ===========================================================================

class LPScraper:
    def fetch(self) -> list:
        if not LP_PDF_PATH.exists():
            log.info("No LP PDF found at %s — skipping", LP_PDF_PATH)
            return []
        try:
            import pdfplumber as _pp
        except ImportError:
            log.warning("pdfplumber not installed — LP PDF skipped. pip install pdfplumber")
            return []

        log.info("Found LP PDF at %s — parsing …", LP_PDF_PATH)
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
        # Group words into lines by top coordinate
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
        grantor = row.get("grantor", "").strip()
        address = row.get("address", "").strip().title()
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

        # Parse city/state/zip from address
        city, state, zipcode = "Dallas", "TX", ""
        m = re.search(r",\s*([^,]+),\s*(TX|TEXAS)\s+(\d{5})", address, re.I)
        if m:
            city    = m.group(1).strip().title()
            zipcode = m.group(3)

        return {
            "doc_num":      docnum or f"LP-{grantor[:15].replace(' ', '')}",
            "doc_type":     "LP",
            "cat":          "LP",
            "cat_label":    "Lis Pendens",
            "filed":        filed,
            "owner":        grantor.title(),
            "grantee":      row.get("grantee", "").strip().title(),
            "amount":       None,
            "legal":        parcel,
            "clerk_url":    "",
            "prop_address": address,
            "prop_city":    city,
            "prop_state":   state,
            "prop_zip":     zipcode,
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":       "Manual (PDF)",
            "neighborhood": "", "viol_status": "", "viol_desc": "", "viol_severity": "",
            "delinquent":   False, "delinq_amt": "",
            "homestead":    None,
            "appraised":    "",
            "out_of_state": False,
            "luc":          "", "luc_desc":   "",
            "score":        0,
            "flags":        [],
        }


# ===========================================================================
# Source: BK — Bankruptcy (CourtListener, Northern District TX)
# ===========================================================================

class BankruptcyScraper:
    """
    Pulls Chapter 7 & 13 bankruptcy filings from CourtListener
    for the Northern District of Texas (txnb).
    Uses the same proven approach as the Cuyahoga scraper.
    """

    def fetch(self) -> list:
        if not COURTLISTENER_TOKEN:
            log.info("No COURTLISTENER_TOKEN — skipping BK")
            return []
        log.info("Scraping Northern District Texas bankruptcy filings…")
        session = make_session()
        session.headers.update({
            "Authorization": f"Token {COURTLISTENER_TOKEN}",
            "Accept": "application/json",
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
            "page_size":               100,
            "format":                  "json",
            "fields":                  "docket,chapter,date_filed",
        }
        next_url = CL_BK_URL
        page = 1
        fetched = 0
        while next_url:
            try:
                r = session.get(next_url,
                                params=params if page == 1 else None,
                                timeout=30)
                if r.status_code == 429:
                    log.warning("CourtListener rate limited — stopping BK")
                    break
                if r.status_code != 200:
                    log.warning("BK Ch.%s HTTP %d: %s", chapter, r.status_code, r.text[:200])
                    break
                data    = r.json()
                results = data.get("results", [])
                if not results:
                    break
                log.info("BK chapter %s page %d: %d results", chapter, page, len(results))
                for item in results:
                    rec = self._to_record(session, item, chapter)
                    if rec:
                        records.append(rec)
                        fetched += 1
                next_url = data.get("next")
                page += 1
                time.sleep(1)
            except Exception as e:
                log.warning("BK Ch.%s page %d error: %s", chapter, page, e)
                break
        return fetched

    def _to_record(self, session, item: dict, chapter: str) -> Optional[dict]:
        docket_data = item.get("docket") or {}

        # docket may be a URL string — fetch it for case name and docket number
        if isinstance(docket_data, str):
            try:
                r = session.get(docket_data,
                                params={"format": "json",
                                        "fields": "case_name,docket_number,date_filed,absolute_url"},
                                timeout=15)
                docket_data = r.json() if r.status_code == 200 else {}
            except Exception:
                docket_data = {}

        case_name = (docket_data.get("case_name") or "").strip()
        case_num  = (docket_data.get("docket_number") or "").strip()
        filed_raw = docket_data.get("date_filed") or item.get("date_filed") or ""
        abs_url   = docket_data.get("absolute_url", "")
        clerk_url = f"https://www.courtlistener.com{abs_url}" if abs_url else                     "https://www.courtlistener.com/recap/"

        if not case_name:
            return None

        # Strip "In re:" prefix common in bankruptcy case names
        name = re.sub(r"(?i)^in\s+re:?\s*", "", case_name).strip()
        name = re.sub(r"\s*\(.*?\)", "", name).strip()

        filed_fmt = ""
        if filed_raw:
            try:
                filed_fmt = datetime.strptime(filed_raw[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                filed_fmt = filed_raw[:10]

        return {
            "doc_num":      case_num or f"BK-{name[:20]}",
            "doc_type":     f"BK{chapter}",
            "cat":          "BK",
            "cat_label":    f"Bankruptcy Ch.{chapter}",
            "filed":        filed_fmt,
            "owner":        name,
            "grantee":      "",
            "amount":       None,
            "legal":        "",
            "clerk_url":    clerk_url,
            "prop_address": "",
            "prop_city":    "Dallas",
            "prop_state":   "TX",
            "prop_zip":     "",
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "source":       "Court Docket",
            "neighborhood": "", "viol_status": "", "viol_desc": "", "viol_severity": "",
            "delinquent":   False, "delinq_amt": "",
            "homestead":    None,
            "appraised":    "",
            "out_of_state": False,
            "luc":          "", "luc_desc": "",
            "bk_chapter":   chapter,
            "score":        0,
            "flags":        [],
        }


# ===========================================================================
# Parcel Enrichment — DCAD ArcGIS
# ===========================================================================

class ParcelLookup:
    """
    Enriches records with owner, address, homestead, and appraised value
    from the Dallas Central Appraisal District (DCAD) ArcGIS MapServer.
    """

    def __init__(self):
        self._by_address = {}
        self._session    = None

    def load(self):
        self._session = make_session()
        # Verify endpoint is reachable
        try:
            r = self._session.get(DCAD_URL, params={
                "where":            "ACCOUNT_NUM='00832550000000000'",
                "outFields":        "ACCOUNT_NUM,OWNER_NAME,SITUS_NUM,SITUS_STREET",
                "f":                "json",
                "resultRecordCount": 1,
                "returnGeometry":   "false",
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
            if not address and not parcel:
                continue
            match = self._lookup(address, parcel)
            if match:
                self._apply(rec, match)
                enriched += 1

        log.info("Parcel enrichment: %d/%d records matched", enriched, len(records))
        return records

    def _lookup(self, address: str, parcel: str) -> Optional[dict]:
        # Try parcel account number first, then address
        queries = []
        if parcel:
            clean_parcel = re.sub(r"[\s\-]", "", parcel)
            queries.append(f"ACCOUNT_NUM='{clean_parcel}'")
        if address:
            # Build address search query
            num_match = re.match(r"^(\d+)\s+(.+?)(?:,|$)", address)
            if num_match:
                num    = num_match.group(1)
                street = num_match.group(2).strip().upper().split(",")[0]
                queries.append(f"SITUS_NUM='{num}' AND SITUS_STREET LIKE '%{street[:20]}%'")

        for where in queries:
            try:
                params = {
                    "where":            where,
                    "outFields":        (
                        "ACCOUNT_NUM,OWNER_NAME,SITUS_NUM,SITUS_STREET,SITUS_CITY,"
                        "SITUS_ZIP,MAIL_ADDR1,MAIL_CITY,MAIL_STATE,MAIL_ZIP,"
                        "HOMESTEAD_EXEMPT,APPRAISED_VALUE,LUC,LUC_DESC"
                    ),
                    "f":                "json",
                    "resultRecordCount": 1,
                    "returnGeometry":   "false",
                }
                r = self._session.get(DCAD_URL, params=params, timeout=15)
                if r.status_code != 200:
                    continue
                data     = r.json()
                features = data.get("features") or []
                if not features:
                    continue
                attrs = features[0].get("attributes", {})
                log.debug("DCAD attrs sample: %s", dict(list(attrs.items())[:10]))
                return attrs
            except Exception as e:
                log.debug("DCAD lookup error: %s", e)
        return None

    def _apply(self, rec: dict, attrs: dict):
        # Property address
        situs_num    = str(attrs.get("SITUS_NUM") or "").strip()
        situs_street = (attrs.get("SITUS_STREET") or "").strip().title()
        if situs_num and situs_street and not rec.get("prop_address"):
            rec["prop_address"] = f"{situs_num} {situs_street}"
            rec["prop_city"]    = (attrs.get("SITUS_CITY") or "Dallas").strip().title()
            rec["prop_zip"]     = str(attrs.get("SITUS_ZIP") or "").strip()

        # Owner
        if not rec.get("owner"):
            rec["owner"] = (attrs.get("OWNER_NAME") or "").strip().title()

        # Mailing address
        mail_addr  = (attrs.get("MAIL_ADDR1")  or "").strip().title()
        mail_city  = (attrs.get("MAIL_CITY")   or "").strip().title()
        mail_state = (attrs.get("MAIL_STATE")  or "TX").strip().upper()
        mail_zip   = str(attrs.get("MAIL_ZIP") or "").strip()
        if mail_addr:
            rec["mail_address"] = mail_addr
            rec["mail_city"]    = mail_city
            rec["mail_state"]   = mail_state
            rec["mail_zip"]     = mail_zip
            rec["out_of_state"] = mail_state not in ("", "TX")

        # Homestead / appraised
        rec["homestead"] = bool(attrs.get("HOMESTEAD_EXEMPT"))
        appr = attrs.get("APPRAISED_VALUE")
        if appr:
            try:
                rec["appraised"] = f"${float(appr):,.0f}"
            except Exception:
                rec["appraised"] = str(appr)

        # LUC
        rec["luc"]      = str(attrs.get("LUC") or "").strip()
        rec["luc_desc"] = (attrs.get("LUC_DESC") or "").strip()

        # Legal / parcel
        acct = str(attrs.get("ACCOUNT_NUM") or "").strip()
        if acct and not rec.get("legal"):
            rec["legal"] = acct


# ===========================================================================
# Scoring
# ===========================================================================

class LeadScorer:
    WEEK_AGO = datetime.now() - timedelta(days=7)

    @staticmethod
    def score(rec: dict, all_recs: list) -> tuple:
        flags, points = [], 15   # Base 15 — spreads distribution
        cat   = rec.get("cat", "")
        dtype = rec.get("doc_type", "")
        owner = rec.get("owner", "")
        amt   = rec.get("amount")

        # --- Primary signal ---
        if cat == "LP":
            flags.append("Lis pendens");         points += 20
        if cat == "NOFC":
            flags.append("Pre-foreclosure");     points += 25
        if cat == "TAXSALE":
            flags.append("Tax sale scheduled");  points += 30  # Highest intent
        if cat == "JUD":
            flags.append("Judgment lien");       points += 15
        if cat == "CODE":
            flags.append("Code violation")
            points += 10
            sev = rec.get("viol_severity", "")
            if sev == "high":
                flags.append("Structural violation"); points += 15
            elif sev == "medium":
                points += 7
        if cat == "CODE_STRUCT":
            flags.append("Code violation");      points += 25  # Structural = high signal
        if cat == "BK":
            chapter = rec.get("bk_chapter", "")
            flags.append(f"Bankruptcy Ch.{chapter}" if chapter else "Bankruptcy")
            points += 20

        # --- Cross-signal stacking ---
        norm = normalize_name(owner)
        if norm:
            owner_cats = {r["cat"] for r in all_recs
                          if normalize_name(r.get("owner", "")) == norm}
            if "LP" in owner_cats and owner_cats & {"NOFC", "TAXSALE"}:
                points += 20
            if owner_cats & {"NOFC", "LP", "TAXSALE"} and owner_cats & {"CODE", "CODE_STRUCT"}:
                flags.append("Foreclosure + violation"); points += 25
            if len(owner_cats) >= 3:
                flags.append("Multi-hit owner"); points += 15

        # --- Debt size ---
        if amt:
            if   amt > 100_000: flags.append("High debt (>$100k)"); points += 15
            elif amt >  50_000: points += 8

        # --- Recency ---
        try:
            dt = datetime.strptime(rec.get("filed", "").strip(), "%m/%d/%Y")
            if dt >= LeadScorer.WEEK_AGO:
                flags.append("New this week"); points += 5
        except Exception:
            pass

        # --- Enrichment signals ---
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
                "Score":             r.get("score", 0),
                "Type":              r.get("cat_label", r.get("cat", "")),
                "Category":          r.get("cat", ""),
                "Filed Date":        r.get("filed", ""),
                "Owner Name":        r.get("owner", ""),
                "Property Address":  r.get("prop_address", ""),
                "Property City":     r.get("prop_city", ""),
                "Property State":    r.get("prop_state", "TX"),
                "Property Zip":      r.get("prop_zip", ""),
                "Mailing Address":   r.get("mail_address", ""),
                "Mailing City":      r.get("mail_city", ""),
                "Mailing State":     r.get("mail_state", "TX"),
                "Mailing Zip":       r.get("mail_zip", ""),
                "Amount":            r.get("amount") or "",
                "Appraised Value":   r.get("appraised", ""),
                "Delinquent":        "Yes" if r.get("delinquent") else "No",
                "Delinquency Amount": r.get("delinq_amt", ""),
                "Homestead":         "Yes" if r.get("homestead") else "No",
                "Out of State":      "Yes" if r.get("out_of_state") else "No",
                "Flags":             ", ".join(r.get("flags") or []),
                "Source":            r.get("source", ""),
                "Doc Number":        r.get("doc_num", ""),
                "Link":              r.get("clerk_url", ""),
            })
    log.info("GHL CSV exported: %s (%d rows)", path, len(records))


# ===========================================================================
# Main pipeline
# ===========================================================================

def main():
    log.info("Dallas Intel scraper starting — lookback %d days", LOOKBACK_DAYS)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list = []

    # 1. Code violations
    code_recs = DallasCodeScraper().fetch()
    all_records.extend(code_recs)
    log.info("Total after code violations: %d", len(all_records))

    # 2. Foreclosure / trustee sale notices
    nofc_recs = DallasNOFCScraper().fetch()
    all_records.extend(nofc_recs)
    log.info("Total after NOFC: %d", len(all_records))

    # 3. Tax sale list
    tax_recs = DallasTaxSaleScraper().fetch()
    all_records.extend(tax_recs)
    log.info("Total after tax sales: %d", len(all_records))

    # 4. LP PDF
    lp_recs = LPScraper().fetch()
    all_records.extend(lp_recs)
    log.info("Total after LP: %d", len(all_records))

    # 5. Bankruptcy
    bk_recs = BankruptcyScraper().fetch()
    all_records.extend(bk_recs)
    log.info("Total after BK: %d", len(all_records))

    # 6. Parcel enrichment
    lookup = ParcelLookup()
    lookup.load()
    all_records = lookup.enrich(all_records)

    # 7. Score
    scorer = LeadScorer()
    for rec in all_records:
        rec["score"], rec["flags"] = scorer.score(rec, all_records)

    # Sort by score desc
    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    hot = sum(1 for r in all_records if r.get("score", 0) >= 70)
    log.info("Scoring complete. Total=%d  Hot(≥70)=%d  Max=%d",
             len(all_records), hot,
             max((r.get("score", 0) for r in all_records), default=0))

    # 8. Write outputs
    RECORDS_JSON.write_text(json.dumps(all_records, indent=2, default=str), encoding="utf-8")
    DASH_JSON.write_text(json.dumps(all_records, indent=2, default=str), encoding="utf-8")
    export_ghl_csv(all_records, GHL_CSV)

    log.info("Dallas Intel complete. %d leads written.", len(all_records))


if __name__ == "__main__":
    main()
