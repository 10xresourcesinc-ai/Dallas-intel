#!/usr/bin/env python3
"""
Dallas Intel â€” Motivated Seller Lead Scraper
Fetches and scores leads from five Dallas-area sources:
  NOFC  â€” Dallas County Clerk trustee-sale PDFs
  CODE  â€” City of Dallas Open Data code violations (Socrata)
  CONDEMN â€” Unsafe/substandard structures (filtered from CODE)
  PRO   â€” Dallas County Probate Court (Odyssey portal)
  TAX   â€” DCAD tax delinquency (bulk CSV + ACT portal)

Output:
  data/leads_raw.json        â€” all raw leads
  data/leads_scored.json     â€” leads with scores + parcel data
  data/leads_ghl.csv         â€” GHL-ready import CSV
  data/run_meta.json         â€” run stats / timestamps
"""

import os
import re
import csv
import json
import time
import logging
import hashlib
import zipfile
import requests
import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Optional PDF parsing â€” only needed for NOFC
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Lis Pendens scraper (Playwright-based)
try:
    from nofc_playwright import fetch_nofc_playwright
    HAS_NOFC_PLAYWRIGHT = True
except ImportError:
    HAS_NOFC_PLAYWRIGHT = False

# Lis Pendens scraper (Playwright-based)
try:
    from nofc_playwright import fetch_nofc_playwright
    HAS_NOFC_PLAYWRIGHT = True
except ImportError:
    HAS_NOFC_PLAYWRIGHT = False

# â”€â”€ Logging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dallas-intel")

# â”€â”€ Paths â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
DCAD_ARCGIS_BASE = "https://maps.dcad.org/prdwa/rest/services/Property/ParcelQuery/MapServer"
DCAD_PARCEL_LAYER = 0          # adjust after inspecting service directory
SOCRATA_BASE = "https://www.dallasopendata.com/resource"
CODE_DATASET = "x9pz-kdq9"    # Code Violations
COURTS_PORTAL = "https://courtsportal.dallascounty.org/DALLASPROD"
FORECLOSURE_BASE = "https://www.dallascounty.org/department/countyclerk/media/foreclosure"
DCAD_BULK_BASE = "https://www.dallascad.org/ViewPDFs.aspx"

# Socrata app token â€” set via env var to avoid rate limiting
SOCRATA_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

# HTTP session with polite headers
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "DallasIntel/1.0 (motivated-seller-research; contact=ops@10xresources.com)",
    "Accept": "application/json",
})

CONDEMN_KEYWORDS = {
    "unsafe structure", "substandard", "demolition", "condemned",
    "structurally deficient", "imminent danger", "uninhabitable",
}

SFH_LAND_USE_CODES = {
    "A1",   # Single family residential
    "A2",   # Single family residential (other)
    "B1",   # Vacant residential lot
    "B2",   # Vacant lot
}

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Utilities
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def make_id(*parts: str) -> str:
    """Deterministic lead ID from concatenated string parts."""
    raw = "|".join(str(p).lower().strip() for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def get_json(url: str, params: dict = None, retries: int = 3, delay: float = 1.5) -> Optional[dict | list]:
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("GET %s attempt %d failed: %s", url, attempt + 1, exc)
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


def get_bytes(url: str, retries: int = 3) -> Optional[bytes]:
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content
        except Exception as exc:
            log.warning("GET bytes %s attempt %d failed: %s", url, attempt + 1, exc)
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


def today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def months_back(n: int) -> str:
    return (datetime.utcnow() - timedelta(days=30 * n)).strftime("%Y-%m-%dT00:00:00")


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# DCAD Parcel Enrichment
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def enrich_parcel(address: str) -> dict:
    """
    Query DCAD ArcGIS REST to get owner name, mailing address,
    appraised value, and land use code for a given situs address.
    Returns empty dict on failure.
    """
    if not address:
        return {}

    url = f"{DCAD_ARCGIS_BASE}/{DCAD_PARCEL_LAYER}/query"
    params = {
        "where": f"UPPER(SITUS_ADDR) LIKE UPPER('%{address.strip()}%')",
        "outFields": (
            "ACCOUNT_NUM,OWNER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP,"
            "APPRAISED_VALUE,LAND_VALUE,LAND_USE_CODE,PROP_TYPE_CODE,"
            "SITUS_ADDR,SITUS_CITY,SITUS_ZIP,YEAR_BUILT,SQFT"
        ),
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 1,
    }
    data = get_json(url, params)
    if not data or "features" not in data or not data["features"]:
        return {}

    attrs = data["features"][0].get("attributes", {})
    return {
        "dcad_account": attrs.get("ACCOUNT_NUM"),
        "owner_name": attrs.get("OWNER_NAME"),
        "mail_addr": attrs.get("MAIL_ADDR"),
        "mail_city": attrs.get("MAIL_CITY"),
        "mail_state": attrs.get("MAIL_STATE"),
        "mail_zip": attrs.get("MAIL_ZIP"),
        "appraised_value": attrs.get("APPRAISED_VALUE"),
        "land_value": attrs.get("LAND_VALUE"),
        "land_use_code": attrs.get("LAND_USE_CODE"),
        "prop_type_code": attrs.get("PROP_TYPE_CODE"),
        "situs_city": attrs.get("SITUS_CITY"),
        "situs_zip": attrs.get("SITUS_ZIP"),
        "year_built": attrs.get("YEAR_BUILT"),
        "sqft": attrs.get("SQFT"),
    }


def is_sfh_or_land(parcel: dict) -> bool:
    """Return True if parcel is single-family residential or vacant land."""
    code = (parcel.get("land_use_code") or "").upper()
    ptype = (parcel.get("prop_type_code") or "").upper()
    if code in SFH_LAND_USE_CODES:
        return True
    if ptype in ("R", "L"):   # Residential / Land
        return True
    return False


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Scoring Engine
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

SCORE_WEIGHTS = {
    # Signal                        points
    "is_tax_delinquent":            30,
    "is_code_violation":            15,
    "is_condemned":                 25,
    "is_probate":                   20,
    "is_foreclosure":               25,
    # Stacking bonuses (multi-signal)
    "multi_signal_2":               10,
    "multi_signal_3":               20,
    # Property characteristics
    "mail_addr_differs":            10,   # absentee owner
    "no_homestead_exemption":        8,
    "low_arv_under_100k":           10,
    "high_arv_over_300k":           -5,   # less motivated
    "vacant_land":                   5,
    "year_built_pre_1970":           5,
}

MAX_SCORE = sum(v for v in SCORE_WEIGHTS.values() if v > 0)


def score_lead(lead: dict) -> int:
    score = 0
    signals = lead.get("signals", [])

    for sig, pts in SCORE_WEIGHTS.items():
        if sig.startswith("multi_signal"):
            continue
        if sig in signals:
            score += pts

    # Multi-signal stacking
    primary = {"is_tax_delinquent", "is_code_violation", "is_condemned",
               "is_probate", "is_foreclosure"}
    hits = len(primary.intersection(set(signals)))
    if hits >= 3:
        score += SCORE_WEIGHTS["multi_signal_3"]
    elif hits >= 2:
        score += SCORE_WEIGHTS["multi_signal_2"]

    return min(score, MAX_SCORE)


def build_signals(lead: dict) -> list[str]:
    signals = list(lead.get("signals", []))
    parcel = lead.get("parcel", {})

    # Absentee owner â€” mail address differs from situs
    situs = (parcel.get("situs_city") or "").lower()
    mail_city = (parcel.get("mail_city") or "").lower()
    if situs and mail_city and situs != mail_city:
        signals.append("mail_addr_differs")

    arv = parcel.get("appraised_value") or 0
    if arv and arv < 100_000:
        signals.append("low_arv_under_100k")
    if arv and arv > 300_000:
        signals.append("high_arv_over_300k")

    luc = (parcel.get("land_use_code") or "").upper()
    if luc in ("B1", "B2"):
        signals.append("vacant_land")

    yr = parcel.get("year_built") or 0
    if yr and yr < 1970:
        signals.append("year_built_pre_1970")

    return list(set(signals))


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Source: CODE â€” Dallas Open Data Socrata API
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def fetch_code_violations(days_back: int = 90) -> list[dict]:
    """
    Fetch open code violation cases from Dallas Open Data.
    Returns list of normalized lead dicts.
    """
    log.info("Fetching CODE violations (last %d days)...", days_back)
    since = months_back(days_back // 30)

    headers = {}
    if SOCRATA_TOKEN:
        headers["X-App-Token"] = SOCRATA_TOKEN

    url = f"{SOCRATA_BASE}/{CODE_DATASET}.json"
    params = {
        "$where": f"created >= '{since}' AND completed_date IS NULL",
        "$limit": 5000,
        "$order": "created DESC",
        "$select": "created,updated,nuisance,str_num,str_pfx,str_name,str_suffix,zone,x_value,y_value",
    }

    data = get_json(url, params)
    if not data:
        log.error("CODE: no data returned")
        return []

    leads = []
    for row in data:
        nuisance = (row.get("nuisance") or "").lower()
        is_condemn = any(kw in nuisance for kw in CONDEMN_KEYWORDS)

        address = " ".join(filter(None, [
            row.get("str_num", ""),
            row.get("str_pfx", ""),
            row.get("str_name", ""),
            row.get("str_suffix", ""),
        ])).strip()

        lead_id = make_id("CODE", address, row.get("created", ""))
        signals = ["is_code_violation"]
        if is_condemn:
            signals.append("is_condemned")

        leads.append({
            "lead_id": lead_id,
            "source": "CONDEMN" if is_condemn else "CODE",
            "address": address,
            "city": "Dallas",
            "state": "TX",
            "zip": row.get("zone", ""),
            "lat": row.get("y_value"),
            "lon": row.get("x_value"),
            "violation_type": row.get("nuisance", ""),
            "created_date": row.get("created", "")[:10],
            "updated_date": row.get("updated", "")[:10],
            "signals": signals,
            "parcel": {},
            "score": 0,
            "fetched_at": today_str(),
        })

    log.info("CODE: %d total (%d condemned)", len(leads),
             sum(1 for l in leads if "is_condemned" in l["signals"]))
    return leads


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Source: TAX â€” DCAD Bulk CSV
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def fetch_tax_delinquent() -> list[dict]:
    """
    Download the current DCAD certified data ZIP and extract tax-delinquent
    SFH/land parcels. DCAD doesn't flag delinquency directly in the roll â€”
    we use: no homestead exemption + property class R/L as a proxy,
    then flag for manual ACT portal verification.
    """
    log.info("Fetching TAX delinquency candidates from DCAD bulk file...")

    # Current year certified comma-delimited file
    year = datetime.utcnow().year
    # DCAD ZIP URL pattern (confirmed from dataproducts.aspx)
    zip_url = (
        f"https://www.dallascad.org/ViewPDFs.aspx?type=3&id="
        r"\\DCAD.ORG\WEB\WEBDATA\WEBFORMS\DATA PRODUCTS\DCAD"
        f"{year}_CURRENT.ZIP"
    )
    # The ViewPDFs.aspx endpoint streams the ZIP â€” fetch raw bytes
    raw = get_bytes(zip_url)
    if not raw:
        log.error("TAX: could not download DCAD bulk ZIP")
        return []

    leads = []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            # DCAD ZIPs contain multiple CSVs; the main one is typically
            # REAL_ACCT.csv or similar â€” inspect namelist first
            names = zf.namelist()
            log.info("TAX ZIP contents: %s", names)

            # Find the real property account file
            acct_file = next(
                (n for n in names if "REAL_ACCT" in n.upper() or "ACCOUNT" in n.upper()),
                None
            )
            if not acct_file:
                log.warning("TAX: could not find account CSV in ZIP; files: %s", names)
                return []

            with zf.open(acct_file) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"))
                for row in reader:
                    prop_type = (row.get("PROP_TYPE_CODE") or row.get("prop_type_code") or "").upper()
                    exemptions = (row.get("EXEMPTIONS") or row.get("exemptions") or "").upper()
                    state_code = (row.get("STATE_CODE") or row.get("state_code") or "").upper()

                    # SFH + Land filter
                    if prop_type not in ("R", "L"):
                        continue

                    # No homestead = potentially absentee / delinquency risk
                    has_homestead = "HS" in exemptions
                    signals = ["is_tax_delinquent"]
                    if not has_homestead:
                        signals.append("no_homestead_exemption")

                    account = row.get("ACCOUNT_NUM") or row.get("account_num", "")
                    address = row.get("SITUS_ADDR") or row.get("situs_addr", "")
                    owner = row.get("OWNER_NAME") or row.get("owner_name", "")

                    lead_id = make_id("TAX", account)
                    leads.append({
                        "lead_id": lead_id,
                        "source": "TAX",
                        "dcad_account": account,
                        "address": address,
                        "city": row.get("SITUS_CITY") or row.get("situs_city", ""),
                        "state": "TX",
                        "zip": row.get("SITUS_ZIP") or row.get("situs_zip", ""),
                        "owner_name": owner,
                        "mail_addr": row.get("MAIL_ADDR") or row.get("mail_addr", ""),
                        "appraised_value": row.get("APPRAISED_VALUE") or row.get("appraised_value"),
                        "prop_type": prop_type,
                        "exemptions": exemptions,
                        "signals": signals,
                        "parcel": {},
                        "score": 0,
                        "fetched_at": today_str(),
                        # Flag for ACT portal delinquency verification
                        "needs_tax_verification": True,
                    })

    except zipfile.BadZipFile as exc:
        log.error("TAX: bad ZIP file: %s", exc)
        return []

    log.info("TAX: %d SFH/Land candidates extracted", len(leads))
    return leads


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Source: NOFC â€” Dallas County Clerk Trustee Sale PDFs
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Regex patterns for trustee sale PDF parsing
RE_ADDRESS = re.compile(
    r"\b(\d{1,5})\s+([A-Z][A-Z0-9 \.\-']+(?:ST|AVE|BLVD|DR|RD|LN|CT|PL|WAY|PKWY|HWY|FWY|CIR|TRL))\b",
    re.IGNORECASE,
)
RE_SALE_DATE = re.compile(
    r"(?:sale date|date of sale)[:\s]+([A-Z]+ \d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)
RE_TRUSTEE = re.compile(
    r"(?:substitute trustee|trustee)[:\s]+([A-Z][A-Z\s,\.]+(?:LLC|INC|CORP|PLLC|LLP)?)",
    re.IGNORECASE,
)


def get_foreclosure_pdf_urls(month: str = None) -> list[tuple[str, str]]:
    """
    Scrape the County Clerk foreclosure page for PDF URLs.
    Returns list of (city, url) tuples.
    month: e.g. 'April', defaults to current month.
    """
    if not month:
        month = datetime.utcnow().strftime("%B")   # e.g. "April"

    page_url = "https://www.dallascounty.org/government/county-clerk/recording/foreclosures.php"
    resp = SESSION.get(page_url, timeout=30)
    if not resp.ok:
        log.error("NOFC: could not load foreclosure page")
        return []

    # Extract PDF links matching the target month
    pattern = re.compile(
        rf"href=['\"]([^'\"]+/foreclosure/{month}/[^'\"]+\.pdf)['\"]",
        re.IGNORECASE,
    )
    matches = pattern.findall(resp.text)
    results = []
    for path in matches:
        if path.startswith("http"):
            url = path
        else:
            url = "https://www.dallascounty.org" + path
        # Extract city from filename: Dallas_1.pdf â†’ Dallas
        fname = Path(path).stem
        city = fname.split("_")[0].replace("-", " ").title()
        results.append((city, url))

    log.info("NOFC: found %d PDFs for %s", len(results), month)
    return results


def parse_foreclosure_pdf(pdf_bytes: bytes, city: str) -> list[dict]:
    """Parse a trustee sale PDF and extract lead records."""
    if not HAS_PDFPLUMBER:
        log.warning("NOFC: pdfplumber not installed â€” skipping PDF parse")
        return []

    leads = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as exc:
        log.warning("NOFC: PDF parse error: %s", exc)
        return []

    # Split into individual notices (each notice separated by blank lines or rule)
    # DCAD PDFs vary; try splitting on common delimiters
    blocks = re.split(r"\n{3,}|_{10,}", full_text)

    for block in blocks:
        if len(block.strip()) < 40:
            continue

        addr_match = RE_ADDRESS.search(block)
        if not addr_match:
            continue

        address = addr_match.group(0).strip()
        sale_date = ""
        sd = RE_SALE_DATE.search(block)
        if sd:
            sale_date = sd.group(1).strip()

        trustee = ""
        tr = RE_TRUSTEE.search(block)
        if tr:
            trustee = tr.group(1).strip()[:80]

        lead_id = make_id("NOFC", address, city, sale_date)
        leads.append({
            "lead_id": lead_id,
            "source": "NOFC",
            "address": address,
            "city": city,
            "state": "TX",
            "zip": "",
            "sale_date": sale_date,
            "trustee": trustee,
            "raw_block": block[:500],
            "signals": ["is_foreclosure"],
            "parcel": {},
            "score": 0,
            "fetched_at": today_str(),
        })

    return leads


def fetch_nofc(months: int = 2) -> list[dict]:
    """Fetch trustee sale PDFs for the past N months and parse leads."""
    leads = []
    now = datetime.utcnow()
    target_months = []
    for i in range(months):
        dt = now - timedelta(days=30 * i)
        target_months.append(dt.strftime("%B"))

    for month in target_months:
        pdf_urls = get_foreclosure_pdf_urls(month)
        for city, url in pdf_urls:
            raw = get_bytes(url)
            if not raw:
                log.warning("NOFC: could not fetch %s", url)
                continue
            parsed = parse_foreclosure_pdf(raw, city)
            leads.extend(parsed)
            time.sleep(0.3)   # polite crawl

    log.info("NOFC: %d leads parsed", len(leads))
    return leads


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Source: PRO â€” Dallas County Probate Courts (Odyssey portal)
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def fetch_probate(days_back: int = 60) -> list[dict]:
    """
    Scrape Dallas County Probate Courts via the Odyssey portal.
    Searches for estate/probate cases filed in the last N days.

    The Dallas Odyssey portal is at courtsportal.dallascounty.org/DALLASPROD
    County and Probate Courts use NodeID=200 (adjust if needed).
    """
    log.info("Fetching PRO probate cases (last %d days)...", days_back)

    # Odyssey portals expose a SmartSearch JSON endpoint
    search_url = f"{COURTS_PORTAL}/Home/Dashboard/26"   # NodeId 26 = County/Probate
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    today = datetime.utcnow().strftime("%m/%d/%Y")

    payload = {
        "NodeID": "26",
        "NodeDesc": "County and Probate Courts",
        "CaseSearchMode": "CaseNumber",
        "StatusType": "A",   # Active
        "DateOfBirth": "",
        "LastName": "",
        "FirstName": "",
        "cboState": "AA",
        "btnSSSubmit": "Search",
        "FiledDateFrom": since,
        "FiledDateTo": today,
        "CaseCategories": "PR",   # Probate
        "CaseTypeID": "",
        "StatusID": "",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": f"{COURTS_PORTAL}/Home/Dashboard/26",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        resp = SESSION.post(
            f"{COURTS_PORTAL}/Home/Dashboard/26",
            data=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("PRO: portal request failed: %s", exc)
        # Fall back â€” return empty so other sources still run
        log.info("PRO: skipping (portal scrape needs session cookie investigation)")
        return []

    leads = []
    cases = data if isinstance(data, list) else data.get("cases", [])
    for case in cases:
        case_num = case.get("CaseNumber") or case.get("caseNumber", "")
        filed_date = case.get("FiledDate") or case.get("filedDate", "")
        parties = case.get("Parties") or case.get("parties", [])

        # Estate of <deceased> â€” usually first party
        decedent = ""
        for p in parties:
            role = (p.get("ConnectionType") or p.get("connectionType") or "").lower()
            if "decedent" in role or "deceased" in role or "estate" in role:
                decedent = p.get("FullName") or p.get("fullName", "")
                break

        lead_id = make_id("PRO", case_num)
        leads.append({
            "lead_id": lead_id,
            "source": "PRO",
            "case_number": case_num,
            "decedent_name": decedent,
            "filed_date": filed_date,
            "case_type": case.get("CaseType") or case.get("caseType", ""),
            "address": "",    # enriched via DCAD after owner name lookup
            "city": "Dallas",
            "state": "TX",
            "zip": "",
            "signals": ["is_probate"],
            "parcel": {},
            "score": 0,
            "fetched_at": today_str(),
        })

    log.info("PRO: %d probate cases fetched", len(leads))
    return leads


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Parcel Enrichment Pass
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def enrich_all(leads: list[dict], max_enrich: int = 500) -> list[dict]:
    """
    Run DCAD parcel enrichment on leads that have an address but no parcel data.
    Caps at max_enrich to avoid hammering the ArcGIS endpoint.
    TAX leads already have parcel data embedded from the bulk file.
    """
    enrichable = [l for l in leads if l.get("address") and not l["parcel"] and l["source"] != "TAX"]
    enrichable = enrichable[:max_enrich]

    log.info("Enriching %d leads via DCAD ArcGIS...", len(enrichable))
    enriched = 0
    for lead in enrichable:
        parcel = enrich_parcel(lead["address"])
        if parcel:
            lead["parcel"] = parcel
            # Promote key fields to top level for GHL export
            if not lead.get("owner_name"):
                lead["owner_name"] = parcel.get("owner_name", "")
            if not lead.get("zip") and parcel.get("situs_zip"):
                lead["zip"] = parcel.get("situs_zip", "")
            enriched += 1
        time.sleep(0.2)   # polite

    log.info("Enrichment complete: %d/%d parcels found", enriched, len(enrichable))
    return leads


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# SFH + Land Filter
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def filter_sfh_land(leads: list[dict]) -> list[dict]:
    """
    Keep only SFH and vacant land parcels.
    Leads with no parcel data pass through (can't filter what we don't know).
    TAX leads are pre-filtered at extraction.
    """
    out = []
    for lead in leads:
        if lead["source"] == "TAX":
            out.append(lead)
            continue
        parcel = lead.get("parcel", {})
        if not parcel:
            out.append(lead)   # pass through unknowns
            continue
        if is_sfh_or_land(parcel):
            out.append(lead)

    removed = len(leads) - len(out)
    log.info("SFH/Land filter: kept %d, removed %d", len(out), removed)
    return out


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Deduplication
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def deduplicate(leads: list[dict]) -> list[dict]:
    """
    Deduplicate by lead_id. When the same property appears in multiple
    sources, merge signals rather than dropping the duplicate.
    """
    seen: dict[str, dict] = {}
    for lead in leads:
        lid = lead["lead_id"]
        if lid not in seen:
            seen[lid] = lead
        else:
            # Merge signals from duplicate
            existing_signals = set(seen[lid]["signals"])
            new_signals = set(lead["signals"])
            merged = list(existing_signals | new_signals)
            seen[lid]["signals"] = merged
            # Keep the richer source label
            seen[lid]["source"] = seen[lid]["source"] + "+" + lead["source"]

    result = list(seen.values())
    log.info("Dedup: %d â†’ %d leads", len(leads), len(result))
    return result


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# GHL CSV Export
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

GHL_FIELDS = [
    "lead_id", "source", "score",
    "address", "city", "state", "zip",
    "owner_name", "mail_addr", "mail_city", "mail_state", "mail_zip",
    "appraised_value", "year_built", "sqft",
    "violation_type", "sale_date", "case_number",
    "signals",
    "fetched_at",
    # GHL standard fields
    "first_name", "last_name", "phone", "email",
]


def to_ghl_row(lead: dict) -> dict:
    parcel = lead.get("parcel", {})
    row = {f: "" for f in GHL_FIELDS}
    row.update({
        "lead_id": lead.get("lead_id", ""),
        "source": lead.get("source", ""),
        "score": lead.get("score", 0),
        "address": lead.get("address", ""),
        "city": lead.get("city", ""),
        "state": lead.get("state", "TX"),
        "zip": lead.get("zip", "") or parcel.get("situs_zip", ""),
        "owner_name": lead.get("owner_name", "") or parcel.get("owner_name", ""),
        "mail_addr": lead.get("mail_addr", "") or parcel.get("mail_addr", ""),
        "mail_city": parcel.get("mail_city", ""),
        "mail_state": parcel.get("mail_state", ""),
        "mail_zip": parcel.get("mail_zip", ""),
        "appraised_value": lead.get("appraised_value") or parcel.get("appraised_value", ""),
        "year_built": parcel.get("year_built", ""),
        "sqft": parcel.get("sqft", ""),
        "violation_type": lead.get("violation_type", ""),
        "sale_date": lead.get("sale_date", ""),
        "case_number": lead.get("case_number", ""),
        "signals": "|".join(lead.get("signals", [])),
        "fetched_at": lead.get("fetched_at", ""),
    })
    # Split owner name into first/last for GHL contact import
    name = row["owner_name"].strip()
    if "," in name:
        parts = name.split(",", 1)
        row["last_name"] = parts[0].strip().title()
        row["first_name"] = parts[1].strip().title() if len(parts) > 1 else ""
    elif " " in name:
        parts = name.rsplit(" ", 1)
        row["first_name"] = parts[0].strip().title()
        row["last_name"] = parts[1].strip().title()
    else:
        row["last_name"] = name.title()

    return row


def export_ghl_csv(leads: list[dict], path: Path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(to_ghl_row(lead))
    log.info("GHL CSV: %d rows â†’ %s", len(leads), path)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Main orchestration
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def run():
    start = datetime.utcnow()
    log.info("=== Dallas Intel scrape started %s ===", start.isoformat())

    all_leads = []

    # â”€â”€ Fetch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # CODE + CONDEMN (same API call, split by signal)
    code_leads = fetch_code_violations(days_back=90)
    all_leads.extend(code_leads)

    # TAX
    tax_leads = fetch_tax_delinquent()
    all_leads.extend(tax_leads)

    # NOFC
    nofc_leads = fetch_nofc_playwright() if HAS_NOFC_PLAYWRIGHT else fetch_nofc(months=2)
    all_leads.extend(nofc_leads)

    # PRO
    pro_leads = fetch_probate(days_back=60)
    all_leads.extend(pro_leads)

    log.info("Raw leads: %d", len(all_leads))

    # â”€â”€ Save raw â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    raw_path = DATA_DIR / "leads_raw.json"
    raw_path.write_text(json.dumps(all_leads, indent=2, default=str))
    log.info("Saved raw leads â†’ %s", raw_path)

    # â”€â”€ Enrich â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_leads = enrich_all(all_leads)

    # â”€â”€ Filter â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_leads = filter_sfh_land(all_leads)

    # â”€â”€ Dedup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_leads = deduplicate(all_leads)

    # â”€â”€ Score â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for lead in all_leads:
        lead["signals"] = build_signals(lead)
        lead["score"] = score_lead(lead)

    # Sort by score descending
    all_leads.sort(key=lambda l: l["score"], reverse=True)

    log.info("Final leads: %d (top score: %s)",
             len(all_leads), all_leads[0]["score"] if all_leads else "N/A")

    # â”€â”€ Save scored â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    scored_path = DATA_DIR / "leads_scored.json"
    scored_path.write_text(json.dumps(all_leads, indent=2, default=str))
    log.info("Saved scored leads â†’ %s", scored_path)

    # â”€â”€ GHL export â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ghl_path = DATA_DIR / "leads_ghl.csv"
    export_ghl_csv(all_leads, ghl_path)

    # â”€â”€ Run metadata â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    end = datetime.utcnow()
    meta = {
        "run_date": today_str(),
        "run_start": start.isoformat(),
        "run_end": end.isoformat(),
        "duration_seconds": (end - start).seconds,
        "total_leads": len(all_leads),
        "by_source": {
            src: sum(1 for l in all_leads if l["source"] == src)
            for src in ["CODE", "CONDEMN", "TAX", "NOFC", "PRO"]
        },
        "score_distribution": {
            "80+": sum(1 for l in all_leads if l["score"] >= 80),
            "60-79": sum(1 for l in all_leads if 60 <= l["score"] < 80),
            "40-59": sum(1 for l in all_leads if 40 <= l["score"] < 60),
            "under_40": sum(1 for l in all_leads if l["score"] < 40),
        },
    }
    meta_path = DATA_DIR / "run_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    log.info("Run meta â†’ %s", meta_path)
    log.info("=== Done. %d leads in %ds ===", len(all_leads), meta["duration_seconds"])


if __name__ == "__main__":
    run()

