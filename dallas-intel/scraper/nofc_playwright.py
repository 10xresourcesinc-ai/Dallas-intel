import asyncio, hashlib, logging, re
from datetime import datetime
from typing import Optional

log = logging.getLogger("dallas-intel")

START_DATE = "20260101"
END_DATE   = datetime.utcnow().strftime("%Y%m%d")
LIMIT      = 50
BASE_URL = (
    "https://dallas.tx.publicsearch.us/results"
    "?department=RP&searchType=quickSearch&searchValue=lis+pendens"
    f"&recordedDateRange={START_DATE},{END_DATE}&limit={LIMIT}"
)

def make_id(*parts):
    raw = "|".join(str(p).lower().strip() for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def today_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

async def _get_rows(page):
    return await page.evaluate("""() => {
        const results = [];
        for (const tr of document.querySelectorAll('tbody tr')) {
            const cb = tr.querySelector('input[id^="table-checkbox-"]');
            if (!cb) continue;
            const docId = cb.id.replace('table-checkbox-', '');
            const cell = i => { const el = tr.querySelector('.col-' + i + ' span'); return el ? el.innerText.trim() : ''; };
            results.push({ doc_id:docId, grantor:cell(3), grantee:cell(4), doc_type:cell(5), recorded_date:cell(6), doc_number:cell(7), city:cell(9), legal_desc:cell(10) });
        }
        const m = document.body.innerText.match(/([0-9][0-9,]+)\s+results/);
        return { rows: results, total: m ? m[1].replace(/,/g,'') : '0' };
    }""")

async def _fetch_page(playwright, offset, retries=3):
    for attempt in range(retries):
        browser = await playwright.chromium.launch(headless=True)
        try:
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = await context.new_page()
            await page.goto("https://dallas.tx.publicsearch.us/", wait_until="networkidle", timeout=20000)
            await asyncio.sleep(1)
            await page.goto(f"{BASE_URL}&offset={offset}", wait_until="networkidle", timeout=45000)
            for _ in range(20):
                data = await _get_rows(page)
                if data["rows"]: return data
                await asyncio.sleep(1)
            return await _get_rows(page)
        except Exception as e:
            log.warning("NOFC offset=%d attempt %d: %s", offset, attempt+1, e)
            await asyncio.sleep(3)
        finally:
            await browser.close()
    return {"rows": [], "total": "0"}

def _extract_address(legal_desc, city):
    m = re.search(r'\b(\d{3,5})\s+([A-Z][A-Z0-9 \.]{3,30}(?:ST|AVE|BLVD|DR|RD|LN|CT|PL|WAY|PKWY|CIR|TRL))\b', legal_desc, re.IGNORECASE)
    return m.group(0).strip() if m else (city or "")

async def _scrape_all():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("NOFC: run: pip install playwright && python -m playwright install chromium")
        return []
    all_leads = []
    total = 9999
    async with async_playwright() as p:
        offset, page_num = 0, 1
        while offset < total:
            log.info("NOFC: page %d offset=%d", page_num, offset)
            data = await _fetch_page(p, offset)
            if page_num == 1 and data["total"] != "0":
                total = int(data["total"])
                log.info("NOFC: total=%d", total)
            batch = data["rows"]
            log.info("NOFC: page %d rows=%d", page_num, len(batch))
            if not batch:
                break
            for r in batch:
                address = _extract_address(r.get("legal_desc",""), r.get("city",""))
                all_leads.append({
                    "lead_id": make_id("NOFC", r["doc_number"] or r["doc_id"]),
                    "source": "NOFC", "doc_id": r["doc_id"], "doc_number": r["doc_number"],
                    "doc_url": f"https://dallas.tx.publicsearch.us/doc/{r['doc_id']}",
                    "grantor": r["grantor"], "grantee": r["grantee"], "doc_type": r["doc_type"],
                    "recorded_date": r["recorded_date"], "address": address,
                    "city": r["city"], "state": "TX", "zip": "",
                    "legal_desc": r["legal_desc"], "owner_name": r["grantor"],
                    "signals": ["is_foreclosure"], "parcel": {}, "score": 0,
                    "fetched_at": today_str(),
                })
            offset += LIMIT; page_num += 1
            if offset < total: await asyncio.sleep(2)
    return all_leads

def fetch_nofc_playwright(start_date=None, end_date=None):
    global START_DATE, END_DATE, BASE_URL
    if start_date: START_DATE = start_date
    if end_date: END_DATE = end_date
    BASE_URL = (
        "https://dallas.tx.publicsearch.us/results"
        "?department=RP&searchType=quickSearch&searchValue=lis+pendens"
        f"&recordedDateRange={START_DATE},{END_DATE}&limit={LIMIT}"
    )
    log.info("NOFC: scraping %s to %s", START_DATE, END_DATE)
    leads = asyncio.run(_scrape_all())
    seen, deduped = set(), []
    for lead in leads:
        key = lead["doc_number"] or lead["doc_id"]
        if key not in seen:
            seen.add(key); deduped.append(lead)
    log.info("NOFC: %d leads (%d deduped)", len(leads), len(deduped))
    return deduped

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    leads = fetch_nofc_playwright()
    print(f"\n✅ {len(leads)} NOFC leads")
    for l in leads[:5]:
        print(f"  {l['grantor']:<35} {l['recorded_date']}  {l['doc_number']}  {l['city']}")
