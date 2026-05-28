"""
scrape_nofc.py — Lis Pendens / NOFC scraper for dallas.tx.publicsearch.us
Uses Playwright to run a real browser, waits for React to render records.
Usage: python scrape_nofc.py
Output: nofc_raw.json
"""

import asyncio
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright

START_DATE = "20260101"
END_DATE   = "20260528"
LIMIT      = 50
OUT_FILE   = "C:/nofc_raw.json"

BASE_URL = (
    "https://dallas.tx.publicsearch.us/results"
    "?department=RP"
    "&searchType=quickSearch"
    "&searchValue=lis+pendens"
    f"&recordedDateRange={START_DATE},{END_DATE}"
    f"&limit={LIMIT}"
)

async def scrape():
    all_leads = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # ── Get total count from first page ────────────────────────────────────
        url = f"{BASE_URL}&offset=0"
        print(f"[1] Loading: {url}")
        await page.goto(url, wait_until="networkidle", timeout=30000)

        # Wait for results to render — look for the results count header
        try:
            await page.wait_for_selector("text=results", timeout=15000)
        except:
            print("  Warning: 'results' text not found, proceeding anyway")

        # Grab total count
        total = 0
        try:
            count_text = await page.inner_text("body")
            match = re.search(r'(\d[\d,]*)\s+results', count_text)
            if match:
                total = int(match.group(1).replace(",", ""))
                print(f"  Total records: {total}")
        except:
            pass

        if total == 0:
            print("  Could not detect total — will scrape until empty pages")
            total = 9999

        # ── Scrape all pages ───────────────────────────────────────────────────
        offset = 0
        page_num = 1

        while offset < total:
            if offset > 0:
                url = f"{BASE_URL}&offset={offset}"
                print(f"[{page_num}] offset={offset}  Loading...")
                await page.goto(url, wait_until="networkidle", timeout=30000)
                try:
                    await page.wait_for_selector("text=results", timeout=15000)
                except:
                    pass
                await asyncio.sleep(0.8)

            # ── Extract rows from the rendered DOM ─────────────────────────────
            rows = await page.evaluate("""() => {
                const leads = [];

                // Try table rows first
                const trs = document.querySelectorAll('tr');
                for (const tr of trs) {
                    const link = tr.querySelector('a[href*="/doc/"]');
                    if (!link) continue;

                    const docMatch = link.getAttribute('href').match(/\\/doc\\/(\\d+)/);
                    const docId = docMatch ? docMatch[1] : '';

                    const cells = Array.from(tr.querySelectorAll('td'))
                        .map(td => td.innerText.trim().replace(/\\s+/g, ' '));

                    if (cells.length > 0) {
                        leads.push({ docId, cells });
                    }
                }

                // Fallback: any clickable result rows/divs with doc links
                if (leads.length === 0) {
                    const links = document.querySelectorAll('a[href*="/doc/"]');
                    for (const link of links) {
                        const docMatch = link.getAttribute('href').match(/\\/doc\\/(\\d+)/);
                        const docId = docMatch ? docMatch[1] : '';
                        const row = link.closest('[class*="result"],[class*="row"],[class*="record"],li,tr') || link.parentElement;
                        const text = row ? row.innerText.trim() : link.innerText.trim();
                        leads.push({ docId, cells: [text] });
                    }
                }

                return leads;
            }""")

            print(f"  Rows extracted: {len(rows)}")

            if len(rows) == 0:
                # Debug: show what's on the page
                body_text = await page.inner_text("body")
                print(f"  Page text snippet: {body_text[:300]}")
                print("  No rows found — stopping")
                break

            for row in rows:
                cells = row["cells"]
                lead = {
                    "source":        "NOFC",
                    "doc_id":        row["docId"],
                    "grantor":       cells[0] if len(cells) > 0 else "",
                    "grantee":       cells[1] if len(cells) > 1 else "",
                    "doc_type":      cells[2] if len(cells) > 2 else "",
                    "recorded_date": cells[3] if len(cells) > 3 else "",
                    "doc_number":    cells[4] if len(cells) > 4 else "",
                    "book_volume":   cells[5] if len(cells) > 5 else "",
                    "doc_url":       f"https://dallas.tx.publicsearch.us/doc/{row['docId']}",
                    "scraped_at":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                }
                all_leads.append(lead)

            offset    += LIMIT
            page_num  += 1
            await asyncio.sleep(1.0)

        await browser.close()

    # Deduplicate by doc_id
    seen = set()
    deduped = []
    for lead in all_leads:
        if lead["doc_id"] not in seen:
            seen.add(lead["doc_id"])
            deduped.append(lead)

    print(f"\n✅ Total scraped: {len(all_leads)}  |  After dedup: {len(deduped)}")

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(deduped, f, indent=2, ensure_ascii=False)
    print(f"   Saved → {OUT_FILE}")

if __name__ == "__main__":
    asyncio.run(scrape())
