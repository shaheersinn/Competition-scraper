"""
CanLII Website Scraper — no API key required

Scrapes canlii.org directly using Playwright to render JS pages.
This is the PRIMARY comprehensive source for all Canadian competition case law.

HOW CANLII BROWSE WORKS (no API):
  canlii.org/en/ca/cact/     → Competition Tribunal (all decisions)
  canlii.org/en/ca/fct/      → Federal Court
  canlii.org/en/ca/fca/      → Federal Court of Appeal
  canlii.org/en/ca/scc/      → Supreme Court of Canada
  canlii.org/en/ab/abca/     → Alberta Court of Appeal
  canlii.org/en/on/onca/     → Ontario Court of Appeal
  canlii.org/en/bc/bcca/     → BC Court of Appeal
  canlii.org/en/qc/qcca/     → Quebec Court of Appeal

  Each database page lists cases with JS-rendered pagination.
  Individual case pages have full HTML text — no authentication required.

URL PATTERN for individual cases:
  canlii.org/en/ca/cact/doc/2024/2024cact5/2024cact5.html
  canlii.org/en/ca/fct/doc/2023/2023fc1234/2023fc1234.html

STRATEGY:
  1. For small databases (CT, ~600 total decisions): browse ALL cases year by year
  2. For large databases (FC 100k+, SCC 80k+): search for competition terms,
     then fetch each matching case page for full text
  3. RSS feeds for incremental updates (last 50 decisions per court)
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser

from .models import CaseRecord, DocumentRecord, PartyRecord
from .utils import (
    abs_url,
    download_file,
    extract_html_text,
    extract_pdf_text,
    filename_from_url,
    is_document_url,
    safe_get,
    session,
    slugify,
)

logger = logging.getLogger(__name__)

CANLII = "https://www.canlii.org"

# ─────────────────────────────────────────────────────────────────────────────
# Database registry
# scrape_all=True  → every single case (small DB like CT)
# scrape_all=False → competition keyword filter (large DBs)
# ─────────────────────────────────────────────────────────────────────────────
DATABASES = [
    # (db_id,   province, source_name,            description,              scrape_all)
    ("cact",  "ca", "canlii_tribunal",        "Competition Tribunal",    True),
    ("fct",   "ca", "canlii_federal_court",   "Federal Court",           False),
    ("fca",   "ca", "canlii_fca",             "Federal Court of Appeal", False),
    ("scc",   "ca", "canlii_scc",             "Supreme Court of Canada", False),
    ("onca",  "on", "canlii_onca",            "Ontario Court of Appeal", False),
    ("bcca",  "bc", "canlii_bcca",            "BC Court of Appeal",      False),
    ("abca",  "ab", "canlii_abca",            "Alberta Court of Appeal", False),
    ("qcca",  "qc", "canlii_qcca",            "Quebec Court of Appeal",  False),
]

# RSS feeds — incremental updates for recent decisions
RSS_FEEDS = [
    ("https://decisions.ct-tc.gc.ca/ct-tc/cdo/en/rss.xml",        "canlii_tribunal"),
    ("https://decisions.fca-caf.gc.ca/fct-cf/en/rss.xml",         "canlii_federal_court"),
    ("https://decisions.fca-caf.gc.ca/fca-caf/en/rss.xml",        "canlii_fca"),
    ("https://decisions.scc-csc.ca/scc-csc/scc-csc/en/rss.xml",   "canlii_scc"),
]

COMPETITION_RE = re.compile(
    r"competition act|combines investigation|competition bureau|competition tribunal|"
    r"commissioner of competition|director of investigation|"
    r"abuse of dominance|price.?fixing|bid.?rigg|predatory pric|"
    r"market power|dominant position|merger.*competition|monopol|cartel|"
    r"refusal to (deal|supply)|exclusive dealing|tied selling|"
    r"misleading advertising|deceptive marketing|"
    r"\bs\.\s*7[456789]\b|\bs\.\s*9[01]\b|\bs\.\s*79\b",
    re.I | re.DOTALL,
)

# ─────────────────────────────────────────────────────────────────────────────
# Playwright helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _new_browser(playwright):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent="CompetitionCaseCa-Scraper/4.0 (legal research; admin@competitioncase.ca)",
        locale="en-CA",
    )
    return browser, context


async def _safe_goto(page: Page, url: str, retries: int = 3) -> bool:
    """Navigate with retry. Returns True on success."""
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until="networkidle", timeout=90000)
            await asyncio.sleep(2)
            return True
        except Exception as exc:
            logger.warning("[CanLII] goto attempt %d failed %s: %s", attempt + 1, url, exc)
            if attempt < retries - 1:
                await asyncio.sleep(3 * (attempt + 1))
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 1: Browse ALL cases in a CanLII database by year
# Used for Competition Tribunal (small corpus, ~600 total decisions)
# ─────────────────────────────────────────────────────────────────────────────

async def _browse_all_year(page: Page, db_id: str, province: str, year: int) -> list[str]:
    """Collect all case URLs for a given year from CanLII database browse page."""
    # CanLII uses hash-fragment routing for year filtering
    browse_url = f"{CANLII}/en/{province}/{db_id}/#!dateDecided%3A{year}-01-01%2C{year}-12-31"
    urls: set[str] = set()

    ok = await _safe_goto(page, browse_url)
    if not ok:
        return []

    # Wait for case list to render
    try:
        await page.wait_for_selector(
            "a[href*='/doc/'], .result-title a, .canlii-decision a",
            timeout=20000,
        )
    except Exception:
        pass  # May be 0 cases for this year

    # Scroll to load all lazy-loaded results
    prev_count = 0
    for _ in range(10):  # up to 10 scroll cycles
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.5)
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        current = {
            abs_url(CANLII, a["href"])
            for a in soup.find_all("a", href=True)
            if f"/en/{province}/{db_id}/doc/" in a.get("href", "")
        }
        urls.update(current)
        if len(urls) == prev_count:
            break  # No new results loaded
        prev_count = len(urls)

    # Also check for "Load more" / pagination button
    try:
        load_more = page.locator("button:has-text('Load more'), a:has-text('Next'), .loadMore")
        while await load_more.first.is_visible():
            await load_more.first.click()
            await asyncio.sleep(2)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                if f"/en/{province}/{db_id}/doc/" in a["href"]:
                    full = abs_url(CANLII, a["href"])
                    if full:
                        urls.add(full)
    except Exception:
        pass

    return sorted(urls)


async def _discover_all_cases(db_id: str, province: str, start_year: int, end_year: int) -> list[str]:
    """Discover all case URLs for a small database (like CT) by iterating years."""
    all_urls: set[str] = set()
    async with async_playwright() as p:
        browser, context = await _new_browser(p)
        page = await context.new_page()

        for year in range(end_year, start_year - 1, -1):
            logger.info("[CanLII browse] %s year %d", db_id.upper(), year)
            year_urls = await _browse_all_year(page, db_id, province, year)
            all_urls.update(year_urls)
            logger.info("[CanLII browse] %s year %d: %d links", db_id.upper(), year, len(year_urls))

        # Also grab the main database page (no year filter) in case it lists recent cases
        main_url = f"{CANLII}/en/{province}/{db_id}/"
        ok = await _safe_goto(page, main_url)
        if ok:
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                if f"/en/{province}/{db_id}/doc/" in a["href"]:
                    full = abs_url(CANLII, a["href"])
                    if full:
                        all_urls.add(full)

        await browser.close()

    logger.info("[CanLII browse] %s total: %d case URLs", db_id.upper(), len(all_urls))
    return sorted(all_urls)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 2: Search CanLII for competition-related cases
# Used for large databases (FC, SCC, appellate courts)
# ─────────────────────────────────────────────────────────────────────────────

SEARCH_TERMS = [
    "Competition Act",
    "Competition Tribunal",
    "abuse of dominance",
    "price fixing conspiracy",
    "merger Competition Bureau",
    "refusal to deal Competition",
    "exclusive dealing Competition Act",
    "misleading advertising Competition",
    "predatory pricing Competition",
    "Commissioner of Competition",
]


async def _search_canlii(
    page: Page,
    db_id: str,
    province: str,
    search_term: str,
    start_year: int,
    end_year: int,
) -> set[str]:
    """Search CanLII for a term within a database, collect matching case URLs."""
    urls: set[str] = set()
    date_filter = f"dateDecided%3A{start_year}-01-01%2C{end_year}-12-31"
    search_url = (
        f"{CANLII}/en/search/#!type=decision"
        f"&text={requests.utils.quote(search_term)}"
        f"&database={db_id}"
        f"&{date_filter}"
    )

    ok = await _safe_goto(page, search_url)
    if not ok:
        return urls

    try:
        await page.wait_for_selector(
            f"a[href*='/{province}/{db_id}/doc/'], .result-title a",
            timeout=20000,
        )
    except Exception:
        return urls

    # Paginate through all search results
    while True:
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        found_this_page = 0
        for a in soup.find_all("a", href=True):
            if f"/{province}/{db_id}/doc/" in a["href"]:
                full = abs_url(CANLII, a["href"])
                if full and full not in urls:
                    urls.add(full)
                    found_this_page += 1

        # Try next page button
        try:
            next_btn = page.locator("a[aria-label='Next page'], a:has-text('Next'), button:has-text('Next')")
            if await next_btn.first.is_visible():
                await next_btn.first.click()
                await asyncio.sleep(2)
            else:
                break
        except Exception:
            break

    return urls


async def _discover_by_search(
    db_id: str,
    province: str,
    start_year: int,
    end_year: int,
) -> list[str]:
    """Find competition-related cases in a large database via search."""
    all_urls: set[str] = set()
    async with async_playwright() as p:
        browser, context = await _new_browser(p)
        page = await context.new_page()

        for term in SEARCH_TERMS:
            logger.info("[CanLII search] %s: %r", db_id.upper(), term)
            found = await _search_canlii(page, db_id, province, term, start_year, end_year)
            all_urls.update(found)
            logger.info("[CanLII search] %s %r → %d new, %d total",
                        db_id.upper(), term, len(found), len(all_urls))
            await asyncio.sleep(1)  # polite delay between searches

        await browser.close()

    logger.info("[CanLII search] %s total: %d competition-related URLs", db_id.upper(), len(all_urls))
    return sorted(all_urls)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy 3: RSS feeds for recent/incremental decisions
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_rss_feeds(session_obj: requests.Session, start_year: int) -> dict[str, list[str]]:
    """
    Pull RSS feeds from Lexum courts. Returns {source_name: [url, ...]}
    Good for incremental weekly runs — captures decisions added since last run.
    """
    results: dict[str, list[str]] = {}
    for rss_url, source_name in RSS_FEEDS:
        try:
            resp = safe_get(session_obj, rss_url, timeout=20)
            root = ET.fromstring(resp.content)
        except Exception as exc:
            logger.warning("[RSS] Failed %s: %s", rss_url, exc)
            continue

        urls = []
        for item in root.findall(".//item"):
            link_el = item.find("link")
            date_el = item.find("pubDate")
            if link_el is None or not link_el.text:
                continue
            link = link_el.text.strip()
            if not link:
                continue
            # Year filter
            if date_el is not None and date_el.text:
                try:
                    year = int(date_el.text.strip()[-4:])
                    if year < start_year:
                        continue
                except ValueError:
                    pass
            urls.append(link)

        if urls:
            results[source_name] = urls
            logger.info("[RSS] %s: %d items from %s", source_name, len(urls), rss_url)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Fetch + parse a single CanLII case page
# ─────────────────────────────────────────────────────────────────────────────

def _parse_canlii_case(
    session_obj: requests.Session,
    url: str,
    source_name: str,
    downloads_dir: str,
    require_competition: bool = False,
) -> tuple | None:
    """Fetch a CanLII case page and return (CaseRecord, [docs], [parties])."""
    try:
        resp = safe_get(session_obj, url, timeout=60)
    except Exception as exc:
        logger.warning("[CanLII] Fetch failed %s: %s", url, exc)
        return None

    full_text = extract_html_text(resp.text)
    soup = BeautifulSoup(resp.text, "lxml")

    # Competition filter for large-DB cases
    if require_competition and not COMPETITION_RE.search(full_text[:5000]):
        return None  # Not competition-related

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""
    if not title:
        title_tag = soup.find("title")
        title = title_tag.text.split(" - CanLII")[0].strip() if title_tag else url

    # Year from URL or text
    year_match = re.search(r"/doc/(\d{4})/", url) or re.search(r"\b(20\d{2}|19\d{2})\b", full_text[:300])
    year = int(year_match.group(1)) if year_match else None

    # Neutral citation e.g. "2024 CACT 5" or "2023 FC 1234"
    cite_match = re.search(
        r"\b(20\d{2}|19\d{2})\s+(CACT|CT|FC[A]?|FCA|SCC|ABCA|ONCA|BCCA|QCCA)\s+\d+\b",
        full_text, re.I,
    )
    citation = cite_match.group(0) if cite_match else None

    date_match = re.search(r"(?:Date[:\s]+|Decided[:\s]+)(\d{4}-\d{2}-\d{2})", full_text)

    # Source case ID from URL slug
    # URL: /en/ca/cact/doc/2024/2024cact5/2024cact5.html
    url_parts = url.rstrip("/").rstrip(".html").split("/")
    source_case_id = url_parts[-1] if url_parts else slugify(url)

    # Court name from DB id in URL
    db_match = re.search(r"/en/\w+/(\w+)/doc/", url)
    db_id = db_match.group(1).upper() if db_match else ""
    COURT_NAMES = {
        "CACT": "Competition Tribunal",
        "FCT": "Federal Court",
        "FCA": "Federal Court of Appeal",
        "SCC": "Supreme Court of Canada",
        "ONCA": "Ontario Court of Appeal",
        "BCCA": "British Columbia Court of Appeal",
        "ABCA": "Alberta Court of Appeal",
        "QCCA": "Quebec Court of Appeal",
    }
    court = COURT_NAMES.get(db_id, db_id)

    rec = CaseRecord(
        source=source_name,
        source_case_id=source_case_id,
        title=title,
        year=year,
        neutral_citation=citation,
        date_decided=date_match.group(1) if date_match else None,
        court_or_tribunal=court,
        case_type="decision",
        case_url=url,
        summary=full_text[:2000],
        full_text=full_text,
        raw={"canlii_url": url, "db_id": db_id},
    )

    # Download PDFs/documents linked from the case page
    docs = []
    db_path_match = re.search(r"/en/(\w+)/(\w+)/doc/", url)
    prov = db_path_match.group(1) if db_path_match else "ca"
    db  = db_path_match.group(2) if db_path_match else "unknown"
    case_folder = Path(downloads_dir) / source_name / str(year or "unknown") / source_case_id
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = abs_url(url, href)
        if not full or full in seen:
            continue
        if not is_document_url(full):
            continue
        # Stay within reasonable domains
        if not any(d in full for d in ["canlii.org", "ct-tc.gc.ca", "fca-caf.gc.ca", "scc-csc.ca"]):
            continue
        seen.add(full)
        label = a.get_text(" ", strip=True) or "Document"
        out_path = case_folder / filename_from_url(full)
        extracted_text = None
        try:
            meta = download_file(session_obj, full, out_path)
            if str(out_path).lower().endswith(".pdf"):
                extracted_text = extract_pdf_text(out_path)
            docs.append(DocumentRecord(
                source=source_name,
                source_case_id=source_case_id,
                document_title=label,
                document_url=full,
                local_path=str(out_path),
                document_type="pdf/decision",
                mime_type=meta.get("mime_type"),
                sha256=meta.get("sha256"),
                file_size=meta.get("file_size"),
                extracted_text=extracted_text,
                raw={},
            ))
        except Exception as exc:
            logger.debug("[CanLII] Doc skip %s: %s", full, exc)

    # Parties from title
    parties: list[PartyRecord] = []
    if " v. " in title or " v " in title:
        sep = " v. " if " v. " in title else " v "
        left, right = title.split(sep, 1)
        parties = [
            PartyRecord(source_case_id, left.strip(), "applicant/appellant"),
            PartyRecord(source_case_id, right.strip(), "respondent"),
        ]

    logger.info("[CanLII] ✓ %s | %d chars | %d docs", title[:70], len(full_text), len(docs))
    return rec, docs, parties


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def scrape_canlii_web(
    start_year: int = 1986,
    end_year: int = 2026,
    downloads_dir: str = "downloads",
) -> list:
    """
    Scrape CanLII public website for all competition-related case law.
    No API key required.

    Approach:
      - Competition Tribunal: browse ALL cases year by year (~600 total)
      - FC, SCC, appellate: search by competition keywords (100k+ decisions total,
        only ~500-1000 are competition-related)
      - RSS feeds: catch very recent decisions missed by above
    """
    s = session()
    out: list = []
    all_urls: dict[str, list[str]] = {}  # source_name → [url]

    # ── Phase 1: Discovery ─────────────────────────────────────────────────
    for db_id, province, source_name, description, scrape_all in DATABASES:
        logger.info("[CanLII] Discovering %s (%s)…", description, db_id)
        if scrape_all:
            # Small DB (CT): browse every year
            urls = asyncio.run(_discover_all_cases(db_id, province, start_year, end_year))
        else:
            # Large DB: search for competition terms
            urls = asyncio.run(_discover_by_search(db_id, province, start_year, end_year))

        all_urls[source_name] = urls
        logger.info("[CanLII] %s: %d URLs to scrape", description, len(urls))

    # ── Phase 2: RSS supplement ────────────────────────────────────────────
    rss_results = _scrape_rss_feeds(s, start_year)
    for source_name, rss_urls in rss_results.items():
        existing = set(all_urls.get(source_name, []))
        new_urls = [u for u in rss_urls if u not in existing]
        if new_urls:
            logger.info("[RSS] Adding %d new URLs for %s", len(new_urls), source_name)
            all_urls.setdefault(source_name, []).extend(new_urls)

    # ── Phase 3: Fetch + parse each case ──────────────────────────────────
    total = sum(len(v) for v in all_urls.values())
    logger.info("[CanLII] Fetching %d total case pages…", total)

    # Find which source_names correspond to large DBs (need competition filter)
    large_db_sources = {
        sn for db_id, province, sn, desc, scrape_all in DATABASES
        if not scrape_all
    }

    done = 0
    for source_name, urls in all_urls.items():
        require_filter = source_name in large_db_sources
        kept = skipped = errors = 0
        for url in urls:
            done += 1
            result = _parse_canlii_case(
                s, url, source_name, downloads_dir,
                require_competition=require_filter,
            )
            if result is None:
                skipped += 1
            else:
                out.append(result)
                kept += 1
            if done % 100 == 0:
                logger.info("[CanLII] Progress: %d/%d total", done, total)
        logger.info("[CanLII] %s complete: kept=%d skipped=%d errors=%d",
                    source_name, kept, skipped, errors)

    logger.info("[CanLII] All done — %d competition-law decisions scraped", len(out))
    return out


# Backwards-compat alias used by main.py / canlii.py
def scrape_canlii(start_year, end_year, downloads_dir, api_key=""):
    return scrape_canlii_web(start_year, end_year, downloads_dir)


def scrape_canlii_optional(downloads_dir):
    return scrape_canlii_web(downloads_dir=downloads_dir)
