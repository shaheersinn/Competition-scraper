"""
CanLII optional scraper — fixed

BUG FIXES:
  1. Old code used hash (#) URLs like canlii.org/en/#search/... — these are
     JavaScript fragment routes. requests() only gets the base shell page,
     NOT the search results. Zero cases were ever found this way.
  2. No full text was fetched — only the link text was stored.
  3. Now uses Playwright to render the JS-based CanLII search, then fetches
     each case's full HTML decision page.

Optional: Set CANLII_API_KEY env var to use the faster JSON API instead of
browser scraping. Free API keys available at https://api.canlii.org/
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .models import CaseRecord, DocumentRecord
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

CANLII_BASE = "https://www.canlii.org"

# Databases to search (Federal Court, FC Appeal, SCC)
# These complement the direct Lexum scrapers with CanLII-indexed versions
CANLII_DATABASES = [
    ("en/ca/fct", "federal_court"),      # Federal Court
    ("en/ca/fca", "federal_court_appeal"), # Federal Court of Appeal
    ("en/ca/scc", "supreme_court"),       # Supreme Court
]

COMPETITION_SEARCH_TERMS = [
    "Competition Act",
    "Competition Tribunal",
    "abuse of dominance",
    "price fixing competition",
    "merger Competition Bureau",
]

COMPETITION_KEYWORDS = re.compile(
    r"competition act|combines investigation|competition bureau|competition tribunal|"
    r"predatory pric|price.?fixing|market power|abuse of dominance|"
    r"merger.*competition|monopoli|cartel|refusal to (deal|supply)|"
    r"exclusive dealing|commissioner of competition",
    re.I,
)


def _scrape_via_api(api_key: str, downloads_dir: str):
    """
    Use CanLII JSON API to find competition-related cases.
    Faster and more reliable than browser scraping.
    """
    s = session()
    results = []

    for db_path, source_name in CANLII_DATABASES:
        for term in COMPETITION_SEARCH_TERMS:
            offset = 0
            while True:
                url = (
                    f"https://api.canlii.org/v1/search/en/"
                    f"?maximumDocumentCount=100&offset={offset}"
                    f"&fullText={requests.utils.quote(term)}"
                    f"&database={db_path.split('/')[-1]}&key={api_key}"
                )
                try:
                    resp = safe_get(s, url)
                    data = resp.json()
                except Exception as exc:
                    logger.warning("[CanLII API] Search failed for '%s': %s", term, exc)
                    break

                cases = data.get("results", [])
                if not cases:
                    break

                for case_meta in cases:
                    case_id = case_meta.get("caseId", {})
                    case_url = f"{CANLII_BASE}/en/{db_path}/{case_id.get('en', '')}"
                    result = _fetch_canlii_case(s, case_url, source_name, downloads_dir)
                    if result:
                        results.append(result)

                total = data.get("totalResults", 0)
                offset += 100
                if offset >= total:
                    break
                time.sleep(0.5)

    # Deduplicate
    seen = set()
    unique = []
    for rec, docs, parties in results:
        key = (rec.source, rec.source_case_id)
        if key not in seen:
            seen.add(key)
            unique.append((rec, docs, parties))
    return unique


async def _scrape_via_playwright(downloads_dir: str):
    """
    Browser-based fallback when no API key is available.
    Renders the JS CanLII search and harvests case links.
    """
    all_links: dict[str, str] = {}  # url -> source_name

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="CompetitionCaseCa-Scraper/2.0 (admin@competitioncase.ca)"
        )
        page = await context.new_page()

        for db_path, source_name in CANLII_DATABASES:
            for term in COMPETITION_SEARCH_TERMS[:2]:  # limit for politeness
                search_url = (
                    f"{CANLII_BASE}/en/"
                    f"?searchUrlType=decisions&partialMetaData=true"
                    f"&offset=0&resultCount=100"
                    f"&text={requests.utils.quote(term)}"
                    f"&database={db_path.split('/')[-1]}"
                )
                logger.info("[CanLII] Searching: %s", search_url)
                try:
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_selector("a[href*='/en/ca/']", timeout=20000)
                except Exception as exc:
                    logger.warning("[CanLII] Search page failed: %s", exc)
                    continue

                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                for a in soup.select("a[href]"):
                    href = a.get("href", "")
                    if f"/{db_path}/" in href and "/item/" not in href.lower():
                        if href.endswith(".html") or re.search(r"/doc/\d", href):
                            full = abs_url(CANLII_BASE, href)
                            if full:
                                all_links[full] = source_name

                await asyncio.sleep(1)

        await browser.close()

    logger.info("[CanLII] Playwright found %d candidate links", len(all_links))

    s = session()
    results = []
    for url, source_name in all_links.items():
        result = _fetch_canlii_case(s, url, source_name, downloads_dir)
        if result:
            results.append(result)

    return results


def _fetch_canlii_case(
    s: requests.Session, url: str, source_name: str, downloads_dir: str
):
    """Fetch and parse a single CanLII case page."""
    try:
        resp = safe_get(s, url, timeout=60)
    except Exception as exc:
        logger.warning("[CanLII] Fetch failed %s: %s", url, exc)
        return None

    full_text = extract_html_text(resp.text)
    if not COMPETITION_KEYWORDS.search(full_text):
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else url

    # CanLII case ID from URL: /en/ca/fct/doc/2023/2023fc123/2023fc123.html
    path_parts = url.rstrip("/").split("/")
    source_case_id = path_parts[-2] if len(path_parts) >= 2 else slugify(url)

    year_match = re.search(r"\b(19|20)\d{2}\b", title + full_text[:200])
    year = int(year_match.group(0)) if year_match else None

    citation_match = re.search(
        r"\b(20\d{2}|19\d{2})\s+(SCC|FC[A]?|FCA)\s+\d+\b", full_text
    )
    neutral_citation = citation_match.group(0) if citation_match else None

    rec = CaseRecord(
        source=f"canlii_{source_name}",
        source_case_id=source_case_id,
        title=title,
        year=year,
        neutral_citation=neutral_citation,
        court_or_tribunal="CanLII indexed decision",
        case_type="competition-law decision",
        case_url=url,
        summary=full_text[:2000],
        full_text=full_text,
        raw={"canlii_url": url},
    )

    # Download documents from the case page
    docs = []
    case_folder = (
        Path(downloads_dir) / f"canlii_{source_name}" / str(year or "unknown") / source_case_id
    )
    seen: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        full = abs_url(url, href)
        if not full or full in seen:
            continue
        if not is_document_url(full):
            continue
        seen.add(full)
        label = a.get_text(" ", strip=True) or "Document"
        out_path = case_folder / filename_from_url(full)
        extracted_text = None
        try:
            meta = download_file(s, full, out_path)
            if str(out_path).lower().endswith(".pdf"):
                extracted_text = extract_pdf_text(out_path)
            docs.append(
                DocumentRecord(
                    source=f"canlii_{source_name}",
                    source_case_id=source_case_id,
                    document_title=label,
                    document_url=full,
                    local_path=str(out_path),
                    document_type="pdf/canlii-document",
                    mime_type=meta.get("mime_type"),
                    sha256=meta.get("sha256"),
                    file_size=meta.get("file_size"),
                    extracted_text=extracted_text,
                    raw={},
                )
            )
        except Exception as exc:
            logger.warning("[CanLII] Doc download failed %s: %s", full, exc)

    logger.info("[CanLII] ✓ %s | %d chars", title[:80], len(full_text))
    return rec, docs, []


def scrape_canlii_optional(downloads_dir: str):
    api_key = os.environ.get("CANLII_API_KEY", "").strip()
    if api_key:
        logger.info("[CanLII] Using API key — JSON API mode")
        return _scrape_via_api(api_key, downloads_dir)
    else:
        logger.info("[CanLII] No CANLII_API_KEY — falling back to Playwright browser mode")
        return asyncio.run(_scrape_via_playwright(downloads_dir))
