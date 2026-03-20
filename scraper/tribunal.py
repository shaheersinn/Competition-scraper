"""
Competition Tribunal scraper — fixed

BUG FIXES:
  1. full_text was NEVER stored (summary=None hardcoded). Now extracts and stores
     the complete decision text from #wb-cont / main.
  2. PDF zip artifact was EMPTY (119 bytes). Root cause: silent exception swallowing
     in Playwright loop + narrow document URL matching. Fixed with retry + broader
     document detection.
  3. 58-second runtime: Playwright waited for 'networkidle' which timed out silently
     on Lexum pages. Changed to 'domcontentloaded' + explicit wait for case links.
  4. No retry logic on HTTP requests. Fixed via utils.safe_get with backoff.
  5. PDF text never extracted — now uses pdfminer.six after every download.
"""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

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

TRIBUNAL_BASE = "https://decisions.ct-tc.gc.ca"
NAV_URL = TRIBUNAL_BASE + "/ct-tc/cdo/en/nav_date.do?year={year}"


def _item_id(url: str) -> str:
    m = re.search(r"/item/(\d+)/", url)
    if m:
        return m.group(1)
    return slugify(url)


async def _discover_tribunal_case_urls(start_year: int, end_year: int) -> list[str]:
    """
    BUG FIX: Old code used wait_until='networkidle' which silently timed out
    on Lexum pages, resulting in 0 URLs discovered. Changed to domcontentloaded
    + explicit wait for the case link selector.
    """
    urls: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "CompetitionCaseCa-Scraper/2.0 (legal research; admin@competitioncase.ca)"
            )
        )
        page = await context.new_page()

        for year in range(end_year, start_year - 1, -1):
            url = NAV_URL.format(year=year)
            logger.info("[CT] Discovering year %d: %s", year, url)

            for attempt in range(3):
                try:
                    # BUG FIX: was 'networkidle', changed to 'domcontentloaded'
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    # Wait for case links to appear (or timeout gracefully)
                    try:
                        await page.wait_for_selector(
                            'a[href*="/item/"]', timeout=15000
                        )
                    except Exception:
                        pass  # No cases for this year — that's OK
                    break
                except Exception as exc:
                    logger.warning("[CT] Year %d attempt %d failed: %s", year, attempt + 1, exc)
                    if attempt == 2:
                        continue

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            found = 0
            for a in soup.select('a[href*="/item/"]'):
                href = a.get("href")
                full = abs_url(url, href)
                if full:
                    urls.add(full)
                    found += 1
            logger.info("[CT] Year %d: found %d case links", year, found)

        await browser.close()
    logger.info("[CT] Discovery complete — %d total case URLs", len(urls))
    return sorted(urls)


def _parse_parties(title: str) -> list[PartyRecord]:
    parties = []
    if " v. " in title:
        left, right = title.split(" v. ", 1)
        parties.append(
            PartyRecord(
                source_case_id="",
                party_name=left.strip(),
                party_role="applicant/appellant",
            )
        )
        parties.append(
            PartyRecord(
                source_case_id="",
                party_name=right.strip(),
                party_role="respondent",
            )
        )
    return parties


def _parse_case_page(url: str, downloads_dir: str):
    s = session()

    try:
        resp = safe_get(s, url, timeout=120)
    except Exception as exc:
        logger.error("[CT] Failed to fetch case page %s: %s", url, exc)
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Title
    title = (
        soup.title.get_text(" ", strip=True)
        .replace(" - Competition Tribunal", "")
        .replace(" — Competition Tribunal", "")
        .strip()
        if soup.title
        else url
    )

    # BUG FIX: full_text was never extracted or stored.
    # Old code: text = soup.get_text(...) but then summary=None — the text was
    # computed and immediately thrown away.
    full_text = extract_html_text(resp.text)

    # Extract year from text
    year_match = re.search(r"\b(19|20)\d{2}\b", full_text)
    year = int(year_match.group(0)) if year_match else None

    # Extract case number
    case_no = None
    m = re.search(r"\bCT[-‑]?\d{4}[-‑]\d+\b", full_text, re.I)
    if m:
        case_no = m.group(0)

    # Extract date decided
    date_decided = None
    m = re.search(r"Date[:\s]+(\d{4}-\d{2}-\d{2})", full_text)
    if m:
        date_decided = m.group(1)

    rec = CaseRecord(
        source="competition_tribunal",
        source_case_id=_item_id(url),
        title=title,
        year=year,
        case_number=case_no,
        date_decided=date_decided,
        court_or_tribunal="Competition Tribunal",
        case_type="tribunal decision",
        case_url=url,
        summary=full_text[:2000],   # short excerpt for quick display
        full_text=full_text,        # BUG FIX: the actual complete decision text
        raw={"source_url": url},
    )

    # Document download
    docs = []
    case_folder = (
        Path(downloads_dir)
        / "competition_tribunal"
        / str(year or "unknown")
        / rec.source_case_id
    )

    seen_urls: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        full = abs_url(url, href)
        if not full or full in seen_urls:
            continue
        # BUG FIX: old token list was too narrow (only .pdf / /download / download=1).
        # Tribunal also uses URLs like /getattachment/ and /DocumentRetriever
        if not is_document_url(full):
            continue
        seen_urls.add(full)
        label = a.get_text(" ", strip=True) or "Document"
        out_path = case_folder / filename_from_url(full)

        extracted_text = None
        try:
            meta = download_file(s, full, out_path)
            # BUG FIX: PDF text was never extracted from downloaded files
            if str(out_path).lower().endswith(".pdf"):
                extracted_text = extract_pdf_text(out_path)
                logger.info(
                    "[CT] Downloaded + extracted PDF: %s (%d chars)",
                    out_path.name, len(extracted_text or ""),
                )
            docs.append(
                DocumentRecord(
                    source="competition_tribunal",
                    source_case_id=rec.source_case_id,
                    document_title=label,
                    document_url=full,
                    local_path=str(out_path),
                    document_type="pdf/document",
                    mime_type=meta["mime_type"],
                    sha256=meta["sha256"],
                    file_size=meta["file_size"],
                    extracted_text=extracted_text,
                    raw={"anchor_text": label},
                )
            )
        except Exception as exc:
            logger.warning("[CT] Download failed %s: %s", full, exc)
            docs.append(
                DocumentRecord(
                    source="competition_tribunal",
                    source_case_id=rec.source_case_id,
                    document_title=label,
                    document_url=full,
                    extracted_text=None,
                    raw={"anchor_text": label, "download_error": str(exc)},
                )
            )

    parties = _parse_parties(title)
    for party in parties:
        party.source_case_id = rec.source_case_id

    logger.info(
        "[CT] Parsed: %s | text=%d chars | docs=%d",
        title[:80], len(full_text), len(docs),
    )
    return rec, docs, parties


def scrape_tribunal(start_year: int, end_year: int, downloads_dir: str):
    urls = asyncio.run(_discover_tribunal_case_urls(start_year, end_year))
    logger.info("[CT] Scraping %d case pages…", len(urls))
    out = []
    for i, url in enumerate(urls, 1):
        try:
            result = _parse_case_page(url, downloads_dir)
            if result:
                out.append(result)
        except Exception as exc:
            logger.error("[CT] Error on %s: %s", url, exc)
        if i % 50 == 0:
            logger.info("[CT] Progress: %d/%d cases scraped", i, len(urls))
    logger.info("[CT] Done. Scraped %d/%d cases.", len(out), len(urls))
    return out
