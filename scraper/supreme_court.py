"""
Supreme Court of Canada — competition law decisions scraper

Uses the SCC's Lexum decision database at decisions.scc-csc.ca.
Same Lexum platform as the Competition Tribunal and Federal Court.

Filters to competition-related decisions only (SCC has 100+ years of decisions;
we only want those touching the Competition Act, Combines Investigation Act, etc.)
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

SCC_BASE = "https://decisions.scc-csc.ca"
SCC_NAV = SCC_BASE + "/scc-csc/scc-csc/en/nav_date.do?year={year}"

# Scrape back to 1975 — Combines Investigation Act cases pre-date Competition Act (1986)
SCC_DEFAULT_START = 1975

COMPETITION_KEYWORDS = re.compile(
    r"competition act|combines investigation act|restrictive trade practices|"
    r"competition bureau|competition tribunal|market power|abuse of dominance|"
    r"predatory pric|price.?fixing|bid.?rigg|conspiracy.*market|"
    r"merger.*competition|monopoli|cartel|refusal to (deal|supply)|"
    r"exclusive dealing|tied selling|misleading advertising|deceptive marketing|"
    r"director of investigation|commissioner of competition",
    re.I,
)


async def _discover_scc_urls(start_year: int, end_year: int) -> list[str]:
    urls: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="CompetitionCaseCa-Scraper/2.0 (legal research; admin@competitioncase.ca)"
        )
        page = await context.new_page()

        for year in range(end_year, start_year - 1, -1):
            url = SCC_NAV.format(year=year)
            logger.info("[SCC] Discovering year %d", year)

            for attempt in range(3):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    try:
                        await page.wait_for_selector('a[href*="/item/"]', timeout=15000)
                    except Exception:
                        pass
                    break
                except Exception as exc:
                    logger.warning("[SCC] Year %d attempt %d: %s", year, attempt + 1, exc)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            found = 0
            for a in soup.select('a[href*="/item/"]'):
                href = a.get("href")
                full = abs_url(url, href)
                if full:
                    urls.add(full)
                    found += 1
            logger.info("[SCC] Year %d: %d case links", year, found)

        await browser.close()
    logger.info("[SCC] Discovery complete — %d total SCC case URLs", len(urls))
    return sorted(urls)


def _parse_scc_case(url: str, downloads_dir: str):
    s = session()

    try:
        resp = safe_get(s, url, timeout=120)
    except Exception as exc:
        logger.error("[SCC] Failed to fetch %s: %s", url, exc)
        return None

    full_text = extract_html_text(resp.text)

    # Pre-filter: check title row on list page first (faster than full parse)
    if not COMPETITION_KEYWORDS.search(full_text):
        return None  # Not competition-related

    soup = BeautifulSoup(resp.text, "lxml")
    title = (
        soup.title.get_text(" ", strip=True)
        .replace(" - Supreme Court of Canada", "")
        .replace(" — Supreme Court of Canada", "")
        .replace(" - SCC", "").replace(" — SCC", "")
        .strip()
        if soup.title
        else url
    )

    year_match = re.search(r"\b(19|20)\d{2}\b", full_text)
    year = int(year_match.group(0)) if year_match else None

    item_id_match = re.search(r"/item/(\d+)/", url)
    source_case_id = item_id_match.group(1) if item_id_match else slugify(url)

    # SCC neutral citation format: 2023 SCC 12
    citation_match = re.search(r"\b(20\d{2}|19\d{2})\s+SCC\s+\d+\b", full_text)
    neutral_citation = citation_match.group(0) if citation_match else None

    date_match = re.search(r"Date[:\s]+(\d{4}-\d{2}-\d{2})", full_text)

    rec = CaseRecord(
        source="supreme_court",
        source_case_id=source_case_id,
        title=title,
        year=year,
        neutral_citation=neutral_citation,
        date_decided=date_match.group(1) if date_match else None,
        court_or_tribunal="Supreme Court of Canada",
        case_type="court decision",
        case_url=url,
        summary=full_text[:2000],
        full_text=full_text,
        raw={"source_url": url},
    )

    # Download all documents
    docs = []
    case_folder = (
        Path(downloads_dir) / "supreme_court" / str(year or "unknown") / source_case_id
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
                    source="supreme_court",
                    source_case_id=source_case_id,
                    document_title=label,
                    document_url=full,
                    local_path=str(out_path),
                    document_type="pdf/scc-document",
                    mime_type=meta.get("mime_type"),
                    sha256=meta.get("sha256"),
                    file_size=meta.get("file_size"),
                    extracted_text=extracted_text,
                    raw={},
                )
            )
        except Exception as exc:
            logger.warning("[SCC] Doc download failed %s: %s", full, exc)
            docs.append(
                DocumentRecord(
                    source="supreme_court",
                    source_case_id=source_case_id,
                    document_title=label,
                    document_url=full,
                    raw={"download_error": str(exc)},
                )
            )

    # Parse parties from title
    parties: list[PartyRecord] = []
    if " v. " in title or " v " in title:
        sep = " v. " if " v. " in title else " v "
        left, right = title.split(sep, 1)
        parties = [
            PartyRecord(source_case_id=source_case_id, party_name=left.strip(), party_role="appellant"),
            PartyRecord(source_case_id=source_case_id, party_name=right.strip(), party_role="respondent"),
        ]

    logger.info("[SCC] ✓ %s | %d chars | %d docs", title[:80], len(full_text), len(docs))
    return rec, docs, parties


def scrape_supreme_court(start_year: int, end_year: int, downloads_dir: str):
    # Use SCC_DEFAULT_START (1975) to catch Combines Investigation Act cases
    effective_start = min(start_year, SCC_DEFAULT_START)
    urls = asyncio.run(_discover_scc_urls(effective_start, end_year))
    logger.info("[SCC] Scraping %d candidate case pages…", len(urls))
    out = []
    kept = skipped = errors = 0
    for url in urls:
        try:
            result = _parse_scc_case(url, downloads_dir)
            if result is None:
                skipped += 1
            else:
                out.append(result)
                kept += 1
        except Exception as exc:
            errors += 1
            logger.error("[SCC] Error on %s: %s", url, exc)
    logger.info(
        "[SCC] Done. kept=%d skipped_non_competition=%d errors=%d",
        kept, skipped, errors,
    )
    return out
