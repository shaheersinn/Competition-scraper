"""
Federal Court of Canada — competition law decisions scraper

Uses the Federal Court's Lexum decision database at decisions.fca-caf.gc.ca,
which is the same platform as the Competition Tribunal so the same discovery
logic applies.

Filters to Competition Act / competition-related decisions only to avoid
downloading the entire Federal Court corpus (~200k+ decisions since 1971).
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

# Federal Court Lexum instance
FC_BASE = "https://decisions.fca-caf.gc.ca"
FC_NAV = FC_BASE + "/fct-cf/en/nav_date.do?year={year}"

# Federal Court of Appeal
FCA_NAV = FC_BASE + "/fca-caf/en/nav_date.do?year={year}"

COMPETITION_KEYWORDS = re.compile(
    r"competition act|combines investigation|competition bureau|competition tribunal|"
    r"predatory pric|price.?fixing|market power|abuse of dominance|"
    r"merger|monopol|cartel|refusal to (deal|supply)|exclusive dealing|"
    r"misleading advertising|deceptive marketing|s\.\s*7[45678]|s\.\s*9[01]",
    re.I,
)


async def _discover_urls_for_court(
    nav_template: str, start_year: int, end_year: int, court_label: str
) -> list[str]:
    urls: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="CompetitionCaseCa-Scraper/2.0 (legal research; admin@competitioncase.ca)"
        )
        page = await context.new_page()

        for year in range(end_year, start_year - 1, -1):
            url = nav_template.format(year=year)
            logger.info("[%s] Discovering year %d", court_label, year)

            for attempt in range(3):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    try:
                        await page.wait_for_selector('a[href*="/item/"]', timeout=15000)
                    except Exception:
                        pass
                    break
                except Exception as exc:
                    logger.warning(
                        "[%s] Year %d attempt %d: %s", court_label, year, attempt + 1, exc
                    )

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            found = 0
            for a in soup.select('a[href*="/item/"]'):
                href = a.get("href")
                full = abs_url(url, href)
                if full:
                    urls.add(full)
                    found += 1
            logger.info("[%s] Year %d: %d case links", court_label, year, found)

        await browser.close()
    return sorted(urls)


def _is_competition_related(text: str) -> bool:
    return bool(COMPETITION_KEYWORDS.search(text))


def _parse_fc_case(url: str, source_name: str, downloads_dir: str):
    s = session()

    try:
        resp = safe_get(s, url, timeout=120)
    except Exception as exc:
        logger.error("[FC] Failed to fetch %s: %s", url, exc)
        return None

    full_text = extract_html_text(resp.text)

    # Filter: only keep competition-related decisions
    if not _is_competition_related(full_text):
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    title = (
        soup.title.get_text(" ", strip=True)
        .replace(" - Federal Court", "")
        .replace(" — Federal Court", "")
        .replace(" - Federal Court of Appeal", "")
        .replace(" — Federal Court of Appeal", "")
        .strip()
        if soup.title
        else url
    )

    year_match = re.search(r"\b(19|20)\d{2}\b", full_text)
    year = int(year_match.group(0)) if year_match else None

    item_id_match = re.search(r"/item/(\d+)/", url)
    source_case_id = item_id_match.group(1) if item_id_match else slugify(url)

    # Extract citation (e.g. 2024 FC 123)
    citation_match = re.search(
        r"\b(20\d{2}|19\d{2})\s+FC[A]?\s+\d+\b", full_text
    )
    neutral_citation = citation_match.group(0) if citation_match else None

    date_match = re.search(r"Date[:\s]+(\d{4}-\d{2}-\d{2})", full_text)

    rec = CaseRecord(
        source=source_name,
        source_case_id=source_case_id,
        title=title,
        year=year,
        neutral_citation=neutral_citation,
        date_decided=date_match.group(1) if date_match else None,
        court_or_tribunal=(
            "Federal Court of Appeal" if "fca-caf" in url else "Federal Court"
        ),
        case_type="court decision",
        case_url=url,
        summary=full_text[:2000],
        full_text=full_text,
        raw={"source_url": url},
    )

    # Download all documents
    docs = []
    case_folder = (
        Path(downloads_dir) / source_name / str(year or "unknown") / source_case_id
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
                    source=source_name,
                    source_case_id=source_case_id,
                    document_title=label,
                    document_url=full,
                    local_path=str(out_path),
                    document_type="pdf/court-document",
                    mime_type=meta.get("mime_type"),
                    sha256=meta.get("sha256"),
                    file_size=meta.get("file_size"),
                    extracted_text=extracted_text,
                    raw={},
                )
            )
        except Exception as exc:
            logger.warning("[FC] Doc download failed %s: %s", full, exc)
            docs.append(
                DocumentRecord(
                    source=source_name,
                    source_case_id=source_case_id,
                    document_title=label,
                    document_url=full,
                    raw={"download_error": str(exc)},
                )
            )

    # Parse parties
    parties: list[PartyRecord] = []
    if " v. " in title or " v " in title:
        sep = " v. " if " v. " in title else " v "
        left, right = title.split(sep, 1)
        parties = [
            PartyRecord(source_case_id=source_case_id, party_name=left.strip(), party_role="appellant/applicant"),
            PartyRecord(source_case_id=source_case_id, party_name=right.strip(), party_role="respondent"),
        ]

    logger.info("[FC] ✓ %s | %d chars | %d docs", title[:80], len(full_text), len(docs))
    return rec, docs, parties


def scrape_federal_court(start_year: int, end_year: int, downloads_dir: str):
    courts = [
        ("federal_court", FC_NAV, "Federal Court"),
        ("federal_court_appeal", FCA_NAV, "Federal Court of Appeal"),
    ]
    out = []
    for source_name, nav_template, label in courts:
        urls = asyncio.run(
            _discover_urls_for_court(nav_template, start_year, end_year, label)
        )
        logger.info("[%s] Scraping %d candidate case pages…", label, len(urls))
        kept = skipped = errors = 0
        for url in urls:
            try:
                result = _parse_fc_case(url, source_name, downloads_dir)
                if result is None:
                    skipped += 1  # not competition-related
                else:
                    out.append(result)
                    kept += 1
            except Exception as exc:
                errors += 1
                logger.error("[%s] Error on %s: %s", label, url, exc)
        logger.info(
            "[%s] Done. kept=%d skipped_non_competition=%d errors=%d",
            label, kept, skipped, errors,
        )
    return out
