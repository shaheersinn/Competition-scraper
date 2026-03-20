"""
Competition Bureau scraper — fixed

BUG FIXES:
  1. Old code only parsed a single text line per case — got a 180-char title
     with no decision content whatsoever.
  2. Bureau case pages link to actual decision documents (PDFs, consent agreements,
     press releases). These were never fetched or downloaded.
  3. No retry logic on HTTP requests.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

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

BUREAU_BASE = "https://competition-bureau.canada.ca"

CASE_LIST_URLS = [
    (
        "competition_bureau_rtp",
        BUREAU_BASE + "/restrictive-trade-practices/cases-and-outcomes/"
        "restrictive-trade-practices-cases-and-outcomes?wbdisable=true",
        "Restrictive Trade Practices",
    ),
    (
        "competition_bureau_dmp",
        BUREAU_BASE + "/en/deceptive-marketing-practices/cases-and-outcomes?wbdisable=true",
        "Deceptive Marketing Practices",
    ),
]


def _parse_case_list(source_name: str, list_url: str, downloads_dir: str):
    s = session()

    try:
        html = safe_get(s, list_url).text
    except Exception as exc:
        logger.error("[Bureau] Failed to fetch list %s: %s", list_url, exc)
        return []

    soup = BeautifulSoup(html, "lxml")
    records = []

    # Bureau pages have a table with case rows — each row links to a detail page
    # BUG FIX: old code only extracted text lines, never followed links to decisions
    rows = soup.select("table tbody tr, .views-row, article.node--type-case")

    if not rows:
        # Fallback: scrape any visible links that look like case links
        rows = soup.select("a[href]")

    current_year = None
    for row in rows:
        if hasattr(row, "get_text"):
            row_text = re.sub(r"\s+", " ", row.get_text(" ", strip=True))
        else:
            continue

        # Detect year headings
        y_match = re.fullmatch(r"20\d{2}", row_text.strip())
        if y_match:
            current_year = int(row_text.strip())
            continue

        # Find a detail-page link in this row
        link = row.find("a", href=True) if hasattr(row, "find") else row
        if not link:
            continue
        href = link.get("href", "")
        detail_url = abs_url(list_url, href)
        if not detail_url or detail_url == list_url:
            continue

        # Skip non-case links (nav, footer)
        if not any(
            token in detail_url
            for token in ["/case", "/decision", "/agreement", "/consent", "/matter"]
        ):
            # Still include if it's clearly a case from the row text pattern
            if not re.search(r"20\d{2}-\d{2}-\d{2}", row_text):
                continue

        title = link.get_text(" ", strip=True) or row_text[:180]
        source_case_id = slugify(href.rstrip("/").split("/")[-1] or title, max_len=120)

        # Fetch the detail page for full content
        full_text = ""
        detail_docs = []
        try:
            detail_resp = safe_get(s, detail_url)
            full_text = extract_html_text(detail_resp.text)
            detail_soup = BeautifulSoup(detail_resp.text, "lxml")

            # Download documents linked from the detail page
            case_folder = (
                Path(downloads_dir)
                / source_name
                / str(current_year or "unknown")
                / source_case_id
            )
            seen: set[str] = set()
            for doc_link in detail_soup.select("a[href]"):
                doc_href = doc_link.get("href")
                doc_full = abs_url(detail_url, doc_href)
                if not doc_full or doc_full in seen:
                    continue
                if not is_document_url(doc_full):
                    continue
                seen.add(doc_full)
                doc_label = doc_link.get_text(" ", strip=True) or "Document"
                out_path = case_folder / filename_from_url(doc_full)
                extracted_text = None
                try:
                    meta = download_file(s, doc_full, out_path)
                    if str(out_path).lower().endswith(".pdf"):
                        extracted_text = extract_pdf_text(out_path)
                    detail_docs.append(
                        DocumentRecord(
                            source=source_name,
                            source_case_id=source_case_id,
                            document_title=doc_label,
                            document_url=doc_full,
                            local_path=str(out_path),
                            document_type="pdf/bureau-document",
                            mime_type=meta.get("mime_type"),
                            sha256=meta.get("sha256"),
                            file_size=meta.get("file_size"),
                            extracted_text=extracted_text,
                            raw={},
                        )
                    )
                except Exception as exc:
                    logger.warning("[Bureau] Doc download failed %s: %s", doc_full, exc)
                    detail_docs.append(
                        DocumentRecord(
                            source=source_name,
                            source_case_id=source_case_id,
                            document_title=doc_label,
                            document_url=doc_full,
                            raw={"download_error": str(exc)},
                        )
                    )
        except Exception as exc:
            logger.warning("[Bureau] Detail page failed %s: %s", detail_url, exc)
            full_text = row_text  # fall back to the list-page text

        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", row_text + full_text[:500])
        date_str = date_match.group(1) if date_match else None
        if not current_year and date_str:
            current_year = int(date_str[:4])

        rec = CaseRecord(
            source=source_name,
            source_case_id=source_case_id,
            title=title,
            year=current_year,
            date_decided=date_str,
            court_or_tribunal="Competition Bureau Canada",
            case_type="public enforcement outcome",
            case_url=detail_url,
            summary=full_text[:2000],
            full_text=full_text,
            raw={"list_url": list_url, "detail_url": detail_url},
        )
        records.append((rec, detail_docs, []))
        logger.info("[Bureau] ✓ %s | %d chars | %d docs", title[:80], len(full_text), len(detail_docs))

    logger.info("[Bureau] %s: %d records", source_name, len(records))
    return records


def scrape_bureau_sources(downloads_dir: str = "downloads"):
    out = []
    for source_name, list_url, _ in CASE_LIST_URLS:
        out.extend(_parse_case_list(source_name, list_url, downloads_dir))
    return out
