"""
Competition Tribunal decision summaries scraper — fixed

BUG FIXES:
  1. summary=text[:4000] was truncating to 4000 chars. Full text now stored in
     full_text field; summary holds a shorter excerpt for display.
  2. Only found the first PDF link per summary page. Now finds all document links.
  3. PDF text never extracted. Now uses extract_pdf_text after download.
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
)

logger = logging.getLogger(__name__)

URL = "https://www.ct-tc.gc.ca/en/cases/decision-summaries.html"
BASE = "https://www.ct-tc.gc.ca"


def scrape_decision_summaries(start_year: int, end_year: int, downloads_dir: str = "downloads"):
    s = session()

    try:
        resp = safe_get(s, URL)
    except Exception as exc:
        logger.error("[Summaries] Failed to fetch index: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    summary_links = soup.select('a[href*="decision-summaries/"]')
    logger.info("[Summaries] Found %d summary links", len(summary_links))

    for a in summary_links:
        href = a.get("href")
        full = abs_url(URL, href)
        title = a.get_text(" ", strip=True)
        if not title or not full:
            continue

        try:
            page_resp = safe_get(s, full)
        except Exception as exc:
            logger.warning("[Summaries] Failed to fetch %s: %s", full, exc)
            continue

        # BUG FIX: extract_html_text strips nav/chrome, was previously getting
        # full page noise. Also was truncated to 4000 chars.
        full_text = extract_html_text(page_resp.text)

        page_soup = BeautifulSoup(page_resp.text, "lxml")

        case_no = None
        m = re.search(r"Case #:?\s*(CT[-‑]?\d{4}[-‑]?\d+)", full_text, re.I)
        if m:
            case_no = m.group(1)

        date_match = re.search(r"Date rendered:?\s*(\d{4}-\d{2}-\d{2})", full_text)
        year = int(date_match.group(1)[:4]) if date_match else None

        if year is not None and not (start_year <= year <= end_year):
            continue

        rec = CaseRecord(
            source="competition_tribunal_summaries",
            source_case_id=case_no or full.rstrip("/").split("/")[-1],
            title=title,
            case_number=case_no,
            year=year,
            date_decided=date_match.group(1) if date_match else None,
            court_or_tribunal="Competition Tribunal",
            case_type="decision summary",
            case_url=full,
            summary=full_text[:2000],  # short excerpt
            full_text=full_text,       # BUG FIX: full text, not truncated
            raw={"summary_url": full},
        )

        # BUG FIX: old code only found the first PDF link. Now finds all documents.
        docs = []
        case_folder = (
            Path(downloads_dir)
            / "competition_tribunal_summaries"
            / str(year or "unknown")
            / rec.source_case_id
        )
        seen: set[str] = set()

        for link in page_soup.select("a[href]"):
            doc_href = link.get("href")
            doc_full = abs_url(full, doc_href)
            if not doc_full or doc_full in seen:
                continue
            if not is_document_url(doc_full):
                continue
            seen.add(doc_full)
            label = link.get_text(" ", strip=True) or "Document"
            out_path = case_folder / filename_from_url(doc_full)

            extracted_text = None
            try:
                meta = download_file(s, doc_full, out_path)
                if str(out_path).lower().endswith(".pdf"):
                    extracted_text = extract_pdf_text(out_path)
                docs.append(
                    DocumentRecord(
                        source="competition_tribunal_summaries",
                        source_case_id=rec.source_case_id,
                        document_title=label,
                        document_url=doc_full,
                        local_path=str(out_path),
                        document_type="pdf/summary",
                        mime_type=meta.get("mime_type"),
                        sha256=meta.get("sha256"),
                        file_size=meta.get("file_size"),
                        extracted_text=extracted_text,
                        raw={},
                    )
                )
            except Exception as exc:
                logger.warning("[Summaries] Download failed %s: %s", doc_full, exc)
                docs.append(
                    DocumentRecord(
                        source="competition_tribunal_summaries",
                        source_case_id=rec.source_case_id,
                        document_title=label,
                        document_url=doc_full,
                        raw={"download_error": str(exc)},
                    )
                )

        logger.info("[Summaries] ✓ %s | %d chars | %d docs", title[:80], len(full_text), len(docs))
        results.append((rec, docs, []))

    logger.info("[Summaries] Done. %d summaries processed.", len(results))
    return results
