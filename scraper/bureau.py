"""
Competition Bureau scraper — v3

FIXES in this version:
1. Old domain (competitionbureau.gc.ca) blocked — was causing 6-min timeouts
   per URL (120s timeout × 3 retries).
2. 404 URLs no longer retried — was wasting ~30s per dead link.
3. Document detection tightened — was finding 38-46 "docs" per case because
   is_document_url() matched HTML navigation links. Now only real files.
4. Moved to the current competition-bureau.canada.ca domain throughout.
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
    is_blocked_domain,
    is_document_url,
    safe_get,
    session,
    slugify,
)

logger = logging.getLogger(__name__)

BUREAU_BASE = "https://competition-bureau.canada.ca"

# Current live list pages (wbdisable=true bypasses WET JS for cleaner HTML)
CASE_LIST_URLS = [
    (
        "competition_bureau_rtp",
        BUREAU_BASE + "/restrictive-trade-practices/cases-and-outcomes/"
        "restrictive-trade-practices-cases-and-outcomes?wbdisable=true",
    ),
    (
        "competition_bureau_dmp",
        BUREAU_BASE + "/en/deceptive-marketing-practices/cases-and-outcomes?wbdisable=true",
    ),
]

# Link tokens that definitely indicate a Bureau case detail page
CASE_LINK_TOKENS = [
    "/cases/", "/case-", "/enforcement/", "/consent-",
    "/matters/", "/matter-", "/agreement", "/court-order",
    "/restrictive-trade-practices/", "/deceptive-marketing",
]


def _is_case_link(url: str, list_url: str) -> bool:
    """Return True if the URL looks like a Bureau case detail page."""
    if not url or url == list_url:
        return False
    if is_blocked_domain(url):
        return False
    lower = url.lower()
    # Skip obvious non-case links
    if any(x in lower for x in [
        "/search", "/home", "/contact", "/about", "/subscribe",
        "/newsroom", "#", "javascript", "mailto", "/rss",
        "/en/competition-bureau", "/fr/bureau",
    ]):
        return False
    # Must be on the Bureau domain
    if "competition-bureau.canada.ca" not in lower and "canada.ca/en/competition" not in lower:
        return False
    return True


def _parse_bureau_page(source_name: str, list_url: str, downloads_dir: str):
    s = session()
    try:
        html = safe_get(s, list_url).text
    except Exception as exc:
        logger.error("[Bureau] Failed to fetch list %s: %s", list_url, exc)
        return []

    soup = BeautifulSoup(html, "lxml")
    records = []
    seen_urls: set[str] = set()

    # Collect case detail page links from the list
    case_links: list[tuple[str, str]] = []  # (url, title)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = abs_url(list_url, href)
        if not full or full in seen_urls:
            continue
        if not _is_case_link(full, list_url):
            continue
        seen_urls.add(full)
        title = a.get_text(" ", strip=True)
        if title:
            case_links.append((full, title))

    logger.info("[Bureau] %s: found %d case links", source_name, len(case_links))

    for detail_url, link_title in case_links:
        source_case_id = slugify(detail_url.rstrip("/").split("/")[-1] or link_title, max_len=120)

        # Fetch detail page
        full_text = ""
        try:
            detail_resp = safe_get(s, detail_url, timeout=30)
            full_text = extract_html_text(detail_resp.text)
            detail_soup = BeautifulSoup(detail_resp.text, "lxml")
        except Exception as exc:
            logger.warning("[Bureau] Detail page failed %s: %s", detail_url, exc)
            detail_soup = BeautifulSoup("", "lxml")
            full_text = link_title

        # Use h1 as title if available
        h1 = detail_soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else link_title

        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", full_text[:500])
        date_str = date_match.group(1) if date_match else None
        year = int(date_str[:4]) if date_str else None

        rec = CaseRecord(
            source=source_name,
            source_case_id=source_case_id,
            title=title,
            year=year,
            date_decided=date_str,
            court_or_tribunal="Competition Bureau Canada",
            case_type="public enforcement outcome",
            case_url=detail_url,
            summary=full_text[:2000],
            full_text=full_text,
            raw={"list_url": list_url, "detail_url": detail_url},
        )

        # Download ONLY actual documents (PDFs, consent agreements, court orders)
        # FIX: previously found 38-46 "docs" which were all nav HTML links
        docs = []
        case_folder = (
            Path(downloads_dir) / source_name / str(year or "unknown") / source_case_id
        )
        seen_doc_urls: set[str] = set()

        for doc_link in detail_soup.select("a[href]"):
            doc_href = doc_link.get("href")
            doc_full = abs_url(detail_url, doc_href)
            if not doc_full or doc_full in seen_doc_urls:
                continue
            if is_blocked_domain(doc_full):
                continue
            if not is_document_url(doc_full):
                continue
            seen_doc_urls.add(doc_full)
            doc_label = doc_link.get_text(" ", strip=True) or "Document"
            out_path = case_folder / filename_from_url(doc_full)
            extracted_text = None
            try:
                meta = download_file(s, doc_full, out_path)
                if str(out_path).lower().endswith(".pdf"):
                    extracted_text = extract_pdf_text(out_path)
                docs.append(DocumentRecord(
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
                ))
            except Exception as exc:
                # Log at debug level — dead links on Bureau site are common
                logger.debug("[Bureau] Doc skipped %s: %s", doc_full, exc)

        logger.info(
            "[Bureau] ✓ %s | %d chars | %d real docs",
            title[:80], len(full_text), len(docs),
        )
        records.append((rec, docs, []))

    logger.info("[Bureau] %s: %d records total", source_name, len(records))
    return records


def scrape_bureau_sources(downloads_dir: str = "downloads"):
    out = []
    for source_name, list_url in CASE_LIST_URLS:
        out.extend(_parse_bureau_page(source_name, list_url, downloads_dir))
    return out
