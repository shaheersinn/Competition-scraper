from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    "CompetitionCaseCa-Scraper/2.0 "
    "(Canadian legal research; contact: admin@competitioncase.ca)"
)

# Document URL patterns — broader than before to catch all linked files
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".rtf", ".htm", ".html"}
DOCUMENT_URL_TOKENS = ["/download", "download=1", "/document/", "/doc/", "fileId="]


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def slugify(value: str, max_len: int = 120) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    value = re.sub(r"-+", "-", value).strip("-._")
    return value[:max_len] or "item"


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def dump_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# BUG FIX: safe_get had NO retry logic — one network glitch silently dropped
# entire years of case law. Now retries with exponential backoff.
def safe_get(
    session_obj: requests.Session,
    url: str,
    timeout: int = 60,
    retries: int = 4,
    delay: float = 2.0,
) -> requests.Response:
    last_exc: Exception | None = None
    current_delay = delay
    for attempt in range(retries):
        try:
            time.sleep(0.5)  # polite crawl delay on every request
            resp = session_obj.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", current_delay * 2))
                logger.warning("Rate-limited on %s — sleeping %ds", url, wait)
                time.sleep(wait)
                current_delay *= 2
                continue
            if resp.status_code in (500, 502, 503, 504):
                logger.warning(
                    "Server error %d on %s (attempt %d/%d)",
                    resp.status_code, url, attempt + 1, retries,
                )
                time.sleep(current_delay)
                current_delay *= 2
                continue
            resp.raise_for_status()
            return resp
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            logger.warning(
                "Network error on %s attempt %d/%d: %s",
                url, attempt + 1, retries, exc,
            )
            time.sleep(current_delay)
            current_delay *= 2
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts") from last_exc


def download_file(
    session_obj: requests.Session,
    url: str,
    out_path: str | Path,
    retries: int = 3,
) -> dict[str, Any]:
    """Download a file with retry. Returns metadata dict."""
    ensure_parent(out_path)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with session_obj.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 128):
                        if chunk:
                            f.write(chunk)
                mime = r.headers.get("Content-Type", "")
            return {
                "file_size": os.path.getsize(out_path),
                "sha256": sha256_file(out_path),
                "mime_type": mime,
            }
        except Exception as exc:
            last_exc = exc
            logger.warning("Download attempt %d failed for %s: %s", attempt + 1, url, exc)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Download failed for {url}") from last_exc


def is_document_url(url: str) -> bool:
    """Return True if URL points to a downloadable document."""
    lower = url.lower()
    parsed = urlparse(lower)
    ext = Path(parsed.path).suffix
    if ext in DOCUMENT_EXTENSIONS:
        return True
    return any(token in lower for token in DOCUMENT_URL_TOKENS)


def extract_pdf_text(path: str | Path) -> str:
    """
    Extract plain text from a PDF using pdfminer.six.
    Returns empty string on failure — never raises.
    """
    try:
        from pdfminer.high_level import extract_text as _extract
        text = _extract(str(path))
        return text.strip() if text else ""
    except Exception as exc:
        logger.warning("PDF text extraction failed for %s: %s", path, exc)
        return ""


def extract_html_text(html: str) -> str:
    """
    Extract clean decision text from Lexum-style HTML pages.
    Strips navigation, headers, footers, breadcrumbs.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    # Remove all nav/chrome elements
    for tag in soup.find_all(
        ["nav", "header", "footer", "script", "style", "noscript"]
    ):
        tag.decompose()

    # Remove WET 4 navigation and breadcrumb elements by class/id
    for selector in [
        "#wb-tphp", "#wb-bar", "#wb-bnr", "#wb-lng", "#wb-bc", "#wb-sec",
        "#wb-info", ".WET4-breadcrumb", ".breadcrumb", "#nav-menu",
        ".printToolbar", ".wb-share", ".pager", ".pagination",
    ]:
        for el in soup.select(selector):
            el.decompose()

    # Try to find the main content container (Lexum / WET 4 standard)
    main = (
        soup.find("div", id="wb-cont")
        or soup.find("main")
        or soup.find("article")
        or soup.find("div", class_="judgmentBody")
        or soup.find("div", class_="decision-body")
        or soup.body
    )

    if not main:
        return soup.get_text("\n", strip=True)

    return main.get_text("\n", strip=True)


def abs_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith(("javascript:", "mailto:", "#")):
        return None
    return urljoin(base, href)


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "download.bin"
    return slugify(name, max_len=180)


def write_jsonl(rows: Iterable[dict[str, Any]], path: str | Path) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
