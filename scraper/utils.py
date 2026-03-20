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
    "CompetitionCaseCa-Scraper/3.0 "
    "(Canadian legal research; contact: admin@competitioncase.ca)"
)

# ── Domain blocklist ────────────────────────────────────────────────────────
# Old/dead domains that waste minutes on connection timeouts
BLOCKED_DOMAINS = {
    "competitionbureau.gc.ca",       # old domain, now times out
    "cb-bc.nsf",                     # old NSF site fragment
    "strategis.gc.ca",               # decommissioned Industry Canada site
}

# ── Document detection ──────────────────────────────────────────────────────
# ONLY match actual downloadable files — NOT html/htm navigation links.
# The previous version matched /doc/ and /document/ which are in many nav URLs.
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".rtf", ".txt", ".odt", ".zip"}

# URL query/path tokens that explicitly signal a file download
DOWNLOAD_TOKENS = {
    "download=1",
    "attachment=1",
    "inline=false",
    "/getattachment/",
    "/downloadfile/",
    "/documentretriever",
    "fileid=",
}


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


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def is_blocked_domain(url: str) -> bool:
    """Return True if this URL's domain is in the blocklist."""
    try:
        host = urlparse(url).netloc.lower()
        return any(bd in host for bd in BLOCKED_DOMAINS)
    except Exception:
        return False


def is_document_url(url: str) -> bool:
    """
    Return True ONLY if the URL points to an actual downloadable file.
    Deliberately conservative — HTML navigation links return False.
    """
    if not url:
        return False
    if is_blocked_domain(url):
        return False

    lower = url.lower()
    parsed = urlparse(lower)
    ext = Path(parsed.path).suffix

    # Must have a real file extension OR an explicit download token
    if ext in DOCUMENT_EXTENSIONS:
        return True
    lower_full = lower + ("?" + parsed.query if parsed.query else "")
    return any(token in lower_full for token in DOWNLOAD_TOKENS)


def safe_get(
    session_obj: requests.Session,
    url: str,
    timeout: int = 60,
    retries: int = 4,
    delay: float = 2.0,
) -> requests.Response:
    """
    GET with exponential backoff.
    KEY FIX: Does NOT retry on 404 — that's a permanent failure, not transient.
    Does NOT attempt blocked domains at all.
    """
    if is_blocked_domain(url):
        raise RuntimeError(f"Domain blocked: {url}")

    last_exc: Exception | None = None
    current_delay = delay
    for attempt in range(retries):
        try:
            time.sleep(0.5)
            resp = session_obj.get(url, timeout=timeout)

            # 404 = permanent — don't retry, raise immediately
            if resp.status_code == 404:
                resp.raise_for_status()

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", current_delay * 2))
                logger.warning("Rate-limited on %s — sleeping %ds", url, wait)
                time.sleep(wait)
                current_delay *= 2
                continue

            if resp.status_code in (500, 502, 503, 504):
                logger.warning("Server error %d on %s (attempt %d)", resp.status_code, url, attempt + 1)
                time.sleep(current_delay)
                current_delay *= 2
                continue

            resp.raise_for_status()
            return resp

        except requests.HTTPError:
            raise  # 404s and other HTTP errors — don't retry

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            logger.warning("Network error on %s attempt %d: %s", url, attempt + 1, exc)
            time.sleep(current_delay)
            current_delay *= 2

    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts") from last_exc


def download_file(
    session_obj: requests.Session,
    url: str,
    out_path: str | Path,
    retries: int = 3,
) -> dict[str, Any]:
    """Download a file with retry. Skips blocked domains. Doesn't retry 404."""
    if is_blocked_domain(url):
        raise RuntimeError(f"Domain blocked: {url}")

    ensure_parent(out_path)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with session_obj.get(url, stream=True, timeout=120) as r:
                if r.status_code == 404:
                    r.raise_for_status()  # immediate failure, no retry
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
        except requests.HTTPError:
            raise  # Don't retry HTTP errors
        except Exception as exc:
            last_exc = exc
            logger.warning("Download attempt %d failed for %s: %s", attempt + 1, url, exc)
            time.sleep(2 ** attempt)

    raise RuntimeError(f"Download failed for {url}") from last_exc


def extract_html_text(html: str) -> str:
    """Extract clean decision text, stripping all nav/chrome."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["nav", "header", "footer", "script", "style", "noscript"]):
        tag.decompose()
    for selector in [
        "#wb-tphp", "#wb-bar", "#wb-bnr", "#wb-lng", "#wb-bc", "#wb-sec",
        "#wb-info", ".breadcrumb", "#nav-menu", ".printToolbar",
        ".wb-share", ".pager", ".pagination", ".wb-fnote",
    ]:
        for el in soup.select(selector):
            el.decompose()

    main = (
        soup.find("div", id="wb-cont")
        or soup.find("main")
        or soup.find("article")
        or soup.find("div", class_=re.compile(r"judgment|decision|content", re.I))
        or soup.body
    )
    return (main or soup).get_text("\n", strip=True) if main else soup.get_text("\n", strip=True)


def extract_pdf_text(path: str | Path) -> str:
    """Extract text from a PDF. Returns empty string on failure."""
    try:
        from pdfminer.high_level import extract_text as _extract
        text = _extract(str(path))
        return text.strip() if text else ""
    except Exception as exc:
        logger.warning("PDF text extraction failed for %s: %s", path, exc)
        return ""


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


def dump_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
