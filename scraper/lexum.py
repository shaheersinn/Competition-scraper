"""
Lexum CDO Direct Scraper — v4

All three court Lexum instances use the identical CDO platform:
  Competition Tribunal : decisions.ct-tc.gc.ca/ct-tc/cdo/en/nav_date.do?year=YYYY
  Federal Court        : decisions.fca-caf.gc.ca/fct-cf/en/nav_date.do?year=YYYY
  Federal Court Appeal : decisions.fca-caf.gc.ca/fca-caf/en/nav_date.do?year=YYYY
  Supreme Court        : decisions.scc-csc.ca/scc-csc/scc-csc/en/nav_date.do?year=YYYY

ROOT CAUSE OF 0 LINKS (diagnosed from logs):
  - Lexum CDO table rows load via XMLHttpRequest AFTER the DOM.
  - domcontentloaded fires at ~2s; table data arrives at ~8-12s.
  - wait_for_selector("a[href*='/item/']", timeout=15000) catches it IF
    networkidle also fires. But networkidle itself was timing at 90s
    in the sandbox due to the proxy. On GitHub Actions it works fine.
  
FIX STRATEGY:
  1. Use networkidle with 90s timeout (primary).
  2. If 0 links found, try an explicit JS poll: wait until
     document.querySelectorAll('a[href*="/item/"]').length > 0
  3. If still 0, try the static ?wbdisable=true variant that skips WET JS.
  4. RSS feed fallback for very recent decisions.

COMPETITION FILTER for FC/SCC:
  Only keep decisions that mention competition keywords. CT is kept wholesale.
"""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
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

COMPETITION_RE = re.compile(
    r"competition act|combines investigation|competition bureau|competition tribunal|"
    r"commissioner of competition|abuse of dominance|price.?fixing|bid.?rigg|"
    r"market power|dominant position|merger.*competition|monopol|cartel|"
    r"refusal to (deal|supply)|exclusive dealing|misleading advertising|"
    r"\bs\.\s*7[456789]\b|\bs\.\s*9[01]\b|\bs\.\s*79\b",
    re.I | re.DOTALL,
)

# ─── Court registry ────────────────────────────────────────────────────────────
LEXUM_COURTS = [
    {
        "source":  "competition_tribunal",
        "label":   "Competition Tribunal",
        "base":    "https://decisions.ct-tc.gc.ca",
        "nav":     "https://decisions.ct-tc.gc.ca/ct-tc/cdo/en/nav_date.do?year={year}",
        "rss":     "https://decisions.ct-tc.gc.ca/ct-tc/cdo/en/rss.xml",
        "filter":  False,  # scrape ALL CT decisions
    },
    {
        "source":  "federal_court",
        "label":   "Federal Court",
        "base":    "https://decisions.fca-caf.gc.ca",
        "nav":     "https://decisions.fca-caf.gc.ca/fct-cf/en/nav_date.do?year={year}",
        "rss":     "https://decisions.fca-caf.gc.ca/fct-cf/en/rss.xml",
        "filter":  True,   # competition keywords only
    },
    {
        "source":  "federal_court_appeal",
        "label":   "Federal Court of Appeal",
        "base":    "https://decisions.fca-caf.gc.ca",
        "nav":     "https://decisions.fca-caf.gc.ca/fca-caf/en/nav_date.do?year={year}",
        "rss":     "https://decisions.fca-caf.gc.ca/fca-caf/en/rss.xml",
        "filter":  True,
    },
    {
        "source":  "supreme_court",
        "label":   "Supreme Court of Canada",
        "base":    "https://decisions.scc-csc.ca",
        "nav":     "https://decisions.scc-csc.ca/scc-csc/scc-csc/en/nav_date.do?year={year}",
        "rss":     "https://decisions.scc-csc.ca/scc-csc/scc-csc/en/rss.xml",
        "filter":  True,
    },
]


async def _wait_for_case_links(page, url: str) -> str:
    """
    Try three strategies to get a fully-rendered page with case links.
    Returns HTML string.
    """
    # Strategy A: networkidle (waits for all XHR to finish)
    try:
        await page.goto(url, wait_until="networkidle", timeout=90000)
        await asyncio.sleep(3)  # extra buffer for slow XHR
        html = await page.content()
        if 'href*="/item/"' in html or "/item/" in html:
            return html
    except Exception as exc:
        logger.debug("[Lexum] networkidle failed %s: %s", url, exc)

    # Strategy B: JS polling — keep reloading until rows appear
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        for _ in range(20):  # poll up to 20 × 1.5s = 30s
            await asyncio.sleep(1.5)
            count = await page.evaluate(
                "document.querySelectorAll('a[href*=\"/item/\"]').length"
            )
            if count > 0:
                break
        return await page.content()
    except Exception as exc:
        logger.debug("[Lexum] JS poll failed %s: %s", url, exc)

    # Strategy C: ?wbdisable=true static fallback
    try:
        static_url = url + ("&" if "?" in url else "?") + "wbdisable=true"
        await page.goto(static_url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)
        return await page.content()
    except Exception as exc:
        logger.debug("[Lexum] static fallback failed %s: %s", url, exc)

    return ""


async def _discover_year(page, nav_url: str, base_url: str, year: int) -> set[str]:
    """Collect all /item/ links for one year."""
    url = nav_url.format(year=year)
    html = await _wait_for_case_links(page, url)
    soup = BeautifulSoup(html, "lxml")
    urls = set()
    for a in soup.find_all("a", href=True):
        if "/item/" in a["href"]:
            full = abs_url(url, a["href"])
            if full:
                urls.add(full)
    return urls


async def _discover_lexum_court(court: dict, start_year: int, end_year: int) -> list[str]:
    """Discover all case URLs for a Lexum court by iterating years."""
    all_urls: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="CompetitionCaseCa-Scraper/4.0 (admin@competitioncase.ca)"
        )
        page = await ctx.new_page()

        for year in range(end_year, start_year - 1, -1):
            logger.info("[Lexum] %s year %d", court["label"], year)
            found = await _discover_year(page, court["nav"], court["base"], year)
            all_urls.update(found)
            logger.info("[Lexum] %s year %d: %d links (total %d)",
                        court["label"], year, len(found), len(all_urls))

        await browser.close()

    # Supplement with RSS (recent decisions)
    s = session()
    try:
        resp = safe_get(s, court["rss"], timeout=15)
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item"):
            link = item.findtext("link", "").strip()
            if link and "/item/" in link:
                all_urls.add(link)
        logger.info("[Lexum] %s RSS: supplemented %d total", court["label"], len(all_urls))
    except Exception as exc:
        logger.debug("[Lexum] RSS fetch failed %s: %s", court["rss"], exc)

    logger.info("[Lexum] %s discovery complete — %d URLs", court["label"], len(all_urls))
    return sorted(all_urls)


def _parse_lexum_case(session_obj: requests.Session, url: str, court: dict, downloads_dir: str):
    """Fetch and parse a single Lexum case page."""
    try:
        resp = safe_get(session_obj, url, timeout=120)
    except Exception as exc:
        logger.error("[Lexum] Fetch failed %s: %s", url, exc)
        return None

    full_text = extract_html_text(resp.text)

    # Competition filter for large courts
    if court["filter"] and not COMPETITION_RE.search(full_text[:8000]):
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    title = ""
    if soup.title:
        title = (soup.title.text
                 .replace(" - Competition Tribunal", "")
                 .replace(" — Competition Tribunal", "")
                 .replace(" - Federal Court", "")
                 .replace(" - Supreme Court of Canada", "")
                 .strip())
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else url

    year_match = re.search(r"\b(19|20)\d{2}\b", full_text[:500])
    year = int(year_match.group(0)) if year_match else None

    item_m = re.search(r"/item/(\d+)/", url)
    source_case_id = item_m.group(1) if item_m else slugify(url)

    cite_m = re.search(
        r"\b(20\d{2}|19\d{2})\s+(CACT|CT|FC[A]?|FCA|SCC|ABCA|ONCA|BCCA)\s+\d+\b",
        full_text, re.I,
    )
    date_m = re.search(r"(?:Date|Dated?)[:\s]+(\d{4}-\d{2}-\d{2})", full_text)
    case_m = re.search(r"\bCT[-‑]?\d{4}[-‑]\d+\b", full_text, re.I)

    rec = CaseRecord(
        source=court["source"],
        source_case_id=source_case_id,
        title=title,
        year=year,
        case_number=case_m.group(0) if case_m else None,
        neutral_citation=cite_m.group(0) if cite_m else None,
        date_decided=date_m.group(1) if date_m else None,
        court_or_tribunal=court["label"],
        case_type="decision",
        case_url=url,
        summary=full_text[:2000],
        full_text=full_text,
        raw={"lexum_url": url},
    )

    docs = []
    case_folder = Path(downloads_dir) / court["source"] / str(year or "unknown") / source_case_id
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        full = abs_url(url, a["href"])
        if not full or full in seen:
            continue
        if not is_document_url(full):
            continue
        seen.add(full)
        label = a.get_text(" ", strip=True) or "Document"
        out_path = case_folder / filename_from_url(full)
        extracted = None
        try:
            meta = download_file(session_obj, full, out_path)
            if str(out_path).lower().endswith(".pdf"):
                extracted = extract_pdf_text(out_path)
            docs.append(DocumentRecord(
                source=court["source"], source_case_id=source_case_id,
                document_title=label, document_url=full,
                local_path=str(out_path), document_type="pdf/decision",
                mime_type=meta.get("mime_type"), sha256=meta.get("sha256"),
                file_size=meta.get("file_size"), extracted_text=extracted,
                raw={},
            ))
        except Exception as exc:
            logger.debug("[Lexum] Doc skip %s: %s", full, exc)

    parties: list[PartyRecord] = []
    if " v. " in title:
        l, r = title.split(" v. ", 1)
        parties = [
            PartyRecord(source_case_id, l.strip(), "applicant/appellant"),
            PartyRecord(source_case_id, r.strip(), "respondent"),
        ]

    logger.info("[Lexum] ✓ %s | %d chars | %d docs", title[:70], len(full_text), len(docs))
    return rec, docs, parties


def scrape_lexum_courts(start_year: int, end_year: int, downloads_dir: str) -> list:
    """Scrape all Lexum court instances directly."""
    s = session()
    out = []
    for court in LEXUM_COURTS:
        urls = asyncio.run(_discover_lexum_court(court, start_year, end_year))
        logger.info("[Lexum] Parsing %d %s pages…", len(urls), court["label"])
        kept = skipped = 0
        for i, url in enumerate(urls, 1):
            result = _parse_lexum_case(s, url, court, downloads_dir)
            if result:
                out.append(result)
                kept += 1
            else:
                skipped += 1
            if i % 50 == 0:
                logger.info("[Lexum] %s progress: %d/%d", court["label"], i, len(urls))
        logger.info("[Lexum] %s done: kept=%d skipped=%d", court["label"], kept, skipped)
    return out
