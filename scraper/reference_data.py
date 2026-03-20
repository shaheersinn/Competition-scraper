"""
Reference Data Scraper — v3

Downloads and stores all non-case reference material used by CompetitionCase.ca:

  CATEGORY                   SOURCE
  ─────────────────────────────────────────────────────────────────────────────
  enforcement/release        Bureau enforcement press releases (HTML→PDF)
  enforcement/consent-order  Consent agreements & orders (PDF direct)
  enforcement/rss            Bureau RSS feed — all enforcement actions
  market-study               Bureau published market studies (PDF)
  stats/industry-conc        Statistics Canada NAICS concentration data (CSV→stored)
  stats/telecom              CRTC Communications Monitoring Reports (PDF)
  stats/banking              OSFI financial institution market share (CSV/PDF)
  stats/airlines             Transport Canada air travel statistics (PDF/CSV)
  stats/grocery              Retail Council grocery market data (PDF)
  legal/act                  Competition Act full text (HTML→PDF)
  legal/rules                Competition Tribunal Rules (PDF)
  legal/guidelines           Bureau guidelines & bulletins (PDF)
  legal/key-decision         Martin 2026 and other defining decisions (HTML→PDF)
  consumer/complaints        Canadian Consumer Handbook complaint data
  consumer/open-data         Open Government Canada datasets

All items are saved to:
  downloads/reference/<category>/<sub_category>/<safe_title>.<ext>

PDF is the primary storage format. For HTML pages we generate a PDF using
playwright's page.pdf(). For CSV/JSON datasets we save the original format
and also a companion .txt summary extracted from the content.
"""
from __future__ import annotations

import asyncio
import io
import logging
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

from .models import ReferenceDocument
from .utils import (
    abs_url,
    download_file,
    extract_html_text,
    extract_pdf_text,
    filename_from_url,
    is_blocked_domain,
    safe_get,
    session,
    sha256_file,
    slugify,
)

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _ref_path(downloads_dir: str, category: str, sub_category: str | None, filename: str) -> Path:
    """Build the canonical download path for a reference document."""
    parts = [downloads_dir, "reference", category]
    if sub_category:
        parts.append(sub_category)
    parts.append(filename)
    return Path(*parts)


def _save_html_as_pdf(url: str, out_path: Path, session_obj: requests.Session) -> dict:
    """
    Save an HTML page as PDF using Playwright's built-in PDF renderer.
    Falls back to saving raw HTML if Playwright fails.
    Returns metadata dict identical to download_file().
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    async def _render():
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent="CompetitionCaseCa-Scraper/3.0 (admin@competitioncase.ca)"
            )
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.pdf(path=str(out_path), format="A4", print_background=False)
            await browser.close()

    try:
        asyncio.run(_render())
        return {
            "file_size": out_path.stat().st_size,
            "sha256": sha256_file(out_path),
            "mime_type": "application/pdf",
        }
    except Exception as exc:
        logger.warning("Playwright PDF failed for %s, saving HTML: %s", url, exc)
        # Fallback: save HTML directly
        html_path = out_path.with_suffix(".html")
        try:
            resp = safe_get(session_obj, url, timeout=30)
            html_path.write_bytes(resp.content)
            return {
                "file_size": html_path.stat().st_size,
                "sha256": sha256_file(html_path),
                "mime_type": "text/html",
            }
        except Exception as exc2:
            raise RuntimeError(f"Both PDF and HTML save failed for {url}: {exc2}") from exc2


def _make_ref(
    category: str,
    sub_category: str | None,
    title: str,
    source_url: str,
    publisher: str,
    description: str,
    downloads_dir: str,
    session_obj: requests.Session,
    is_html: bool = False,
    published_date: str | None = None,
    extra_raw: dict | None = None,
) -> ReferenceDocument | None:
    """
    Download one reference document and return a ReferenceDocument record.
    Handles PDF, CSV, JSON, and HTML (converted to PDF).
    """
    if is_blocked_domain(source_url):
        logger.warning("Blocked domain, skipping: %s", source_url)
        return None

    ext = Path(urlparse(source_url).path).suffix.lower()
    safe_name = slugify(title, max_len=120)

    if is_html or ext in ("", ".html", ".htm", ".aspx", ".php"):
        out_path = _ref_path(downloads_dir, category, sub_category, safe_name + ".pdf")
        try:
            meta = _save_html_as_pdf(source_url, out_path, session_obj)
            actual_path = out_path
        except Exception as exc:
            logger.warning("HTML→PDF failed for %s: %s", source_url, exc)
            return None
    else:
        # Direct download (PDF, CSV, JSON, XML, ZIP)
        out_path = _ref_path(downloads_dir, category, sub_category, safe_name + ext)
        try:
            meta = download_file(session_obj, source_url, out_path)
            actual_path = out_path
        except Exception as exc:
            logger.warning("Download failed for %s: %s", source_url, exc)
            return None

    # Extract text
    extracted_text = None
    path_lower = str(actual_path).lower()
    if path_lower.endswith(".pdf"):
        extracted_text = extract_pdf_text(actual_path)
    elif path_lower.endswith((".html", ".htm")):
        extracted_text = extract_html_text(actual_path.read_text(errors="replace"))
    elif path_lower.endswith((".csv", ".txt", ".json")):
        try:
            extracted_text = actual_path.read_text(encoding="utf-8", errors="replace")[:50000]
        except Exception:
            pass

    logger.info("[Ref] ✓ [%s/%s] %s → %s (%s bytes)",
                category, sub_category or "-", title[:60],
                actual_path.name, meta.get("file_size", "?"))

    return ReferenceDocument(
        category=category,
        sub_category=sub_category,
        title=title,
        source_url=source_url,
        local_path=str(actual_path),
        file_type=actual_path.suffix.lstrip("."),
        publisher=publisher,
        published_date=published_date,
        description=description,
        extracted_text=extracted_text,
        sha256=meta.get("sha256"),
        file_size=meta.get("file_size"),
        mime_type=meta.get("mime_type"),
        raw=extra_raw or {},
    )


# ══════════════════════════════════════════════════════════════════════════════
# 1. BUREAU ENFORCEMENT RELEASES + RSS
# ══════════════════════════════════════════════════════════════════════════════

BUREAU_NEW_BASE = "https://competition-bureau.canada.ca"
BUREAU_NEW_NEWS = BUREAU_NEW_BASE + "/en/news?wbdisable=true"

# Specific enforcement pages on the new domain
BUREAU_ENFORCEMENT_PAGES = [
    (BUREAU_NEW_BASE + "/en/how-we-foster-competition/education-and-outreach/news-releases?wbdisable=true",
     "enforcement/release", "News Releases"),
    (BUREAU_NEW_BASE + "/en/how-we-foster-competition/enforcement-matters?wbdisable=true",
     "enforcement/consent-order", "Enforcement Matters"),
    (BUREAU_NEW_BASE + "/restrictive-trade-practices/cases-and-outcomes/restrictive-trade-practices-cases-and-outcomes?wbdisable=true",
     "enforcement/consent-order", "Restrictive Trade Practices Cases"),
    (BUREAU_NEW_BASE + "/en/deceptive-marketing-practices/cases-and-outcomes?wbdisable=true",
     "enforcement/consent-order", "Deceptive Marketing Cases"),
]

# The Bureau's RSS feed (old domain URL but still valid — redirects)
BUREAU_RSS_URLS = [
    "https://www.canada.ca/en/competition-bureau.atom.xml",  # New Atom feed
    "https://competition-bureau.canada.ca/en/news.rss",      # May 404 but try
]


def _scrape_bureau_rss(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    """Parse the Bureau's RSS/Atom feed and save each item as a PDF."""
    out = []
    for rss_url in BUREAU_RSS_URLS:
        try:
            resp = safe_get(session_obj, rss_url, timeout=20)
            root = ET.fromstring(resp.content)
        except Exception as exc:
            logger.warning("[Bureau RSS] Failed to fetch %s: %s", rss_url, exc)
            continue

        # Handle both RSS 2.0 and Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        logger.info("[Bureau RSS] Found %d items in %s", len(items), rss_url)

        for item in items:
            # RSS 2.0
            title_el = item.find("title")
            link_el = item.find("link")
            date_el = item.find("pubDate") or item.find("dc:date")
            desc_el = item.find("description") or item.find("summary")
            # Atom
            if link_el is None:
                link_el = item.find("atom:link", ns)
            link = (link_el.text or link_el.get("href", "") if link_el is not None else "")
            title = title_el.text.strip() if title_el is not None and title_el.text else link
            date_str = date_el.text.strip()[:10] if date_el is not None and date_el.text else None
            desc = desc_el.text.strip()[:500] if desc_el is not None and desc_el.text else ""

            if not link or is_blocked_domain(link):
                continue

            ref = _make_ref(
                category="enforcement",
                sub_category="rss",
                title=title,
                source_url=link,
                publisher="Competition Bureau Canada",
                description=desc,
                downloads_dir=downloads_dir,
                session_obj=session_obj,
                is_html=True,
                published_date=date_str,
                extra_raw={"rss_source": rss_url},
            )
            if ref:
                out.append(ref)

        if out:
            break  # Got results from first working feed

    logger.info("[Bureau RSS] Scraped %d enforcement release items", len(out))
    return out


def _scrape_bureau_enforcement_pages(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    """Scrape Bureau enforcement pages — save each release as PDF."""
    out = []
    for list_url, category, label in BUREAU_ENFORCEMENT_PAGES:
        try:
            resp = safe_get(session_obj, list_url, timeout=30)
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception as exc:
            logger.warning("[Bureau enforcement] List page failed %s: %s", list_url, exc)
            continue

        # Find all links that look like enforcement items
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = abs_url(list_url, href)
            if not full or full in seen or is_blocked_domain(full):
                continue
            if BUREAU_NEW_BASE not in full:
                continue
            title = a.get_text(" ", strip=True)
            if not title or len(title) < 5:
                continue
            # Skip nav/utility links
            if any(x in full.lower() for x in ["/search", "/home", "/subscribe", "#", "mailto"]):
                continue
            seen.add(full)

            sub = "release" if "news-release" in full.lower() or "release" in category else "consent-order"
            ref = _make_ref(
                category="enforcement",
                sub_category=sub,
                title=title,
                source_url=full,
                publisher="Competition Bureau Canada",
                description=f"{label} — {title}",
                downloads_dir=downloads_dir,
                session_obj=session_obj,
                is_html=True,
            )
            if ref:
                out.append(ref)

    logger.info("[Bureau enforcement] Scraped %d enforcement items", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 2. BUREAU MARKET STUDIES
# ══════════════════════════════════════════════════════════════════════════════

MARKET_STUDIES_PAGE = BUREAU_NEW_BASE + "/en/how-we-foster-competition/education-and-outreach/market-studies?wbdisable=true"

# Known direct PDF URLs for major studies (fallback if scraping misses them)
KNOWN_MARKET_STUDIES = [
    (
        "Retail Grocery — Market Study Report (2023)",
        "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/market-studies/retail-grocery-market-study-report",
        "grocery-retail", "2023",
        "Full report on grocery retail market. Includes HHI data, market shares for Loblaw, Sobeys, Metro, Walmart, Costco.",
    ),
    (
        "Digital Advertising — Market Study (2022)",
        "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/market-studies/digital-advertising-market-study",
        "digital-advertising", "2022",
        "Examination of digital advertising markets, Google/Meta dominance, HHI estimates.",
    ),
    (
        "Real Estate — Market Study (2022)",
        "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/market-studies/real-estate-market-study",
        "real-estate", "2022",
        "Examination of real estate brokerage market, CREA MLS policies, commission structures.",
    ),
    (
        "Competition in Canada's Insulin Market (2020)",
        "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/market-studies/competition-canadas-insulin-market",
        "pharma", "2020",
        "Market study on insulin pricing and competition barriers.",
    ),
    (
        "Competition Issues in the Wireless Telecommunications Sector",
        "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/market-studies/wireless-telecommunications-sector",
        "telecom", None,
        "Analysis of wireless competition, spectrum allocation, barriers to entry.",
    ),
]


def _scrape_market_studies(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []

    # First: dynamic scraping of the studies index page
    try:
        resp = safe_get(session_obj, MARKET_STUDIES_PAGE, timeout=30)
        soup = BeautifulSoup(resp.text, "lxml")
        seen: set[str] = set()

        for a in soup.find_all("a", href=True):
            full = abs_url(MARKET_STUDIES_PAGE, a["href"])
            if not full or full in seen or is_blocked_domain(full):
                continue
            if "market-stud" not in full.lower() and ".pdf" not in full.lower():
                continue
            seen.add(full)
            title = a.get_text(" ", strip=True) or "Market Study"
            sub = "general"
            for keyword, sub_cat in [
                ("grocery", "grocery-retail"), ("retail", "grocery-retail"),
                ("digital", "digital-advertising"), ("advertis", "digital-advertising"),
                ("real estate", "real-estate"), ("insulin", "pharma"),
                ("wireless", "telecom"), ("pharma", "pharma"),
            ]:
                if keyword in title.lower() or keyword in full.lower():
                    sub = sub_cat
                    break
            ref = _make_ref(
                category="market-study", sub_category=sub,
                title=title, source_url=full,
                publisher="Competition Bureau Canada",
                description=f"Bureau market study: {title}",
                downloads_dir=downloads_dir, session_obj=session_obj,
                is_html=".pdf" not in full.lower(),
            )
            if ref:
                out.append(ref)
    except Exception as exc:
        logger.warning("[Market studies] Index scrape failed: %s", exc)

    # Then: ensure all known studies are captured
    fetched_urls = {r.source_url for r in out}
    for title, url, sub_cat, year, desc in KNOWN_MARKET_STUDIES:
        if url in fetched_urls:
            continue
        ref = _make_ref(
            category="market-study", sub_category=sub_cat,
            title=title, source_url=url,
            publisher="Competition Bureau Canada",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=True, published_date=year,
        )
        if ref:
            out.append(ref)

    logger.info("[Market studies] Scraped %d market study documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 3. STATISTICS CANADA — INDUSTRY CONCENTRATION
# ══════════════════════════════════════════════════════════════════════════════

STATSCAN_CATALOGUE = "https://www150.statcan.gc.ca/n1/en/catalogue/11-621-M"
STATSCAN_BUSINESS_REGISTER = "https://www150.statcan.gc.ca/n1/pub/11-621-m/2021001/tbl/tbl01-eng.htm"

STATSCAN_KNOWN = [
    (
        "Analytical Studies Branch Research Paper — Business Dynamism (11-621-M)",
        STATSCAN_CATALOGUE,
        "2023",
        "Market concentration analytical papers series. Contains HHI estimates by NAICS code.",
    ),
    (
        "Corporate concentration in Canada: A data profile",
        "https://www150.statcan.gc.ca/n1/pub/11-621-m/2021001/article/00001-eng.htm",
        "2021",
        "Concentration ratios (CR4, CR8) and HHI by industry sector.",
    ),
    (
        "Firm Dynamics — Entry, Exit and Market Concentration",
        "https://www150.statcan.gc.ca/n1/pub/11-621-m/2019008/article/00001-eng.htm",
        "2019",
        "Long-run trends in Canadian industry concentration.",
    ),
]


def _scrape_statscan(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for title, url, year, desc in STATSCAN_KNOWN:
        ref = _make_ref(
            category="stats", sub_category="industry-concentration",
            title=title, source_url=url,
            publisher="Statistics Canada",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=True, published_date=year,
        )
        if ref:
            out.append(ref)

    # Try to dynamically find CSV downloads on the catalogue page
    try:
        resp = safe_get(session_obj, STATSCAN_CATALOGUE, timeout=20)
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            full = abs_url(STATSCAN_CATALOGUE, a["href"])
            if not full:
                continue
            if any(ext in full.lower() for ext in [".csv", ".zip", "download"]):
                title = a.get_text(" ", strip=True) or "StatsCan Dataset"
                ref = _make_ref(
                    category="stats", sub_category="industry-concentration",
                    title=title, source_url=full,
                    publisher="Statistics Canada",
                    description="Statistics Canada concentration dataset",
                    downloads_dir=downloads_dir, session_obj=session_obj,
                )
                if ref:
                    out.append(ref)
    except Exception as exc:
        logger.warning("[StatsCan] Dynamic scrape failed: %s", exc)

    logger.info("[StatsCan] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 4. CRTC — TELECOM & BROADCASTING MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════

CRTC_REPORTS = [
    (
        "Communications Monitoring Report 2023",
        "https://crtc.gc.ca/eng/publications/reports/PolicyMonitoring/2023/cmr2023.pdf",
        "2023",
        "Annual monitoring report. Contains HHI for telecom, broadcasting, internet. Market share by carrier.",
    ),
    (
        "Communications Monitoring Report 2022",
        "https://crtc.gc.ca/eng/publications/reports/PolicyMonitoring/2022/cmr2022.pdf",
        "2022",
        "Annual CRTC monitoring report — telecom/broadcasting market concentration 2022.",
    ),
    (
        "Communications Monitoring Report 2021",
        "https://crtc.gc.ca/eng/publications/reports/PolicyMonitoring/2021/cmr2021.pdf",
        "2021",
        "Annual CRTC monitoring report 2021.",
    ),
    (
        "Wireless Market — Sector Review 2023",
        "https://crtc.gc.ca/eng/publications/reports/PolicyMonitoring/2023/cmr2023c3.htm",
        "2023",
        "Detailed wireless market analysis with operator market shares and HHI.",
    ),
    (
        "Internet Service — Market Report 2023",
        "https://crtc.gc.ca/eng/publications/reports/PolicyMonitoring/2023/cmr2023c4.htm",
        "2023",
        "ISP market shares and concentration metrics.",
    ),
]


def _scrape_crtc(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for title, url, year, desc in CRTC_REPORTS:
        ref = _make_ref(
            category="stats", sub_category="telecom",
            title=title, source_url=url,
            publisher="CRTC",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=url.endswith(".htm"),
            published_date=year,
        )
        if ref:
            out.append(ref)
    logger.info("[CRTC] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 5. OSFI — BANKING MARKET SHARE DATA
# ══════════════════════════════════════════════════════════════════════════════

OSFI_PAGES = [
    (
        "OSFI — Banks and Trust Companies Quarterly Data (2024)",
        "https://www.osfi-bsif.gc.ca/en/data-forms/data-research/banks-trust-companies",
        "2024",
        "Quarterly balance sheet and market share data for Schedule I/II banks. Use for HHI in banking sector.",
    ),
    (
        "OSFI — Deposit-Taking Institutions Statistics",
        "https://www.osfi-bsif.gc.ca/en/data-forms/data-research/deposit-taking-statistics",
        "2024",
        "Aggregate statistics on market share for deposit-taking institutions.",
    ),
    (
        "OSFI — Insurance Industry Statistics",
        "https://www.osfi-bsif.gc.ca/en/data-forms/data-research/insurance-industry-statistics",
        "2024",
        "P&C and life insurance market concentration data.",
    ),
]


def _scrape_osfi(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for title, url, year, desc in OSFI_PAGES:
        ref = _make_ref(
            category="stats", sub_category="banking",
            title=title, source_url=url,
            publisher="OSFI",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=True, published_date=year,
        )
        if ref:
            out.append(ref)
        # Try to find CSV/PDF downloads on the page
        try:
            resp = safe_get(session_obj, url, timeout=20)
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                full = abs_url(url, a["href"])
                if full and any(ext in full.lower() for ext in [".csv", ".xlsx", ".pdf", ".zip"]):
                    dl_title = a.get_text(" ", strip=True) or "OSFI Dataset"
                    dl_ref = _make_ref(
                        category="stats", sub_category="banking",
                        title=dl_title, source_url=full,
                        publisher="OSFI",
                        description=f"OSFI data download: {dl_title}",
                        downloads_dir=downloads_dir, session_obj=session_obj,
                        published_date=year,
                    )
                    if dl_ref:
                        out.append(dl_ref)
        except Exception:
            pass
    logger.info("[OSFI] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 6. TRANSPORT CANADA — AIRLINE STATISTICS
# ══════════════════════════════════════════════════════════════════════════════

TC_PAGES = [
    (
        "Air Travel Statistics — Annual Report 2023",
        "https://tc.gc.ca/en/services/aviation/air-travel/air-travel-statistics.html",
        "2023",
        "Domestic and international airline market share by carrier. Use for aviation sector HHI.",
    ),
    (
        "Air Travel Complaints Commissioner — Annual Report 2023",
        "https://otc-cta.gc.ca/eng/air-travel-complaints-annual-report",
        "2023",
        "Passenger complaint volumes by carrier — useful for consumer harm analysis.",
    ),
]


def _scrape_transport_canada(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for title, url, year, desc in TC_PAGES:
        ref = _make_ref(
            category="stats", sub_category="airlines",
            title=title, source_url=url,
            publisher="Transport Canada",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=True, published_date=year,
        )
        if ref:
            out.append(ref)
        # Check for CSV/PDF links on the page
        try:
            resp = safe_get(session_obj, url, timeout=20)
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                full = abs_url(url, a["href"])
                if full and any(ext in full.lower() for ext in [".csv", ".xlsx", ".pdf"]):
                    dl_title = a.get_text(" ", strip=True) or "TC Dataset"
                    dl_ref = _make_ref(
                        category="stats", sub_category="airlines",
                        title=dl_title, source_url=full,
                        publisher="Transport Canada",
                        description=f"Air travel data: {dl_title}",
                        downloads_dir=downloads_dir, session_obj=session_obj,
                        published_date=year,
                    )
                    if dl_ref:
                        out.append(dl_ref)
        except Exception:
            pass
    logger.info("[Transport Canada] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 7. RETAIL COUNCIL — GROCERY MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════

RCC_PAGES = [
    (
        "Retail Council of Canada — Grocery Industry Overview 2024",
        "https://www.retailcouncil.org/resources/quick-facts/grocery-industry-overview/",
        "2024",
        "Annual market share data for major Canadian grocery chains. Loblaw, Sobeys, Metro, Walmart, Costco.",
    ),
    (
        "RCC — State of Retail Canada 2023",
        "https://www.retailcouncil.org/resources/research-and-reports/",
        "2023",
        "Annual retail industry report including concentration metrics.",
    ),
]


def _scrape_retail_council(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for title, url, year, desc in RCC_PAGES:
        ref = _make_ref(
            category="stats", sub_category="grocery",
            title=title, source_url=url,
            publisher="Retail Council of Canada",
            description=desc,
            downloads_dir=downloads_dir, session_obj=session_obj,
            is_html=True, published_date=year,
        )
        if ref:
            out.append(ref)
    logger.info("[Retail Council] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 8. OPEN GOVERNMENT CANADA — CONSUMER & MARKET DATASETS
# ══════════════════════════════════════════════════════════════════════════════

OPEN_CANADA_API = "https://open.canada.ca/data/api/3/action/package_search"
OPEN_CANADA_SEARCHES = [
    ("consumer complaints Competition Bureau", "consumer/complaints"),
    ("market concentration industry", "stats/industry-concentration"),
    ("competition enforcement", "enforcement/release"),
]

CANADIAN_CONSUMER_HANDBOOK = "https://www.canadianconsumerhandbook.ca/complaints"


def _scrape_open_canada(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []

    # Search Open Canada API for relevant datasets
    for search_term, category_path in OPEN_CANADA_SEARCHES:
        cat, sub = (category_path.split("/", 1) + [None])[:2]
        try:
            resp = safe_get(
                session_obj,
                f"{OPEN_CANADA_API}?q={requests.utils.quote(search_term)}&rows=10",
                timeout=20,
            )
            data = resp.json()
            results = data.get("result", {}).get("results", [])
        except Exception as exc:
            logger.warning("[Open Canada] API search failed '%s': %s", search_term, exc)
            continue

        for pkg in results:
            pkg_title = pkg.get("title", {})
            if isinstance(pkg_title, dict):
                pkg_title = pkg_title.get("en", "") or next(iter(pkg_title.values()), "")
            pkg_title = str(pkg_title)

            for resource in pkg.get("resources", []):
                res_url = resource.get("url", "")
                res_fmt = resource.get("format", "").lower()
                res_title = resource.get("name", pkg_title)
                if isinstance(res_title, dict):
                    res_title = res_title.get("en", "") or next(iter(res_title.values()), "")

                if not res_url or is_blocked_domain(res_url):
                    continue
                # Only grab structured data or PDFs
                if res_fmt not in ("csv", "json", "geojson", "pdf", "xlsx", "xml"):
                    continue

                ref = _make_ref(
                    category=cat, sub_category=sub,
                    title=str(res_title) or pkg_title,
                    source_url=res_url,
                    publisher=str(pkg.get("organization", {}).get("title", "Government of Canada")),
                    description=f"Open Canada dataset: {pkg_title}",
                    downloads_dir=downloads_dir, session_obj=session_obj,
                )
                if ref:
                    out.append(ref)

    # Canadian Consumer Handbook
    ref = _make_ref(
        category="consumer", sub_category="complaints",
        title="Canadian Consumer Handbook — Complaint Directory",
        source_url=CANADIAN_CONSUMER_HANDBOOK,
        publisher="Government of Canada",
        description="Searchable database of consumer complaints by sector and province.",
        downloads_dir=downloads_dir, session_obj=session_obj,
        is_html=True,
    )
    if ref:
        out.append(ref)

    logger.info("[Open Canada] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 9. LEGAL TEXTS — Competition Act, Rules, Guidelines, Key Decisions
# ══════════════════════════════════════════════════════════════════════════════

LEGAL_TEXTS = [
    # Competition Act full text
    {
        "title": "Competition Act (R.S.C., 1985, c. C-34) — Current to 2025",
        "url": "https://laws-lois.justice.gc.ca/eng/acts/C-34/",
        "sub": "act",
        "publisher": "Department of Justice Canada",
        "desc": "Full text of the Competition Act. Includes all 2024 amendments (Bill C-59, C-56). Essential reference.",
        "year": "2025",
        "is_html": True,
    },
    {
        "title": "Competition Act — Unofficial Consolidated PDF",
        "url": "https://laws-lois.justice.gc.ca/PDF/C-34.pdf",
        "sub": "act",
        "publisher": "Department of Justice Canada",
        "desc": "PDF version of the Competition Act — printable consolidated version.",
        "year": "2025",
        "is_html": False,
    },
    # Competition Tribunal Rules
    {
        "title": "Competition Tribunal Rules (SOR/2008-141) — Current",
        "url": "https://laws-lois.justice.gc.ca/eng/regulations/SOR-2008-141/",
        "sub": "rules",
        "publisher": "Competition Tribunal",
        "desc": "Rules 1–136 governing all Tribunal proceedings. Rules 48–68 cover private access (s.103.1) leave applications.",
        "year": "2024",
        "is_html": True,
    },
    {
        "title": "Competition Tribunal Rules — PDF",
        "url": "https://laws-lois.justice.gc.ca/PDF/SOR-2008-141.pdf",
        "sub": "rules",
        "publisher": "Competition Tribunal",
        "desc": "PDF version of Competition Tribunal Rules.",
        "year": "2024",
        "is_html": False,
    },
    {
        "title": "Competition Tribunal Rules — ct-tc.gc.ca",
        "url": "https://www.ct-tc.gc.ca/eng/tribunal-rules.html",
        "sub": "rules",
        "publisher": "Competition Tribunal",
        "desc": "Official Tribunal rules page with filing guides.",
        "year": None,
        "is_html": True,
    },
    # Bureau Guidelines
    {
        "title": "Bureau Guidelines — Private Access to the Competition Tribunal",
        "url": "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/publications/private-access-competition-tribunal",
        "sub": "guidelines",
        "publisher": "Competition Bureau Canada",
        "desc": "Official Bureau position on s.103.1 private access applications. Key for understanding leave test.",
        "year": None,
        "is_html": True,
    },
    {
        "title": "Bureau Merger Enforcement Guidelines (2022)",
        "url": "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/publications/merger-enforcement-guidelines",
        "sub": "guidelines",
        "publisher": "Competition Bureau Canada",
        "desc": "HHI thresholds: < 1500 unproblematic; 1500-2500 potentially concerning; > 2500 likely problematic. Defines substantial lessening of competition.",
        "year": "2022",
        "is_html": True,
    },
    {
        "title": "Bureau Abuse of Dominance Guidelines (2019)",
        "url": "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/publications/abuse-dominance-enforcement-guidelines",
        "sub": "guidelines",
        "publisher": "Competition Bureau Canada",
        "desc": "Market share thresholds: 35%+ for dominance finding under s.78-79.",
        "year": "2019",
        "is_html": True,
    },
    {
        "title": "Bureau Deceptive Marketing Guidelines",
        "url": "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/publications/deceptive-marketing-practices-guidelines",
        "sub": "guidelines",
        "publisher": "Competition Bureau Canada",
        "desc": "Guidelines on ss.74.01-74.22 deceptive marketing practices.",
        "year": None,
        "is_html": True,
    },
    {
        "title": "Bureau Competitor Collaboration Guidelines (2023)",
        "url": "https://competition-bureau.canada.ca/en/how-we-foster-competition/education-and-outreach/publications/competitor-collaboration-guidelines",
        "sub": "guidelines",
        "publisher": "Competition Bureau Canada",
        "desc": "Guidelines on s.90.1 agreements and price-fixing conspiracy analysis.",
        "year": "2023",
        "is_html": True,
    },
    # Key Decisions — Martin 2026 and foundational cases
    {
        "title": "Martin v. Alphabet Inc., 2026 CACT 1 — Defining Leave Decision",
        "url": "https://www.canlii.org/en/ca/cact/doc/2026/2026cact1/2026cact1.html",
        "sub": "key-decision",
        "publisher": "Competition Tribunal",
        "desc": "THE defining case for private access leave under s.103.1. Sets the 'not plain and obvious' + affected person test. Martin 2026 leave test.",
        "year": "2026",
        "is_html": True,
    },
    {
        "title": "Commissioner of Competition v. Toronto Real Estate Board, 2017 FCA 236",
        "url": "https://www.canlii.org/en/ca/fca/doc/2017/2017fca236/2017fca236.html",
        "sub": "key-decision",
        "publisher": "Federal Court of Appeal",
        "desc": "Landmark refusal-to-deal / abuse of dominance. FCA upheld Tribunal order against TREB MLS restrictions.",
        "year": "2017",
        "is_html": True,
    },
    {
        "title": "Canada (Commissioner of Competition) v. CCS Corporation, 2012 CACT 6",
        "url": "https://www.canlii.org/en/ca/cact/doc/2012/2012cact6/2012cact6.html",
        "sub": "key-decision",
        "publisher": "Competition Tribunal",
        "desc": "Abuse of dominance in waste management. Defines market power analysis and anticompetitive acts standard.",
        "year": "2012",
        "is_html": True,
    },
    {
        "title": "Commissioner of Competition v. Vancouver Airport Authority, 2019 CACT 6",
        "url": "https://www.canlii.org/en/ca/cact/doc/2019/2019cact6/2019cact6.html",
        "sub": "key-decision",
        "publisher": "Competition Tribunal",
        "desc": "Airport ground handling services. Key case on exclusionary conduct and refusal to deal.",
        "year": "2019",
        "is_html": True,
    },
]


def _scrape_legal_texts(downloads_dir: str, session_obj: requests.Session) -> list[ReferenceDocument]:
    out = []
    for item in LEGAL_TEXTS:
        ref = _make_ref(
            category="legal",
            sub_category=item["sub"],
            title=item["title"],
            source_url=item["url"],
            publisher=item["publisher"],
            description=item["desc"],
            downloads_dir=downloads_dir,
            session_obj=session_obj,
            is_html=item.get("is_html", True),
            published_date=item.get("year"),
        )
        if ref:
            out.append(ref)
    logger.info("[Legal texts] Scraped %d documents", len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def scrape_reference_data(
    downloads_dir: str = "downloads",
    include: set[str] | None = None,
) -> list[ReferenceDocument]:
    """
    Scrape all reference data sources.

    Args:
        downloads_dir: Root directory for downloads.
        include: Optional set of category names to restrict scraping.
                 e.g. {"enforcement", "market-study", "legal"}
                 None = scrape all.

    Returns:
        List of ReferenceDocument objects (also saved to disk).
    """
    s = session()
    all_docs: list[ReferenceDocument] = []

    def _run(label: str, category: str, fn) -> None:
        if include and category not in include:
            return
        logger.info("[Reference] ── %s ──", label)
        try:
            docs = fn()
            all_docs.extend(docs)
            logger.info("[Reference] %s: %d items", label, len(docs))
        except Exception as exc:
            logger.error("[Reference] %s failed: %s", label, exc)

    _run("Bureau Enforcement RSS",          "enforcement",   lambda: _scrape_bureau_rss(downloads_dir, s))
    _run("Bureau Enforcement Pages",        "enforcement",   lambda: _scrape_bureau_enforcement_pages(downloads_dir, s))
    _run("Bureau Market Studies",           "market-study",  lambda: _scrape_market_studies(downloads_dir, s))
    _run("Statistics Canada",               "stats",         lambda: _scrape_statscan(downloads_dir, s))
    _run("CRTC Telecom Reports",            "stats",         lambda: _scrape_crtc(downloads_dir, s))
    _run("OSFI Banking Data",               "stats",         lambda: _scrape_osfi(downloads_dir, s))
    _run("Transport Canada Airlines",       "stats",         lambda: _scrape_transport_canada(downloads_dir, s))
    _run("Retail Council Grocery",          "stats",         lambda: _scrape_retail_council(downloads_dir, s))
    _run("Open Government Canada",          "consumer",      lambda: _scrape_open_canada(downloads_dir, s))
    _run("Legal Texts & Key Decisions",     "legal",         lambda: _scrape_legal_texts(downloads_dir, s))

    logger.info("[Reference] Complete — %d total reference documents", len(all_docs))
    return all_docs
