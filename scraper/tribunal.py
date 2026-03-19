from __future__ import annotations

import asyncio
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .models import CaseRecord, DocumentRecord, PartyRecord
from .utils import abs_url, download_file, filename_from_url, session, slugify

TRIBUNAL_BASE = "https://decisions.ct-tc.gc.ca"
SUMMARIES_URL = "https://www.ct-tc.gc.ca/en/cases/decision-summaries.html"


def _item_id(url: str) -> str:
    m = re.search(r"/item/(\d+)/", url)
    if m:
        return m.group(1)
    return slugify(url)


async def _discover_tribunal_case_urls(start_year: int, end_year: int) -> list[str]:
    urls: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        for year in range(end_year, start_year - 1, -1):
            url = f"{TRIBUNAL_BASE}/ct-tc/cdo/en/nav_date.do?year={year}"
            try:
                await page.goto(url, wait_until="networkidle", timeout=90000)
            except Exception:
                continue
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            for a in soup.select('a[href*="/item/"]'):
                href = a.get("href")
                full = abs_url(url, href)
                if full:
                    urls.add(full)
        await browser.close()
    return sorted(urls)


def _parse_parties(title: str) -> list[PartyRecord]:
    parties = []
    if " v. " in title:
        left, right = title.split(" v. ", 1)
        parties.append(PartyRecord(source_case_id="", party_name=left.strip(), party_role="applicant/appellant"))
        parties.append(PartyRecord(source_case_id="", party_name=right.strip(), party_role="respondent"))
    return parties


def _parse_case_page(url: str, downloads_dir: str):
    s = session()
    resp = s.get(url, timeout=120)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    title = soup.title.get_text(" ", strip=True).replace(" - Competition Tribunal", "").strip() if soup.title else url
    text = soup.get_text("\n", strip=True)
    year_match = re.search(r"\b(19|20)\d{2}\b", text)
    year = int(year_match.group(0)) if year_match else None
    rec = CaseRecord(
        source="competition_tribunal",
        source_case_id=_item_id(url),
        title=title,
        year=year,
        court_or_tribunal="Competition Tribunal",
        case_type="tribunal case document",
        case_url=url,
        summary=None,
        raw={"source_url": url},
    )
    docs = []
    case_folder = Path(downloads_dir) / "competition_tribunal" / str(year or "unknown") / rec.source_case_id
    for a in soup.select("a[href]"):
        href = a.get("href")
        full = abs_url(url, href)
        label = a.get_text(" ", strip=True) or "Document"
        if not full:
            continue
        if any(token in full.lower() for token in [".pdf", "/download", "download=1"]):
            out_path = case_folder / filename_from_url(full)
            try:
                meta = download_file(s, full, out_path)
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
                        raw={"anchor_text": label},
                    )
                )
            except Exception:
                docs.append(
                    DocumentRecord(
                        source="competition_tribunal",
                        source_case_id=rec.source_case_id,
                        document_title=label,
                        document_url=full,
                        raw={"anchor_text": label, "download_error": True},
                    )
                )
    parties = _parse_parties(title)
    for p in parties:
        p.source_case_id = rec.source_case_id
    return rec, docs, parties


def scrape_tribunal(start_year: int, end_year: int, downloads_dir: str):
    urls = asyncio.run(_discover_tribunal_case_urls(start_year, end_year))
    out = []
    for url in urls:
        try:
            out.append(_parse_case_page(url, downloads_dir))
        except Exception:
            continue
    return out
