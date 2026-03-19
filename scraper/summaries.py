from __future__ import annotations

import re
from bs4 import BeautifulSoup

from .models import CaseRecord, DocumentRecord
from .utils import abs_url, safe_get, session

URL = "https://www.ct-tc.gc.ca/en/cases/decision-summaries.html"


def scrape_decision_summaries(start_year: int, end_year: int):
    s = session()
    html = safe_get(s, URL).text
    soup = BeautifulSoup(html, "lxml")
    results = []
    for a in soup.select('a[href*="decision-summaries/"]'):
        href = a.get("href")
        full = abs_url(URL, href)
        title = a.get_text(" ", strip=True)
        if not title or not full:
            continue
        page = safe_get(s, full).text
        page_soup = BeautifulSoup(page, "lxml")
        text = page_soup.get_text("\n", strip=True)
        case_no = None
        m = re.search(r"Case #:\s*(CT-[0-9-]+)", text)
        if m:
            case_no = m.group(1)
        date_match = re.search(r"Date rendered:\s*(\d{4}-\d{2}-\d{2})", text)
        year = int(date_match.group(1)[:4]) if date_match else None
        if year is not None and not (start_year <= year <= end_year):
            continue
        pdf_link = None
        for link in page_soup.select("a[href]"):
            full_link = abs_url(full, link.get("href"))
            if full_link and full_link.lower().endswith(".pdf"):
                pdf_link = full_link
                break
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
            summary=text[:4000],
            raw={"summary_url": full},
        )
        docs = []
        if pdf_link:
            docs.append(
                DocumentRecord(
                    source="competition_tribunal_summaries",
                    source_case_id=rec.source_case_id,
                    document_title=f"{title} summary PDF",
                    document_url=pdf_link,
                    document_type="pdf/summary",
                    raw={},
                )
            )
        results.append((rec, docs, []))
    return results
