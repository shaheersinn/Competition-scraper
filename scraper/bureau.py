from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .models import CaseRecord, DocumentRecord
from .utils import safe_get, session

RTP_URL = (
    "https://competition-bureau.canada.ca/restrictive-trade-practices/"
    "cases-and-outcomes/restrictive-trade-practices-cases-and-outcomes?wbdisable=true"
)
DMP_URL = (
    "https://competition-bureau.canada.ca/en/deceptive-marketing-practices/"
    "cases-and-outcomes?wbdisable=true"
)


def _parse_table(url: str, source_name: str):
    s = session()
    html = safe_get(s, url).text
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    current_year = None
    records: list[tuple[CaseRecord, list[DocumentRecord]]] = []

    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if re.fullmatch(r"20\d{2}", line):
            current_year = int(line)
            continue
        if not re.match(r"20\d{2}-\d{2}-\d{2}", line):
            continue
        parts = line.split(" ")
        date_public = parts[0]
        summary = line[len(date_public):].strip()
        title = summary[:180]
        source_case_id = f"{source_name}:{date_public}:{hash(line)}"
        rec = CaseRecord(
            source=source_name,
            source_case_id=source_case_id,
            title=title,
            year=current_year,
            date_filed=date_public,
            date_decided=date_public,
            court_or_tribunal="Competition Bureau Canada",
            case_type="public enforcement outcome",
            summary=summary,
            case_url=url,
            raw={"line": line, "source_url": url},
        )
        records.append((rec, []))
    return records


def scrape_bureau_sources():
    out = []
    out.extend(_parse_table(RTP_URL, "competition_bureau_rtp"))
    out.extend(_parse_table(DMP_URL, "competition_bureau_dmp"))
    return out
