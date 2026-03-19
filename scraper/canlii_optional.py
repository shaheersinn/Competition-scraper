from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .models import CaseRecord
from .utils import abs_url, safe_get, session

SEARCHES = [
    "https://www.canlii.org/en/#search/type=decision&text=%22Competition%20Act%22",
    "https://www.canlii.org/en/#search/type=decision&text=%22Competition%20Tribunal%22",
]


def scrape_canlii_optional(downloads_dir: str):
    _ = downloads_dir
    s = session()
    results = []
    for search_url in SEARCHES:
        try:
            html = safe_get(s, search_url).text
        except Exception:
            continue
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            title = a.get_text(" ", strip=True)
            if not href.startswith("/t/") or not title:
                continue
            case_url = abs_url("https://www.canlii.org", href)
            source_case_id = href.strip("/")
            text = title
            year_match = re.search(r"(19|20)\d{2}", text)
            year = int(year_match.group(0)) if year_match else None
            rec = CaseRecord(
                source="canlii_optional",
                source_case_id=source_case_id,
                title=title,
                year=year,
                court_or_tribunal="CanLII indexed source",
                case_type="competition-law-related decision",
                case_url=case_url,
                raw={"search_url": search_url},
            )
            results.append((rec, []))
    unique = {}
    for rec, docs in results:
        unique[(rec.source, rec.source_case_id)] = (rec, docs)
    return list(unique.values())
