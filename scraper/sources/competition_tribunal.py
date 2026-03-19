"""
Competition Tribunal scraper — fixed
Fixes: full pagination, correct text selector, retry logic, Sentry, MongoDB upsert
"""
import re, time, logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import sentry_sdk
from ..db import get_collection
from ..config import settings

logger = logging.getLogger(__name__)
BASE = "https://www.ct-tc.gc.ca"
LIST_URL = f"{BASE}/en/cases.html"

def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "CompetitionCaseCa-Bot/2.0 (legal research; admin@competitioncase.ca)",
    })
    return s

def _get(session, url, retries=5):
    """GET with exponential backoff for 429/5xx/timeouts."""
    delay = 2
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", delay * 2))
                logger.warning("Rate-limited — sleeping %ds", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                logger.warning("Server error %d on %s (attempt %d)", r.status_code, url, attempt+1)
                time.sleep(delay); delay *= 2; continue
            r.raise_for_status()
            time.sleep(settings.CRAWL_DELAY_SECONDS)
            return r
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.warning("Network error on %s attempt %d: %s", url, attempt+1, e)
            time.sleep(delay); delay *= 2
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries")

def _all_case_links(session):
    """
    FIX: old code only read page 1.
    WET framework paginates via ?start=0, ?start=10, ?start=20 …
    We walk pages until no 'next' link found.
    """
    url = LIST_URL
    visited = set()
    while url:
        if url in visited:
            break
        visited.add(url)
        logger.info("Case list page: %s", url)
        soup = BeautifulSoup(_get(session, url).text, "lxml")
        for row in soup.select("table.table tbody tr"):
            a = row.find("a", href=True)
            if a:
                href = a["href"]
                yield href if href.startswith("http") else BASE + href
        # WET 'next' button
        nxt = (soup.find("a", rel=lambda r: r and "next" in r)
               or soup.find("a", string=re.compile(r"Next|>>", re.I)))
        if nxt and nxt.get("href"):
            h = nxt["href"]
            url = h if h.startswith("http") else BASE + h
        else:
            url = None

def _parse_case(session, url):
    try:
        resp = _get(session, url)
    except RuntimeError as e:
        sentry_sdk.capture_exception(e)
        logger.error("Skipping %s: %s", url, e)
        return None
    soup = BeautifulSoup(resp.text, "lxml")

    def _meta(label):
        for dt in soup.find_all(["dt", "th"]):
            if label.lower() in dt.get_text(strip=True).lower():
                sib = dt.find_next_sibling(["dd", "td"])
                if sib:
                    return sib.get_text(separator=" ", strip=True)
        return ""

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # FIX: extract from #wb-cont, strip chrome, get ALL text
    main = (soup.find("div", id="wb-cont")
            or soup.find("main")
            or soup.body)
    for tag in main.find_all(["nav","header","footer","script","style"]):
        tag.decompose()
    full_text = main.get_text(separator="\n", strip=True)

    docs = [
        {"label": a.get_text(strip=True),
         "url": a["href"] if a["href"].startswith("http") else BASE + a["href"]}
        for a in soup.find_all("a", href=True)
        if a["href"].lower().endswith((".pdf",".doc",".docx"))
    ]

    slug = url.rstrip("/").split("/")[-1]
    return {
        "_id": f"ct_{slug}",
        "source": "Competition Tribunal",
        "url": url,
        "title": title,
        "case_number": _meta("case number") or _meta("dossier"),
        "date_decision": _meta("date"),
        "respondent": _meta("respondent"),
        "applicant": _meta("applicant") or _meta("complainant"),
        "act_sections": list(set(re.findall(r"s\.\s*\d+(?:\.\d+)?", full_text))),
        "full_text": full_text,
        "documents": docs,
        "scraped_at": datetime.utcnow().isoformat(),
    }

def scrape_competition_tribunal():
    col = get_collection("cases")
    session = _session()
    total = scraped = errors = 0
    for url in _all_case_links(session):
        total += 1
        try:
            doc = _parse_case(session, url)
            if not doc:
                errors += 1; continue
            col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            scraped += 1
            logger.info("[CT] ✓ %s", doc["title"] or doc["_id"])
        except Exception as e:
            errors += 1
            sentry_sdk.capture_exception(e)
            logger.error("[CT] ✗ %s — %s", url, e)
    logger.info("[CT] Done. total=%d scraped=%d errors=%d", total, scraped, errors)
