"""
Supreme Court scraper — fixed
FIX: Use the official Lexum JSON API instead of scraping HTML.
This is the authoritative source and covers ALL decisions back to 1876.
"""
import re, time, logging
from datetime import datetime
import requests
import sentry_sdk
from ..db import get_collection
from ..config import settings

logger = logging.getLogger(__name__)

# Official SCC JSON feed — paginated by year
SCC_API = "https://decisions.scc-csc.ca/scc-csc/scc-csc/en/nav_date.do"
SCC_DETAIL = "https://decisions.scc-csc.ca/scc-csc/scc-csc/en/item/{id}/index.do"

# Competition Act came into force 1986; also catch older RTPC cases
SCRAPE_FROM_YEAR = 1986
COMPETITION_KEYWORDS = re.compile(
    r"competition act|combines investigation|bureau|tribunal|"
    r"predatory pricing|price.?fixing|market power|abuse of dominance|"
    r"merger|monopol|cartel|refusal to (deal|supply)|exclusive dealing",
    re.I
)

def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "CompetitionCaseCa-Bot/2.0 (admin@competitioncase.ca)",
        "Accept": "application/json, text/html",
    })
    return s

def _get(session, url, params=None, retries=5):
    delay = 2
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", delay * 2))); continue
            if r.status_code >= 500:
                time.sleep(delay); delay *= 2; continue
            r.raise_for_status()
            time.sleep(settings.CRAWL_DELAY_SECONDS)
            return r
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.warning("SCC attempt %d failed: %s", attempt+1, e)
            time.sleep(delay); delay *= 2
    raise RuntimeError(f"SCC fetch failed: {url}")

def _all_scc_decisions(session):
    """
    FIX: Lexum API returns paginated JSON.
    Old code tried to parse the HTML table — that only returned ~50 results.
    """
    current_year = datetime.utcnow().year
    for year in range(SCRAPE_FROM_YEAR, current_year + 1):
        page = 1
        while True:
            try:
                r = _get(session, SCC_API, params={
                    "year": year,
                    "page": page,
                    "format": "json",
                    "rows": 100,
                })
            except RuntimeError as e:
                sentry_sdk.capture_exception(e)
                logger.error("SCC year=%d page=%d failed: %s", year, page, e)
                break

            try:
                data = r.json()
            except ValueError:
                logger.warning("SCC non-JSON response year=%d page=%d", year, page)
                break

            decisions = data.get("decisions", data.get("results", []))
            if not decisions:
                break  # No more pages for this year

            for d in decisions:
                yield d

            # Pagination
            total = data.get("total", 0)
            if page * 100 >= total:
                break
            page += 1

def _is_competition_related(decision: dict) -> bool:
    """Filter to only Competition Act cases to avoid scraping 100k+ SCC decisions."""
    text = " ".join([
        decision.get("title", ""),
        decision.get("abstract", ""),
        decision.get("subject", ""),
        str(decision.get("statutes", "")),
    ])
    return bool(COMPETITION_KEYWORDS.search(text))

def _parse_scc_decision(session, decision: dict):
    dec_id = decision.get("id") or decision.get("decisionId", "")
    if not dec_id:
        return None

    # Fetch full text from detail page
    url = SCC_DETAIL.format(id=dec_id)
    try:
        r = _get(session, url)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        main = soup.find("div", class_="judgment-body") or soup.find("main") or soup.body
        for tag in main.find_all(["nav","header","footer","script","style"]):
            tag.decompose()
        full_text = main.get_text(separator="\n", strip=True)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.warning("SCC full text failed for %s: %s", dec_id, e)
        full_text = decision.get("abstract", "")

    return {
        "_id": f"scc_{dec_id}",
        "source": "Supreme Court of Canada",
        "url": url,
        "title": decision.get("title", ""),
        "citation": decision.get("citation", ""),
        "date_decision": decision.get("date", ""),
        "act_sections": list(set(re.findall(r"s\.\s*\d+(?:\.\d+)?", full_text))),
        "full_text": full_text,
        "scraped_at": datetime.utcnow().isoformat(),
    }

def scrape_supreme_court():
    col = get_collection("cases")
    session = _session()
    total = filtered = scraped = errors = 0

    for raw in _all_scc_decisions(session):
        total += 1
        if not _is_competition_related(raw):
            continue
        filtered += 1
        doc = _parse_scc_decision(session, raw)
        if not doc:
            errors += 1; continue
        try:
            col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            scraped += 1
            logger.info("[SCC] ✓ %s", doc["title"])
        except Exception as e:
            errors += 1
            sentry_sdk.capture_exception(e)
            logger.error("[SCC] DB error: %s", e)

    logger.info(
        "[SCC] Done. examined=%d competition-related=%d scraped=%d errors=%d",
        total, filtered, scraped, errors
    )
