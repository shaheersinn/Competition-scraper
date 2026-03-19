"""
Federal Court scraper — fixed
FIX: FC site renders pagination with JS. We use Playwright for list pages,
then requests for the static detail pages (much faster hybrid approach).
"""
import re, time, logging
from datetime import datetime
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import sentry_sdk
from ..db import get_collection
from ..config import settings

logger = logging.getLogger(__name__)
BASE = "https://www.fct-cf.gc.ca"
# The FC decision search — filter by Competition Act
SEARCH_URL = (
    "https://www.fct-cf.gc.ca/en/pages/law-and-practice"
    "/decisions#results"
)

def _all_case_links_playwright():
    """
    FIX: pure requests missed JS-rendered results.
    Playwright scrolls through all results and harvests links.
    """
    links = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "CompetitionCaseCa-Bot/2.0 (admin@competitioncase.ca)"
        })
        page.goto(SEARCH_URL, timeout=60000)
        page.wait_for_selector("a.decision-link, a[href*='/decisions/']", timeout=15000)

        while True:
            # Harvest links on current page
            hrefs = page.eval_on_selector_all(
                "a[href*='/decisions/']",
                "els => els.map(e => e.href)"
            )
            links.extend(hrefs)
            # Try to click "Next"
            try:
                nxt = page.locator("a:has-text('Next'), a[rel='next']").first
                if nxt.is_visible():
                    nxt.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(1)
                else:
                    break
            except Exception:
                break
        browser.close()
    # Deduplicate
    return list(dict.fromkeys(links))

def _parse_fc_case(session, url):
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        time.sleep(settings.CRAWL_DELAY_SECONDS)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.error("FC fetch failed %s: %s", url, e)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    main = soup.find("div", class_="decision-content") or soup.find("main") or soup.body
    for tag in main.find_all(["nav","header","footer","script","style"]):
        tag.decompose()

    h1 = soup.find("h1")
    full_text = main.get_text(separator="\n", strip=True)
    slug = re.sub(r"[^a-z0-9]+", "_", url.rstrip("/").split("/")[-1].lower())

    return {
        "_id": f"fc_{slug}",
        "source": "Federal Court",
        "url": url,
        "title": h1.get_text(strip=True) if h1 else "",
        "act_sections": list(set(re.findall(r"s\.\s*\d+(?:\.\d+)?", full_text))),
        "full_text": full_text,
        "scraped_at": datetime.utcnow().isoformat(),
    }

def scrape_federal_court():
    col = get_collection("cases")
    session = requests.Session()
    session.headers["User-Agent"] = "CompetitionCaseCa-Bot/2.0"
    total = scraped = errors = 0

    logger.info("[FC] Collecting links via Playwright…")
    try:
        links = _all_case_links_playwright()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.error("[FC] Playwright failed: %s", e)
        return

    logger.info("[FC] Found %d case links", len(links))
    for url in links:
        total += 1
        doc = _parse_fc_case(session, url)
        if not doc:
            errors += 1; continue
        try:
            col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            scraped += 1
            logger.info("[FC] ✓ %s", doc["title"] or doc["_id"])
        except Exception as e:
            errors += 1
            sentry_sdk.capture_exception(e)
            logger.error("[FC] DB error %s: %s", url, e)

    logger.info("[FC] Done. total=%d scraped=%d errors=%d", total, scraped, errors)
