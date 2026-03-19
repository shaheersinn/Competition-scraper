"""Run all scrapers. Safe to re-run — all upserts are idempotent."""
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log"),
    ]
)
from scraper.db import create_indexes
from scraper.sources.competition_tribunal import scrape_competition_tribunal
from scraper.sources.federal_court import scrape_federal_court
from scraper.sources.supreme_court import scrape_supreme_court

if __name__ == "__main__":
    create_indexes()
    scrape_competition_tribunal()
    scrape_federal_court()
    scrape_supreme_court()
```

---

## 🗄️ MongoDB + Digital Ocean recommendation

For your scale (competition law cases, one jurisdiction), the best setup is:

**MongoDB Atlas M0 (free)** → upgrade to M10 ($57/mo) only when you need Atlas Search. Pair with a **DO App Platform Basic** droplet ($6/mo) for the scraper cron job and your web app. This is simpler and cheaper than self-hosting MongoDB on a droplet.
```
DO App Platform (web + scraper cron)
        ↕
MongoDB Atlas (free tier → M10 when ready)
        ↕
Sentry (error monitoring, free via GitHub Education)
