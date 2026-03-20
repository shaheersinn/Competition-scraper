# Competition Law Case Scraper — Fixed Edition

Scrapes the full text of all Canadian competition law decisions from:

| Source | What | Coverage |
|--------|------|----------|
| Competition Tribunal (Lexum) | All decisions + attached PDFs | 1986–present |
| Competition Tribunal summaries | HTML summaries + PDF downloads | 1986–present |
| Competition Bureau — RTP | Restrictive trade practices outcomes | ~2000–present |
| Competition Bureau — DMP | Deceptive marketing outcomes | ~2000–present |
| Federal Court (Lexum) | Competition-related decisions + PDFs | 1986–present |
| Federal Court of Appeal (Lexum) | Competition-related decisions + PDFs | 1986–present |
| Supreme Court of Canada (Lexum) | Competition-related decisions + PDFs | 1975–present |
| CanLII (optional) | Cross-indexed decisions (requires API key) | Varies |

---

## Quick start (local)

```bash
# 1. Clone
git clone https://github.com/shaheersinn/Competition-scraper.git
cd Competition-scraper

# 2. Install
pip install -r requirements.txt
python -m playwright install chromium --with-deps

# 3. Run (full corpus)
mkdir -p data downloads artifacts
python -m scraper.main \
  --start-year 1986 \
  --end-year 2026 \
  --db-path data/competition_law_cases.db \
  --downloads-dir downloads \
  --csv-path artifacts/cases.csv \
  --jsonl-path artifacts/cases.jsonl

# 4. Run a single source only (much faster for testing)
python -m scraper.main \
  --start-year 2020 --end-year 2026 \
  --db-path data/test.db \
  --downloads-dir downloads \
  --csv-path artifacts/cases.csv \
  --jsonl-path artifacts/cases.jsonl \
  --sources tribunal summaries
```

---

## GitHub Actions

Trigger a manual run from the **Actions** tab → **Scrape Canadian Competition Law Cases** → **Run workflow**.

Options:
- **start_year / end_year** — restrict the date range (useful for incremental updates)
- **sources** — comma-separated list e.g. `tribunal,summaries` to run only specific sources
- **enable_canlii** — set `true` to also scrape CanLII (slower, requires API key for best results)

### Required secrets

Add these in **Settings → Secrets → Actions**:

| Secret | Required | Description |
|--------|----------|-------------|
| `SENTRY_DSN` | Optional | Sentry DSN for error monitoring (GitHub Education) |
| `CANLII_API_KEY` | Optional | [Free CanLII API key](https://api.canlii.org/) for faster CanLII scraping |

---

## Output files

After a run, download the `competition-law-data` artifact:

```
data/
  competition_law_cases.db   ← SQLite database (main deliverable)
artifacts/
  cases.csv / cases.jsonl    ← One row per case
  documents.csv / .jsonl     ← One row per document (PDF etc.)
  parties.csv / .jsonl       ← Applicants and respondents
  sources.csv                ← Source registry
downloads/
  competition_tribunal/      ← Downloaded PDFs organised by year/case
  federal_court/
  supreme_court/
  ...
```

### Database schema

```sql
cases (
  id, source, source_case_id, case_number, title,
  year, date_filed, date_decided,
  court_or_tribunal, case_type, neutral_citation,
  status, summary,       -- short excerpt
  full_text,             -- COMPLETE decision text
  case_url, scraped_at, raw_json
)

documents (
  id, case_id, source, document_title, document_type,
  document_url, local_path, sha256, file_size, mime_type,
  extracted_text,        -- text extracted from downloaded PDF
  scraped_at, raw_json
)

parties (id, case_id, party_name, party_role)

cases_fts (title, full_text, summary)  -- full-text search index
```

### Querying the database

```python
import sqlite3
conn = sqlite3.connect("data/competition_law_cases.db")

# Full-text search across all decisions
results = conn.execute("""
    SELECT c.title, c.year, c.case_url, snippet(cases_fts, 1, '[', ']', '...', 20) AS excerpt
    FROM cases_fts
    JOIN cases c ON c.id = cases_fts.rowid
    WHERE cases_fts MATCH 'abuse of dominance'
    ORDER BY rank
    LIMIT 20
""").fetchall()

# All decisions citing s.79 (abuse of dominance)
conn.execute("""
    SELECT title, year, court_or_tribunal, case_url
    FROM cases
    WHERE full_text LIKE '%s.79%' OR full_text LIKE '%section 79%'
    ORDER BY year DESC
""")
```

---

## Deployment on Digital Ocean

### Recommended architecture (GitHub Education credits)

```
GitHub Actions (free)        ← runs the scraper on a schedule
        ↓ exports SQLite DB as artifact
DigitalOcean App Platform    ← hosts your web app ($6/mo Basic)
        ↓ reads from
MongoDB Atlas M0 (free)      ← stores cases for your app
        ↓ monitored by
Sentry (free via GH Ed.)     ← catches scraper errors
```

### Syncing scraped data to MongoDB

After a GitHub Actions run, download the artifact and run:

```bash
python scripts/sync_to_mongo.py \
  --db-path data/competition_law_cases.db \
  --mongo-uri "$MONGO_URI" \
  --mongo-db competition_cases
```

(See `scripts/sync_to_mongo.py` — coming soon)

---

## Bug fixes in this version

| # | Bug | Old behaviour | Fixed |
|---|-----|--------------|-------|
| 1 | `summary=None` hardcoded | Zero decision text stored | Full text extracted and stored |
| 2 | `networkidle` Playwright timeout silently swallowed | 58s runtime, 0 cases found | `domcontentloaded` + selector wait |
| 3 | No retry in `safe_get()` | One glitch = silent data loss | Exponential backoff, 4 retries |
| 4 | CanLII used `#hash` URLs with `requests` | 0 CanLII results | Playwright renders JS pages |
| 5 | PDF token matching too narrow | Empty downloads folder | `is_document_url()` handles all formats |
| 6 | `full_text` column missing from DB | Couldn't store text even if extracted | Added to schema + migration |
| 7 | Bureau scraper only parsed 180-char line | No decision content | Fetches full detail pages |
| 8 | No Federal Court scraper | FC decisions missing | New `federal_court.py` |
| 9 | No Supreme Court scraper | SCC decisions missing | New `supreme_court.py` |
| 10 | PDF text never extracted | Downloaded but unusable | `pdfminer.six` integration |
