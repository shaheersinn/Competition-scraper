# Canadian Competition Law Case Scraper

GitHub-ready scraper package that builds a SQLite database of Canadian competition-law-related cases and downloads public PDFs where available.

## Sources

Enabled by default:
- Competition Tribunal case documents and decision pages
- Competition Tribunal decision summaries
- Competition Bureau public "cases and outcomes" pages for restrictive trade practices and deceptive marketing

Optional:
- CanLII discovery for competition-related Canadian judgments and tribunal decisions (`--enable-canlii true`)

## Outputs

- `data/competition_law_cases.db` — main SQLite database
- `artifacts/cases.csv` — case export
- `artifacts/cases.jsonl` — case export
- `artifacts/documents.csv` — document export
- `artifacts/documents.jsonl` — document export
- `downloads/` — downloaded PDFs and other public files

## Database schema

### `cases`
- `id`
- `source`
- `source_case_id`
- `case_number`
- `title`
- `year`
- `date_filed`
- `date_decided`
- `court_or_tribunal`
- `case_type`
- `neutral_citation`
- `language`
- `status`
- `summary`
- `case_url`
- `scraped_at`
- `raw_json`

### `documents`
- `id`
- `case_id`
- `source`
- `document_title`
- `document_type`
- `document_date`
- `document_url`
- `local_path`
- `sha256`
- `file_size`
- `mime_type`
- `scraped_at`
- `raw_json`

### `parties`
- `id`
- `case_id`
- `party_name`
- `party_role`

### `sources`
- `id`
- `name`
- `base_url`
- `notes`

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium

python -m scraper.main \
  --start-year 1986 \
  --end-year 2026 \
  --db-path data/competition_law_cases.db \
  --downloads-dir downloads \
  --csv-path artifacts/cases.csv \
  --jsonl-path artifacts/cases.jsonl \
  --enable-canlii false
```

## GitHub Actions

The included workflow runs manually or every Monday at 06:00 UTC.

1. Create a new GitHub repo.
2. Upload this package.
3. Go to **Actions**.
4. Run **Scrape Canadian Competition Law Cases**.
5. Download the `competition-law-data` and `competition-law-pdfs` artifacts.

## Notes

- This scraper uses polite rate limiting and stores source metadata.
- Some public sites are heavily dynamic or may apply bot controls. The Competition Tribunal source therefore uses Playwright for discovery.
- The optional CanLII adapter is included because it is useful for competition-law-related appellate and court decisions, but you should confirm your intended use aligns with the target site's terms and robots rules before large-scale runs.
- I did not live-verify a full crawl inside this environment.
