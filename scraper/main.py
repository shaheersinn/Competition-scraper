"""
Competition Law Case Scraper — main entry point

Sources scraped:
  1. Competition Tribunal (decisions.ct-tc.gc.ca) — all decisions
  2. Competition Tribunal decision summaries (ct-tc.gc.ca)
  3. Competition Bureau — restrictive trade practices cases
  4. Competition Bureau — deceptive marketing practices cases
  5. Federal Court (decisions.fca-caf.gc.ca) — competition-related only
  6. Federal Court of Appeal — competition-related only
  7. Supreme Court of Canada (decisions.scc-csc.ca) — competition-related only
  8. CanLII (optional, requires --enable-canlii true or CANLII_API_KEY)

All runs are idempotent — safe to re-run without duplicating data.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from .bureau import scrape_bureau_sources
from .canlii_optional import scrape_canlii_optional
from .db import Database
from .federal_court import scrape_federal_court
from .summaries import scrape_decision_summaries
from .supreme_court import scrape_supreme_court
from .tribunal import scrape_tribunal
from .utils import write_jsonl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(
        description="Scrape Canadian competition law case law from all major sources."
    )
    p.add_argument("--start-year", type=int, default=1986,
                   help="Earliest year to scrape (default: 1986)")
    p.add_argument("--end-year", type=int, default=2026,
                   help="Latest year to scrape (default: current year)")
    p.add_argument("--db-path", required=True,
                   help="Path to SQLite database file")
    p.add_argument("--downloads-dir", required=True,
                   help="Directory for downloaded PDFs and documents")
    p.add_argument("--csv-path", required=True,
                   help="Output path for cases CSV export")
    p.add_argument("--jsonl-path", required=True,
                   help="Output path for cases JSONL export")
    p.add_argument("--enable-canlii", default="false",
                   help="Enable CanLII scraping (true/false, default: false)")
    p.add_argument("--sources", nargs="+",
                   choices=[
                       "tribunal", "summaries", "bureau",
                       "federal_court", "supreme_court", "canlii"
                   ],
                   default=None,
                   help="Run only specific sources (default: all except canlii)")
    return p.parse_args()


def export(db: Database, cases_csv: str, cases_jsonl: str):
    artifacts_dir = Path(cases_csv).parent
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Exporting to CSV and JSONL…")
    cases = db.export_table("cases")
    docs = db.export_table("documents")
    parties = db.export_table("parties")
    sources = db.export_table("sources")

    pd.DataFrame(cases).to_csv(cases_csv, index=False)
    write_jsonl(cases, cases_jsonl)
    pd.DataFrame(docs).to_csv(artifacts_dir / "documents.csv", index=False)
    write_jsonl(docs, artifacts_dir / "documents.jsonl")
    pd.DataFrame(parties).to_csv(artifacts_dir / "parties.csv", index=False)
    write_jsonl(parties, artifacts_dir / "parties.jsonl")
    pd.DataFrame(sources).to_csv(artifacts_dir / "sources.csv", index=False)

    logger.info(
        "Export complete — %d cases, %d documents, %d parties",
        len(cases), len(docs), len(parties),
    )


def ingest(db: Database, entries: list):
    """Write a list of (CaseRecord, [DocumentRecord], [PartyRecord]) into the DB."""
    for entry in entries:
        if not entry:
            continue
        if len(entry) == 2:
            case, docs = entry
            parties = []
        else:
            case, docs, parties = entry
        case_id = db.upsert_case(case)
        for party in parties:
            db.add_parties(case_id, [party])
        for doc in docs:
            db.upsert_document(case_id, doc)


def main():
    args = parse_args()
    run_all = args.sources is None
    sources = set(args.sources or [])

    db = Database(args.db_path)
    downloads_dir = args.downloads_dir
    Path(downloads_dir).mkdir(parents=True, exist_ok=True)

    total_cases = 0

    # 1. Competition Tribunal — full decisions (Lexum)
    if run_all or "tribunal" in sources:
        logger.info("=" * 60)
        logger.info("SOURCE 1/7: Competition Tribunal decisions")
        logger.info("=" * 60)
        entries = scrape_tribunal(args.start_year, args.end_year, downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("Tribunal: ingested %d records", len(entries))

    # 2. Competition Tribunal — decision summaries page
    if run_all or "summaries" in sources:
        logger.info("=" * 60)
        logger.info("SOURCE 2/7: Competition Tribunal decision summaries")
        logger.info("=" * 60)
        entries = scrape_decision_summaries(args.start_year, args.end_year, downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("Summaries: ingested %d records", len(entries))

    # 3 & 4. Competition Bureau enforcement outcomes
    if run_all or "bureau" in sources:
        logger.info("=" * 60)
        logger.info("SOURCE 3-4/7: Competition Bureau enforcement outcomes")
        logger.info("=" * 60)
        entries = scrape_bureau_sources(downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("Bureau: ingested %d records", len(entries))

    # 5. Federal Court (+ Court of Appeal)
    if run_all or "federal_court" in sources:
        logger.info("=" * 60)
        logger.info("SOURCE 5/7: Federal Court & Federal Court of Appeal")
        logger.info("=" * 60)
        entries = scrape_federal_court(args.start_year, args.end_year, downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("Federal Court: ingested %d records", len(entries))

    # 6. Supreme Court of Canada
    if run_all or "supreme_court" in sources:
        logger.info("=" * 60)
        logger.info("SOURCE 6/7: Supreme Court of Canada")
        logger.info("=" * 60)
        entries = scrape_supreme_court(args.start_year, args.end_year, downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("SCC: ingested %d records", len(entries))

    # 7. CanLII (optional)
    enable_canlii = (
        str(args.enable_canlii).lower() == "true"
        or "canlii" in sources
    )
    if enable_canlii:
        logger.info("=" * 60)
        logger.info("SOURCE 7/7: CanLII (optional)")
        logger.info("=" * 60)
        entries = scrape_canlii_optional(downloads_dir)
        ingest(db, entries)
        total_cases += len(entries)
        logger.info("CanLII: ingested %d records", len(entries))

    logger.info("=" * 60)
    logger.info("ALL SOURCES DONE — total records ingested: %d", total_cases)
    logger.info("=" * 60)

    export(db, args.csv_path, args.jsonl_path)
    db.close()
    logger.info("Scraper finished successfully.")


if __name__ == "__main__":
    main()
