"""
Competition Law Case Scraper — v4
No CanLII API key required.

Source priority (all run by default):
  1.  Competition Tribunal summaries page (always works, 14 summaries)
  2.  Competition Bureau enforcement outcomes
  3.  Lexum CDO — CT, FC, FCA, SCC direct (4 courts, one scraper)
  4.  CanLII website (no API) — CT + FC + FCA + SCC + 4 appellate courts
  5.  Reference data — Bureau releases, market studies, StatsCan, legal texts...

Layers 3 and 4 are complementary: Lexum gives raw court HTML,
CanLII gives cross-indexed, cleaned, well-linked versions.
Both run by default; idempotent upserts avoid duplicates.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from .bureau import scrape_bureau_sources
from .canlii_web import scrape_canlii_web
from .db import Database
from .lexum import scrape_lexum_courts
from .reference_data import scrape_reference_data
from .summaries import scrape_decision_summaries
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
        description=(
            "Scrape all Canadian competition law case law + reference data.\n"
            "No CanLII API key required."
        )
    )
    p.add_argument("--start-year",     type=int, default=1986)
    p.add_argument("--end-year",       type=int, default=2026)
    p.add_argument("--db-path",        required=True)
    p.add_argument("--downloads-dir",  required=True)
    p.add_argument("--csv-path",       required=True)
    p.add_argument("--jsonl-path",     required=True)
    p.add_argument("--sources", nargs="+",
                   choices=[
                       "summaries", "bureau",
                       "lexum",          # CT + FC + FCA + SCC via Lexum CDO directly
                       "canlii",         # CanLII website (no API)
                       "ref:enforcement", "ref:market-study",
                       "ref:stats", "ref:legal", "ref:consumer", "ref:all",
                   ],
                   default=None,
                   help="Specific sources only (blank = all)")
    # Keep --enable-canlii for backwards compat but it's now always on
    p.add_argument("--enable-canlii",  default="true")
    return p.parse_args()


def export(db: Database, cases_csv: str, cases_jsonl: str):
    artifacts_dir = Path(cases_csv).parent
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    for table, stem in [
        ("cases",               "cases"),
        ("documents",           "documents"),
        ("parties",             "parties"),
        ("sources",             "sources"),
        ("reference_documents", "reference_documents"),
    ]:
        rows = db.export_table(table)
        pd.DataFrame(rows).to_csv(artifacts_dir / f"{stem}.csv", index=False)
        write_jsonl(rows, artifacts_dir / f"{stem}.jsonl")
    cases = db.export_table("cases")
    pd.DataFrame(cases).to_csv(cases_csv, index=False)
    write_jsonl(cases, cases_jsonl)
    logger.info("Export done — %d cases, %d reference docs",
                len(cases), len(db.export_table("reference_documents")))


def ingest_cases(db, entries):
    for entry in entries:
        if not entry:
            continue
        if len(entry) == 2:
            case, docs = entry; parties = []
        else:
            case, docs, parties = entry
        cid = db.upsert_case(case)
        for party in (parties or []):
            db.add_parties(cid, [party])
        for doc in (docs or []):
            db.upsert_document(cid, doc)


def ingest_reference(db, refs):
    for ref in refs:
        if ref:
            db.upsert_reference(ref)


def main():
    args = parse_args()
    run_all = args.sources is None
    sources = set(args.sources or [])

    run_ref_all = run_all or "ref:all" in sources
    ref_categories = None
    if not run_ref_all and any(s.startswith("ref:") for s in sources):
        ref_categories = {s[4:] for s in sources if s.startswith("ref:")}

    db = Database(args.db_path)
    dl = args.downloads_dir
    Path(dl).mkdir(parents=True, exist_ok=True)

    def run(label, key, fn):
        if not (run_all or key in sources):
            return
        logger.info("=" * 60)
        logger.info("SOURCE: %s", label)
        logger.info("=" * 60)
        entries = fn()
        ingest_cases(db, entries)
        logger.info("%s: ingested %d records", label, len(entries))

    # ── Case law ─────────────────────────────────────────────────────────────
    run("Tribunal Decision Summaries",  "summaries",
        lambda: scrape_decision_summaries(args.start_year, args.end_year, dl))

    run("Competition Bureau outcomes",  "bureau",
        lambda: scrape_bureau_sources(dl))

    run("Lexum CDO (CT + FC + FCA + SCC)", "lexum",
        lambda: scrape_lexum_courts(args.start_year, args.end_year, dl))

    run("CanLII Website (no API)",      "canlii",
        lambda: scrape_canlii_web(args.start_year, args.end_year, dl))

    # ── Reference data ───────────────────────────────────────────────────────
    if run_ref_all or run_all or ref_categories:
        logger.info("=" * 60)
        logger.info("REFERENCE DATA")
        logger.info("=" * 60)
        refs = scrape_reference_data(downloads_dir=dl, include=ref_categories)
        ingest_reference(db, refs)
        logger.info("Reference data: ingested %d documents", len(refs))

    export(db, args.csv_path, args.jsonl_path)
    db.close()

    # Print final summary
    import sqlite3
    conn = sqlite3.connect(args.db_path)
    cases_n = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
    ref_n   = conn.execute("SELECT COUNT(*) FROM reference_documents").fetchone()[0]
    logger.info("=" * 60)
    logger.info("COMPLETE — %d cases | %d reference docs", cases_n, ref_n)
    logger.info("Cases by source:")
    for r in conn.execute("SELECT source, COUNT(*) n FROM cases GROUP BY source ORDER BY n DESC"):
        logger.info("  %-45s %d", r[0], r[1])
    logger.info("Reference by category:")
    for r in conn.execute("SELECT category, sub_category, COUNT(*) n FROM reference_documents GROUP BY category, sub_category ORDER BY category, n DESC"):
        logger.info("  %-20s %-30s %d", r[0], r[1] or "-", r[2])
    conn.close()


if __name__ == "__main__":
    main()
