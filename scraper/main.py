from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .bureau import scrape_bureau_sources
from .canlii_optional import scrape_canlii_optional
from .db import Database
from .summaries import scrape_decision_summaries
from .tribunal import scrape_tribunal
from .utils import write_jsonl


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start-year", type=int, default=1986)
    p.add_argument("--end-year", type=int, default=2026)
    p.add_argument("--db-path", required=True)
    p.add_argument("--downloads-dir", required=True)
    p.add_argument("--csv-path", required=True)
    p.add_argument("--jsonl-path", required=True)
    p.add_argument("--enable-canlii", default="false")
    return p.parse_args()


def export(db: Database, cases_csv: str, cases_jsonl: str):
    artifacts_dir = Path(cases_csv).parent
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    cases = db.export_table("cases")
    docs = db.export_table("documents")
    sources = db.export_table("sources")

    pd.DataFrame(cases).to_csv(cases_csv, index=False)
    write_jsonl(cases, cases_jsonl)
    pd.DataFrame(docs).to_csv(artifacts_dir / "documents.csv", index=False)
    write_jsonl(docs, artifacts_dir / "documents.jsonl")
    pd.DataFrame(sources).to_csv(artifacts_dir / "sources.csv", index=False)


def main():
    args = parse_args()
    db = Database(args.db_path)

    sources = []
    sources.extend(scrape_tribunal(args.start_year, args.end_year, args.downloads_dir))
    sources.extend(scrape_decision_summaries(args.start_year, args.end_year))
    sources.extend(scrape_bureau_sources())
    if str(args.enable_canlii).lower() == "true":
        sources.extend(scrape_canlii_optional(args.downloads_dir))

    for entry in sources:
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

    export(db, args.csv_path, args.jsonl_path)
    db.close()


if __name__ == "__main__":
    main()
