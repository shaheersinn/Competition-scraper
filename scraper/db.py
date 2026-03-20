from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .models import CaseRecord, DocumentRecord, PartyRecord, ReferenceDocument


class Database:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self._migrate()
        self._init_sources()

    def _init_schema(self) -> None:
        self.conn.executescript("""
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS sources (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT UNIQUE, base_url TEXT, notes TEXT
            );

            CREATE TABLE IF NOT EXISTS cases (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source TEXT NOT NULL,
              source_case_id TEXT NOT NULL,
              case_number TEXT, title TEXT NOT NULL,
              year INTEGER, date_filed TEXT, date_decided TEXT,
              court_or_tribunal TEXT, case_type TEXT,
              neutral_citation TEXT, language TEXT, status TEXT,
              summary TEXT, full_text TEXT,
              case_url TEXT NOT NULL,
              scraped_at TEXT NOT NULL, raw_json TEXT,
              UNIQUE(source, source_case_id)
            );

            CREATE TABLE IF NOT EXISTS documents (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              case_id INTEGER NOT NULL,
              source TEXT NOT NULL,
              document_title TEXT, document_type TEXT,
              document_date TEXT, document_url TEXT,
              local_path TEXT, sha256 TEXT,
              file_size INTEGER, mime_type TEXT,
              extracted_text TEXT,
              scraped_at TEXT NOT NULL, raw_json TEXT,
              UNIQUE(case_id, document_url),
              FOREIGN KEY(case_id) REFERENCES cases(id)
            );

            CREATE TABLE IF NOT EXISTS parties (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              case_id INTEGER NOT NULL,
              party_name TEXT NOT NULL,
              party_role TEXT NOT NULL DEFAULT '',
              UNIQUE(case_id, party_name, party_role),
              FOREIGN KEY(case_id) REFERENCES cases(id)
            );

            CREATE TABLE IF NOT EXISTS reference_documents (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              category TEXT NOT NULL,
              sub_category TEXT,
              title TEXT NOT NULL,
              source_url TEXT NOT NULL UNIQUE,
              local_path TEXT,
              file_type TEXT,
              publisher TEXT,
              published_date TEXT,
              description TEXT,
              extracted_text TEXT,
              sha256 TEXT,
              file_size INTEGER,
              mime_type TEXT,
              scraped_at TEXT NOT NULL,
              raw_json TEXT
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS cases_fts
              USING fts5(title, full_text, summary,
                         content='cases', content_rowid='id');

            CREATE VIRTUAL TABLE IF NOT EXISTS reference_fts
              USING fts5(title, description, extracted_text,
                         content='reference_documents', content_rowid='id');
        """)
        self.conn.commit()

    def _migrate(self) -> None:
        """Add new columns to existing DBs without destroying data."""
        cur = self.conn.cursor()
        case_cols = {r[1] for r in cur.execute("PRAGMA table_info(cases)")}
        for col, defn in [("full_text", "TEXT"), ("summary", "TEXT")]:
            if col not in case_cols:
                cur.execute(f"ALTER TABLE cases ADD COLUMN {col} {defn}")
        doc_cols = {r[1] for r in cur.execute("PRAGMA table_info(documents)")}
        if "extracted_text" not in doc_cols:
            cur.execute("ALTER TABLE documents ADD COLUMN extracted_text TEXT")
        self.conn.commit()

    def _init_sources(self) -> None:
        rows = [
            ("competition_tribunal",     "https://decisions.ct-tc.gc.ca",         "Competition Tribunal Lexum decisions"),
            ("competition_tribunal_summaries", "https://www.ct-tc.gc.ca",          "Tribunal decision summaries page"),
            ("competition_bureau_rtp",   "https://competition-bureau.canada.ca",   "Restrictive trade practices outcomes"),
            ("competition_bureau_dmp",   "https://competition-bureau.canada.ca",   "Deceptive marketing outcomes"),
            ("federal_court",            "https://decisions.fca-caf.gc.ca",        "Federal Court decisions"),
            ("federal_court_appeal",     "https://decisions.fca-caf.gc.ca",        "Federal Court of Appeal decisions"),
            ("supreme_court",            "https://decisions.scc-csc.ca",           "Supreme Court of Canada decisions"),
            ("canlii_tribunal",          "https://www.canlii.org",                 "CanLII — Competition Tribunal"),
            ("canlii_federal_court",     "https://www.canlii.org",                 "CanLII — Federal Court"),
            ("canlii_fca",               "https://www.canlii.org",                 "CanLII — Federal Court of Appeal"),
            ("canlii_scc",               "https://www.canlii.org",                 "CanLII — Supreme Court of Canada"),
            ("bureau_enforcement",       "https://competition-bureau.canada.ca",   "Bureau enforcement releases + RSS"),
            ("bureau_market_studies",    "https://competition-bureau.canada.ca",   "Bureau published market studies"),
            ("statscan",                 "https://www150.statcan.gc.ca",           "Statistics Canada industry concentration"),
            ("crtc",                     "https://crtc.gc.ca",                     "CRTC communications monitoring reports"),
            ("osfi",                     "https://www.osfi-bsif.gc.ca",            "OSFI banking market share data"),
            ("transport_canada",         "https://tc.gc.ca",                       "Transport Canada airline statistics"),
            ("retail_council",           "https://www.retailcouncil.org",          "Retail Council grocery market data"),
            ("open_canada",              "https://open.canada.ca",                 "Open Government Canada datasets"),
            ("consumer_handbook",        "https://www.canadianconsumerhandbook.ca","Canadian Consumer Handbook complaints"),
            ("legal_texts",              "https://laws-lois.justice.gc.ca",        "Competition Act and legal texts"),
        ]
        self.conn.executemany(
            "INSERT OR IGNORE INTO sources(name, base_url, notes) VALUES (?,?,?)", rows
        )
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Cases ────────────────────────────────────────────────────────────────
    def upsert_case(self, case: CaseRecord) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO cases (
              source, source_case_id, case_number, title, year,
              date_filed, date_decided, court_or_tribunal, case_type,
              neutral_citation, language, status, summary, full_text,
              case_url, scraped_at, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source, source_case_id) DO UPDATE SET
              case_number=excluded.case_number, title=excluded.title,
              year=excluded.year, date_filed=excluded.date_filed,
              date_decided=excluded.date_decided,
              court_or_tribunal=excluded.court_or_tribunal,
              case_type=excluded.case_type,
              neutral_citation=excluded.neutral_citation,
              language=excluded.language, status=excluded.status,
              summary=excluded.summary, full_text=excluded.full_text,
              case_url=excluded.case_url, scraped_at=excluded.scraped_at,
              raw_json=excluded.raw_json
        """, (
            case.source, case.source_case_id, case.case_number, case.title,
            case.year, case.date_filed, case.date_decided,
            case.court_or_tribunal, case.case_type, case.neutral_citation,
            case.language, case.status, case.summary, case.full_text,
            case.case_url, self._now(),
            json.dumps(case.raw, ensure_ascii=False),
        ))
        self.conn.commit()
        row = cur.execute(
            "SELECT id FROM cases WHERE source=? AND source_case_id=?",
            (case.source, case.source_case_id),
        ).fetchone()
        return int(row[0])

    def upsert_document(self, case_id: int, doc: DocumentRecord) -> None:
        self.conn.execute("""
            INSERT INTO documents (
              case_id, source, document_title, document_type, document_date,
              document_url, local_path, sha256, file_size, mime_type,
              extracted_text, scraped_at, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(case_id, document_url) DO UPDATE SET
              document_title=excluded.document_title,
              document_type=excluded.document_type,
              document_date=excluded.document_date,
              local_path=excluded.local_path, sha256=excluded.sha256,
              file_size=excluded.file_size, mime_type=excluded.mime_type,
              extracted_text=excluded.extracted_text,
              scraped_at=excluded.scraped_at, raw_json=excluded.raw_json
        """, (
            case_id, doc.source, doc.document_title, doc.document_type,
            doc.document_date, doc.document_url, doc.local_path,
            doc.sha256, doc.file_size, doc.mime_type, doc.extracted_text,
            self._now(), json.dumps(doc.raw, ensure_ascii=False),
        ))
        self.conn.commit()

    def add_parties(self, case_id: int, parties: Iterable[PartyRecord]) -> None:
        self.conn.executemany(
            "INSERT OR IGNORE INTO parties(case_id, party_name, party_role) VALUES (?,?,?)",
            [(case_id, p.party_name, p.party_role or "") for p in parties],
        )
        self.conn.commit()

    # ── Reference documents ───────────────────────────────────────────────────
    def upsert_reference(self, ref: ReferenceDocument) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO reference_documents (
              category, sub_category, title, source_url, local_path,
              file_type, publisher, published_date, description,
              extracted_text, sha256, file_size, mime_type,
              scraped_at, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_url) DO UPDATE SET
              category=excluded.category,
              sub_category=excluded.sub_category,
              title=excluded.title,
              local_path=excluded.local_path,
              file_type=excluded.file_type,
              publisher=excluded.publisher,
              published_date=excluded.published_date,
              description=excluded.description,
              extracted_text=excluded.extracted_text,
              sha256=excluded.sha256, file_size=excluded.file_size,
              mime_type=excluded.mime_type,
              scraped_at=excluded.scraped_at,
              raw_json=excluded.raw_json
        """, (
            ref.category, ref.sub_category, ref.title, ref.source_url,
            ref.local_path, ref.file_type, ref.publisher, ref.published_date,
            ref.description, ref.extracted_text, ref.sha256, ref.file_size,
            ref.mime_type, self._now(),
            json.dumps(ref.raw, ensure_ascii=False),
        ))
        self.conn.commit()
        row = cur.execute(
            "SELECT id FROM reference_documents WHERE source_url=?", (ref.source_url,)
        ).fetchone()
        return int(row[0])

    def export_table(self, table: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(f"SELECT * FROM {table}").fetchall()]

    def close(self) -> None:
        self.conn.close()
