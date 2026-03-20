from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .models import CaseRecord, DocumentRecord, PartyRecord


class Database:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self._migrate()
        self._init_sources()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sources (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT UNIQUE,
              base_url TEXT,
              notes TEXT
            );
            CREATE TABLE IF NOT EXISTS cases (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source TEXT NOT NULL,
              source_case_id TEXT NOT NULL,
              case_number TEXT,
              title TEXT NOT NULL,
              year INTEGER,
              date_filed TEXT,
              date_decided TEXT,
              court_or_tribunal TEXT,
              case_type TEXT,
              neutral_citation TEXT,
              language TEXT,
              status TEXT,
              summary TEXT,
              full_text TEXT,
              case_url TEXT NOT NULL,
              scraped_at TEXT NOT NULL,
              raw_json TEXT,
              UNIQUE(source, source_case_id)
            );
            CREATE TABLE IF NOT EXISTS documents (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              case_id INTEGER NOT NULL,
              source TEXT NOT NULL,
              document_title TEXT,
              document_type TEXT,
              document_date TEXT,
              document_url TEXT,
              local_path TEXT,
              sha256 TEXT,
              file_size INTEGER,
              mime_type TEXT,
              extracted_text TEXT,
              scraped_at TEXT NOT NULL,
              raw_json TEXT,
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
            CREATE VIRTUAL TABLE IF NOT EXISTS cases_fts
              USING fts5(title, full_text, summary, content='cases', content_rowid='id');
            """
        )
        self.conn.commit()

    def _migrate(self) -> None:
        """Add new columns to existing databases without destroying data."""
        cur = self.conn.cursor()
        existing_cols = {
            row[1] for row in cur.execute("PRAGMA table_info(cases)")
        }
        if "full_text" not in existing_cols:
            cur.execute("ALTER TABLE cases ADD COLUMN full_text TEXT")
        doc_cols = {
            row[1] for row in cur.execute("PRAGMA table_info(documents)")
        }
        if "extracted_text" not in doc_cols:
            cur.execute("ALTER TABLE documents ADD COLUMN extracted_text TEXT")
        self.conn.commit()

    def _init_sources(self) -> None:
        rows = [
            (
                "competition_tribunal",
                "https://decisions.ct-tc.gc.ca",
                "Competition Tribunal decisions and case documents",
            ),
            (
                "competition_tribunal_summaries",
                "https://www.ct-tc.gc.ca",
                "Competition Tribunal decision summaries",
            ),
            (
                "competition_bureau_rtp",
                "https://competition-bureau.canada.ca",
                "Restrictive trade practices cases and outcomes",
            ),
            (
                "competition_bureau_dmp",
                "https://competition-bureau.canada.ca",
                "Deceptive marketing practices cases and outcomes",
            ),
            (
                "federal_court",
                "https://decisions.fca-caf.gc.ca",
                "Federal Court competition-related decisions",
            ),
            (
                "supreme_court",
                "https://decisions.scc-csc.ca",
                "Supreme Court of Canada competition-related decisions",
            ),
            (
                "canlii_optional",
                "https://www.canlii.org",
                "CanLII indexed competition-law decisions",
            ),
        ]
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT OR IGNORE INTO sources(name, base_url, notes) VALUES (?, ?, ?)",
            rows,
        )
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_case(self, case: CaseRecord) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO cases (
              source, source_case_id, case_number, title, year, date_filed, date_decided,
              court_or_tribunal, case_type, neutral_citation, language, status, summary,
              full_text, case_url, scraped_at, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_case_id) DO UPDATE SET
              case_number=excluded.case_number,
              title=excluded.title,
              year=excluded.year,
              date_filed=excluded.date_filed,
              date_decided=excluded.date_decided,
              court_or_tribunal=excluded.court_or_tribunal,
              case_type=excluded.case_type,
              neutral_citation=excluded.neutral_citation,
              language=excluded.language,
              status=excluded.status,
              summary=excluded.summary,
              full_text=excluded.full_text,
              case_url=excluded.case_url,
              scraped_at=excluded.scraped_at,
              raw_json=excluded.raw_json
            """,
            (
                case.source,
                case.source_case_id,
                case.case_number,
                case.title,
                case.year,
                case.date_filed,
                case.date_decided,
                case.court_or_tribunal,
                case.case_type,
                case.neutral_citation,
                case.language,
                case.status,
                case.summary,
                case.full_text,
                case.case_url,
                self._now(),
                json.dumps(case.raw, ensure_ascii=False),
            ),
        )
        self.conn.commit()
        row = cur.execute(
            "SELECT id FROM cases WHERE source=? AND source_case_id=?",
            (case.source, case.source_case_id),
        ).fetchone()
        return int(row[0])

    def upsert_document(self, case_id: int, doc: DocumentRecord) -> None:
        self.conn.execute(
            """
            INSERT INTO documents (
              case_id, source, document_title, document_type, document_date,
              document_url, local_path, sha256, file_size, mime_type,
              extracted_text, scraped_at, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(case_id, document_url) DO UPDATE SET
              document_title=excluded.document_title,
              document_type=excluded.document_type,
              document_date=excluded.document_date,
              local_path=excluded.local_path,
              sha256=excluded.sha256,
              file_size=excluded.file_size,
              mime_type=excluded.mime_type,
              extracted_text=excluded.extracted_text,
              scraped_at=excluded.scraped_at,
              raw_json=excluded.raw_json
            """,
            (
                case_id,
                doc.source,
                doc.document_title,
                doc.document_type,
                doc.document_date,
                doc.document_url,
                doc.local_path,
                doc.sha256,
                doc.file_size,
                doc.mime_type,
                doc.extracted_text,
                self._now(),
                json.dumps(doc.raw, ensure_ascii=False),
            ),
        )
        self.conn.commit()

    def add_parties(self, case_id: int, parties: Iterable[PartyRecord]) -> None:
        rows = [(case_id, p.party_name, p.party_role or "") for p in parties]
        self.conn.executemany(
            "INSERT OR IGNORE INTO parties(case_id, party_name, party_role) VALUES (?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def export_table(self, table: str) -> list[dict]:
        cur = self.conn.execute(f"SELECT * FROM {table}")
        return [dict(r) for r in cur.fetchall()]

    def close(self) -> None:
        self.conn.close()
