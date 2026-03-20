"""
Smoke tests — fast checks that the scraper package imports and core logic works.
Run with: pytest tests/ -v
"""
import re
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scraper.db import Database
from scraper.models import CaseRecord, DocumentRecord, PartyRecord
from scraper.utils import (
    extract_html_text,
    is_document_url,
    slugify,
)


# ── Model tests ────────────────────────────────────────────────────────────────

def test_case_record_has_full_text():
    rec = CaseRecord(
        source="test",
        source_case_id="123",
        title="Test v. Respondent",
        case_url="https://example.com/item/123/",
        full_text="This is the complete decision text.",
    )
    assert rec.full_text == "This is the complete decision text."


def test_document_record_has_extracted_text():
    doc = DocumentRecord(
        source="test",
        source_case_id="123",
        document_title="Order",
        document_url="https://example.com/doc.pdf",
        extracted_text="PDF text content here.",
    )
    assert doc.extracted_text == "PDF text content here."


# ── Database tests ─────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return Database(str(tmp_path / "test.db"))


def test_db_stores_full_text(tmp_db):
    rec = CaseRecord(
        source="test",
        source_case_id="ct-001",
        title="Commissioner v. Acme Corp",
        case_url="https://example.com/item/1/",
        full_text="The Tribunal finds that Acme Corp engaged in abuse of dominance under s.79.",
        summary="Abuse of dominance finding.",
    )
    case_id = tmp_db.upsert_case(rec)
    assert case_id > 0

    row = tmp_db.conn.execute(
        "SELECT full_text, summary FROM cases WHERE id=?", (case_id,)
    ).fetchone()
    assert "abuse of dominance" in row["full_text"]
    assert row["summary"] is not None


def test_db_upsert_is_idempotent(tmp_db):
    rec = CaseRecord(
        source="test",
        source_case_id="ct-001",
        title="First Title",
        case_url="https://example.com/item/1/",
        full_text="Original text.",
    )
    id1 = tmp_db.upsert_case(rec)

    rec.title = "Updated Title"
    rec.full_text = "Updated text."
    id2 = tmp_db.upsert_case(rec)

    # Same logical record — different inserts update in place
    count = tmp_db.conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
    assert count == 1

    row = tmp_db.conn.execute("SELECT title, full_text FROM cases").fetchone()
    assert row["title"] == "Updated Title"
    assert row["full_text"] == "Updated text."


def test_db_stores_extracted_text(tmp_db):
    rec = CaseRecord(
        source="test", source_case_id="ct-001",
        title="T", case_url="https://example.com/",
    )
    case_id = tmp_db.upsert_case(rec)
    doc = DocumentRecord(
        source="test",
        source_case_id="ct-001",
        document_title="Decision PDF",
        document_url="https://example.com/doc.pdf",
        extracted_text="Full PDF text here.",
    )
    tmp_db.upsert_document(case_id, doc)

    row = tmp_db.conn.execute("SELECT extracted_text FROM documents").fetchone()
    assert row["extracted_text"] == "Full PDF text here."


def test_db_parties(tmp_db):
    rec = CaseRecord(
        source="test", source_case_id="ct-001",
        title="A v. B", case_url="https://example.com/",
    )
    case_id = tmp_db.upsert_case(rec)
    tmp_db.add_parties(case_id, [
        PartyRecord("ct-001", "Company A", "applicant"),
        PartyRecord("ct-001", "Company B", "respondent"),
    ])
    parties = tmp_db.conn.execute("SELECT * FROM parties WHERE case_id=?", (case_id,)).fetchall()
    assert len(parties) == 2


def test_db_migration_runs_on_existing_db(tmp_path):
    """Migration should add full_text column to a db that was created without it."""
    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE cases (
          id INTEGER PRIMARY KEY, source TEXT, source_case_id TEXT, title TEXT,
          case_url TEXT, scraped_at TEXT, UNIQUE(source, source_case_id)
        )
    """)
    conn.execute("""
        CREATE TABLE documents (
          id INTEGER PRIMARY KEY, case_id INTEGER, source TEXT,
          document_title TEXT, document_url TEXT, scraped_at TEXT,
          UNIQUE(case_id, document_url)
        )
    """)
    conn.execute("CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY, name TEXT UNIQUE, base_url TEXT, notes TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS parties (id INTEGER PRIMARY KEY, case_id INTEGER, party_name TEXT, party_role TEXT, UNIQUE(case_id, party_name, party_role))")
    conn.commit()
    conn.close()

    # Opening with Database() should trigger migration and add full_text
    db = Database(db_path)
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(cases)")}
    assert "full_text" in cols
    doc_cols = {row[1] for row in db.conn.execute("PRAGMA table_info(documents)")}
    assert "extracted_text" in doc_cols
    db.close()


# ── Utils tests ────────────────────────────────────────────────────────────────

def test_is_document_url_pdf():
    assert is_document_url("https://example.com/decision.pdf")


def test_is_document_url_download_token():
    assert is_document_url("https://example.com/getattachment/file?download=1")


def test_is_document_url_negative():
    assert not is_document_url("https://example.com/about")
    assert not is_document_url("https://example.com/cases")


def test_extract_html_text_strips_nav():
    html = """
    <html><body>
      <nav id="wb-bar">Navigation junk</nav>
      <header>Header junk</header>
      <div id="wb-cont">
        <h1>Commissioner of Competition v. Acme Corp</h1>
        <p>The Tribunal orders that Acme cease the practice.</p>
      </div>
      <footer>Footer junk</footer>
    </body></html>
    """
    text = extract_html_text(html)
    assert "Commissioner" in text
    assert "Tribunal orders" in text
    # Nav/header/footer should be stripped
    assert "Navigation junk" not in text
    assert "Header junk" not in text
    assert "Footer junk" not in text


def test_slugify():
    assert slugify("Hello World / Test") == "Hello-World-Test"
    assert len(slugify("x" * 500)) <= 120


# ── Tribunal item ID extraction ────────────────────────────────────────────────

def test_item_id_from_url():
    from scraper.tribunal import _item_id
    assert _item_id("https://decisions.ct-tc.gc.ca/ct-tc/cdo/en/item/12345/index.do") == "12345"


def test_parse_parties():
    from scraper.tribunal import _parse_parties
    parties = _parse_parties("Commissioner of Competition v. Acme Corp")
    assert len(parties) == 2
    assert parties[0].party_role == "applicant/appellant"
    assert parties[1].party_role == "respondent"
    assert "Acme Corp" in parties[1].party_name


def test_parse_parties_no_v():
    from scraper.tribunal import _parse_parties
    parties = _parse_parties("Re: Application of XYZ Corp")
    assert parties == []
