from pathlib import Path

from scraper.db import Database
from scraper.main import parse_args
from scraper.models import CaseRecord


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--db-path",
            "data/test.db",
            "--downloads-dir",
            "downloads",
            "--csv-path",
            "artifacts/cases.csv",
            "--jsonl-path",
            "artifacts/cases.jsonl",
        ],
    )
    args = parse_args()
    assert args.start_year == 1986
    assert args.end_year == 2026
    assert args.enable_canlii == "false"


def test_database_upsert_case(tmp_path):
    db_path = tmp_path / "cases.db"
    db = Database(str(db_path))
    case = CaseRecord(
        source="test",
        source_case_id="abc123",
        title="Test Case",
        year=2026,
        case_url="https://example.com/case",
    )
    case_id = db.upsert_case(case)
    rows = db.export_table("cases")
    db.close()

    assert case_id > 0
    assert len(rows) == 1
    assert rows[0]["title"] == "Test Case"
    assert Path(db_path).exists()
