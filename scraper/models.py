from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CaseRecord:
    source: str
    source_case_id: str
    title: str
    case_url: str
    court_or_tribunal: str | None = None
    case_number: str | None = None
    year: int | None = None
    date_filed: str | None = None
    date_decided: str | None = None
    case_type: str | None = None
    neutral_citation: str | None = None
    language: str | None = None
    status: str | None = None
    summary: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentRecord:
    source: str
    source_case_id: str
    document_title: str
    document_url: str
    local_path: str | None = None
    document_type: str | None = None
    document_date: str | None = None
    mime_type: str | None = None
    sha256: str | None = None
    file_size: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PartyRecord:
    source_case_id: str
    party_name: str
    party_role: str | None = None
