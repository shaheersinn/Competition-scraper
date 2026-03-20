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
    full_text: str | None = None
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
    extracted_text: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PartyRecord:
    source_case_id: str
    party_name: str
    party_role: str | None = None


@dataclass
class ReferenceDocument:
    """
    A standalone reference document (NOT a court case).

    category / sub_category map to the download folder hierarchy:
        downloads/
          enforcement/
            release/            Bureau press releases
            consent-order/      Consent agreements & orders
            rss/                Live RSS feed items
          market-study/
            grocery-retail/
            digital-advertising/
            real-estate/
            general/
          stats/
            industry-concentration/   StatsCan NAICS data
            telecom/                  CRTC monitoring reports
            banking/                  OSFI financial data
            airlines/                 Transport Canada air stats
            grocery/                  Retail Council data
          legal/
            act/                Competition Act full text
            rules/              Competition Tribunal Rules
            guidelines/         Bureau guidelines & bulletins
            key-decision/       Defining case law (Martin 2026 etc.)
          consumer/
            complaints/         Complaint datasets
            open-data/          Open Government Canada CSVs/JSONs
    """
    category: str
    sub_category: str | None
    title: str
    source_url: str
    local_path: str | None = None
    file_type: str | None = None      # "pdf", "csv", "json", "html", "xml"
    publisher: str | None = None
    published_date: str | None = None
    description: str | None = None
    extracted_text: str | None = None
    sha256: str | None = None
    file_size: int | None = None
    mime_type: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)
