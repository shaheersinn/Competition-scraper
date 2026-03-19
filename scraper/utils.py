from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def slugify(value: str, max_len: int = 120) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    value = re.sub(r"-+", "-", value).strip("-._")
    return value[:max_len] or "item"


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def dump_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_get(session_obj: requests.Session, url: str, timeout: int = 60) -> requests.Response:
    time.sleep(0.4)
    resp = session_obj.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


def download_file(session_obj: requests.Session, url: str, out_path: str | Path) -> dict[str, Any]:
    ensure_parent(out_path)
    with session_obj.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 128):
                if chunk:
                    f.write(chunk)
        mime = r.headers.get("Content-Type", "")
    return {
        "file_size": os.path.getsize(out_path),
        "sha256": sha256_file(out_path),
        "mime_type": mime,
    }


def abs_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(base, href)


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "download.bin"
    return slugify(name, max_len=180)


def write_jsonl(rows: Iterable[dict[str, Any]], path: str | Path) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
