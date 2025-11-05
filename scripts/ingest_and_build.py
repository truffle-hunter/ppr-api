#!/usr/bin/env python3
"""
Ingest the Irish Property Price Register, normalize it, and write outputs to:
  - shards/by-year/<YYYY>.ndjson
  - api/v1/<ZIP_MD5>/all.ndjson
  - api/v1/<ZIP_MD5>/by-county/<county>.ndjson
  - api/v1/<ZIP_MD5>/summary.json
Also updates ./meta.json and prunes older api/v1 snapshots (keeps 3).

Standard-library only.
"""

from __future__ import annotations
import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import urllib.request
import zipfile

PPR_URL = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

ROOT = Path(__file__).resolve().parent.parent
API_DIR = ROOT / "api" / "v1"
SHARDS_DIR = ROOT / "shards" / "by-year"
META_PATH = ROOT / "meta.json"
KEEP_SNAPSHOTS = 3

HEADER_MAP = {
    "date": ["date of sale (dd/mm/yyyy)", "date of sale", "date"],
    "address": ["address"],
    "county": ["county"],
    "eircode": ["eircode"],
    "price_text": ["price (€)", "price (â‚¬)", "price (ä)", "price", "price (eur)"],
    "not_full_market_price": ["not full market price", "not full market price?"],
    "vat_exclusive": ["vat exclusive", "vat-exclusive", "vat excl"],
    "property_description": ["description of property"],
    "property_size_description": ["property size description"],
}

BOOL_TRUE = {"y","yes","true","t"}
BOOL_FALSE = {"n","no","false","f"}

EURO_ARTIFACTS = ["€","â‚¬","Ä","EUR","eur"]

def log(msg: str) -> None:
    print(msg, file=sys.stderr)

def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "")).strip().lower()

def build_field_map(headers: list[str]) -> dict[str, str]:
    norm = [normalize_header(h) for h in headers]
    mapping = {}
    for canonical, options in HEADER_MAP.items():
        for i, h in enumerate(norm):
            if h in options:
                mapping[canonical] = headers[i]
                break
    required = ["date","address","county","price_text"]
    missing = [r for r in required if r not in mapping]
    if missing:
        raise ValueError(f"Missing required columns: {missing}\nHeaders: {headers}")
    return mapping

def to_bool(s: str | None) -> bool | None:
    if s is None:
        return None
    v = (s or "").strip().lower()
    if v in BOOL_TRUE: return True
    if v in BOOL_FALSE: return False
    if v == "yes": return True
    if v == "no": return False
    return None

def parse_date(d: str) -> tuple[str,int]:
    d = (d or "").strip()
    obj = dt.datetime.strptime(d, "%d/%m/%Y").date()
    return obj.isoformat(), obj.year

def parse_price(p: str | None) -> tuple[int | None, str]:
    """
    Return (euros_without_cents, original_text).

    Logic:
      - Strip everything except digits, commas, and dots for detection.
      - If the cleaned string ends with a separator followed by EXACTLY 2 digits
        (e.g., ",00" or ".02"), treat those two digits as cents and drop them.
      - Otherwise, treat all separators as thousands and keep all digits.
    """
    import re
    if p is None:
        return None, ""
    original = (p or "").strip()

    # Keep only digits and separators for detection
    cleaned = re.sub(r"[^\d\.,]", "", original.replace("\xa0", "").replace(" ", ""))

    # Just the digits, used to build the integer result
    digits_only = re.sub(r"\D", "", cleaned)
    if not digits_only:
        return None, original

    # If there are cents (…,[.,]dd at the very end), drop the last two digits
    if re.search(r"[.,]\d{2}\s*$", cleaned):
        euros = int(digits_only[:-2]) if len(digits_only) > 2 else 0
    else:
        euros = int(digits_only)

    return euros, original


def sanitize_filename(s: str) -> str:
    s = (s or "").strip().lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_\-]","", s) or "unknown"

def open_ndjson_writer(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w", encoding="utf-8", newline="\n")

def pick_csv_member(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    candidates = [m for m in zf.infolist() if m.filename.lower().endswith(".csv")]
    if not candidates:
        raise FileNotFoundError("No CSV inside ZIP.")
    candidates.sort(key=lambda m: (not re.search(r"ppr.*all", m.filename.lower()), m.filename.lower()))
    return candidates[0]

def read_csv_text(member: zipfile.ZipInfo, zf: zipfile.ZipFile) -> io.StringIO:
    raw = zf.read(member)
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return io.StringIO(raw.decode(enc))
        except UnicodeDecodeError:
            continue
    return io.StringIO(raw.decode("utf-8", errors="replace"))

def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download_zip(tmp_path: Path) -> None:
    req = urllib.request.Request(
        PPR_URL,
        headers={"User-Agent":"ppr-updater/1.0 (+github actions)"}
    )
    with urllib.request.urlopen(req) as r, tmp_path.open("wb") as f:
        shutil.copyfileobj(r, f)

def prune_old_snapshots(api_dir: Path, keep: int, keep_id: str) -> None:
    # delete older version folders, preserving 'keep' most-recent plus the current
    items = []
    for p in api_dir.glob("[0-9a-fA-F]"*32):  # md5 length
        if p.is_dir():
            ts = p.stat().st_mtime
            items.append((ts, p.name, p))
    items.sort(reverse=True)
    keep_names = set([keep_id] + [name for _, name, _ in items[:keep]])
    for _, name, path in items:
        if name not in keep_names:
            shutil.rmtree(path, ignore_errors=True)

def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    API_DIR.mkdir(parents=True, exist_ok=True)
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)

    tmp_zip = ROOT / ".tmp-ppr.zip"
