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

# CSV headers vary slightly across vintages; map them to canonical names.
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

BOOL_TRUE = {"y", "yes", "true", "t"}
BOOL_FALSE = {"n", "no", "false", "f"}

def log(msg: str) -> None:
    print(msg, file=sys.stderr)

def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "")).strip().lower()

def build_field_map(headers: list[str]) -> dict[str, str]:
    norm = [normalize_header(h) for h in headers]
    mapping: dict[str, str] = {}
    for canonical, options in HEADER_MAP.items():
        for i, h in enumerate(norm):
            if h in options:
                mapping[canonical] = headers[i]
                break
    required = ["date", "address", "county", "price_text"]
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

def parse_date(d: str) -> tuple[str, int]:
    d = (d or "").strip()
    obj = dt.datetime.strptime(d, "%d/%m/%Y").date()
    return obj.isoformat(), obj.year

def parse_price(p: str | None) -> tuple[int | None, str]:
    """
    Return (euros_without_cents, original_text), robust across US/EU formats.

    Rules:
      - Strip euro symbols/mojibake (€, â‚¬, Ä, EUR) and spaces.
      - Treat the **rightmost** of '.' or ',' as decimal only if there are exactly 1–2 digits after it;
        otherwise treat separators as thousands.
      - Drop the fractional part and return an int.
    """
    if p is None:
        return None, ""
    original = (p or "").strip()

    s = original
    for art in ("€", "â‚¬", "Ä", "EUR", "eur"):
        s = s.replace(art, "")
    s = s.replace("\xa0", "").replace(" ", "")
    # Keep only digits and separators
    s = re.sub(r"[^0-9\.,-]", "", s)
    if not s:
        return None, original

    has_dot = "." in s
    has_comma = "," in s

    integer_part = s
    if has_dot and has_comma:
        # Consider the rightmost separator as decimal and drop fraction
        idx = max(s.rfind("."), s.rfind(","))
        integer_part = s[:idx]
    elif has_dot or has_comma:
        sep = "." if has_dot else ","
        idx = s.rfind(sep)
        # Count numeric digits after the separator
        digits_after = len(re.sub(r"[^0-9]", "", s[idx+1:]))
        if 1 <= digits_after <= 2:  # likely cents
            integer_part = s[:idx]
        else:
            integer_part = s  # separator is thousands, not decimal

    # Remove any remaining non-digits (like thousands separators) and parse
    integer_digits = re.sub(r"[^0-9-]", "", integer_part)
    if integer_digits in ("", "-"):
        return None, original

    try:
        euros = int(integer_digits)
    except ValueError:
        euros = None

    return euros, original

def fix_hundred_inflation(euros: int | None, price_text: str | None) -> int | None:
    """
    If the original text ends with a decimal separator + two digits (cents),
    and our integer still *appears* to include those two digits (the classic ×100 bug),
    divide by 100. This is a final safety clamp.
    """
    if euros is None or not price_text:
        return euros
    s = price_text.strip().replace("\xa0", "").replace(" ", "")
    has_cents = bool(re.search(r"[.,]\d{2}\s*$", s))
    if not has_cents:
        return euros

    digits_in_text = len(re.sub(r"\D", "", s))      # includes cents
    digits_in_euros = len(str(abs(euros)))

    # If digit counts match, we likely kept the cents; fix it.
    if digits_in_euros == digits_in_text:
        return euros // 100
    return euros

def sanitize_filename(s: str) -> str:
    s = (s or "").strip().lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_\-]", "", s) or "unknown"

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
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download_zip(tmp_path: Path) -> None:
    req = urllib.request.Request(
        PPR_URL,
        headers={"User-Agent": "ppr-updater/1.0 (+github actions)"}
    )
    with urllib.request.urlopen(req) as r, tmp_path.open("wb") as f:
        shutil.copyfileobj(r, f)

def prune_old_snapshots(api_dir: Path, keep: int, keep_id: str) -> None:
    # delete older version folders, preserving 'keep' most-recent plus the current
    items = []
    for p in api_dir.iterdir():
        if p.is_dir() and re.fullmatch(r"[0-9a-fA-F]{32}", p.name):
            items.append((p.stat().st_mtime, p.name, p))
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
    log("Downloading ZIP…")
    download_zip(tmp_zip)
    zip_md5 = md5_file(tmp_zip)
    snapshot_dir = API_DIR / zip_md5
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    log(f"ZIP MD5: {zip_md5}")

    with zipfile.ZipFile(tmp_zip, "r") as zf:
        member = pick_csv_member(zf)
        log(f"CSV member: {member.filename}")
        sio = read_csv_text(member, zf)

        sample = sio.read(4096); sio.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
            dialect.doublequote = True
        except csv.Error:
            class D(csv.Dialect):
                delimiter=","; quotechar='"'; doublequote=True; skipinitialspace=False; lineterminator="\n"; quoting=csv.QUOTE_MINIMAL
            dialect = D()

        reader = csv.reader(sio, dialect)
        headers = next(reader)
        field_map = build_field_map(headers)

        # Prepare writers (write to snapshot + shards)
        all_path = snapshot_dir / "all.ndjson"
        by_county_dir = snapshot_dir / "by-county"
        by_county_dir.mkdir(parents=True, exist_ok=True)

        # clear shard dir (we fully re-build)
        if SHARDS_DIR.exists():
            for p in SHARDS_DIR.glob("*.ndjson"):
                p.unlink()

        summary = {
            "source": "Property Price Register",
            "source_url": PPR_URL,
            "zip_md5": zip_md5,
            "zip_member": member.filename,
            "generated_at_utc": dt.datetime.utcnow().isoformat() + "Z",
            "total_records": 0,
            "by_year": {},
            "by_county": {}
        }

        writers_by_year: dict[int, io.TextIOBase] = {}
        writers_by_county: dict[str, io.TextIOBase] = {}
        counts_year: dict[int, int] = {}
        counts_county: dict[str, int] = {}

        with open_ndjson_writer(all_path) as all_f:
            sio.seek(0)
            dict_reader = csv.DictReader(sio, fieldnames=headers, dialect=dialect)
            next(dict_reader)  # skip header

            for idx, row in enumerate(dict_reader, start=1):
                def get(canon: str) -> str | None:
                    col = field_map.get(canon)
                    return (row.get(col) if col else None)

                # Date
                try:
                    iso_date, year = parse_date(get("date") or "")
                except Exception:
                    # skip malformed dates
                    continue

                # Price (parse + safety clamp)
                price_eur, price_text = parse_price(get("price_text"))
                price_eur = fix_hundred_inflation(price_eur, price_text)

                # Other fields
                address = (get("address") or "").strip()
                county = (get("county") or "").strip()
                eircode_raw = (get("eircode") or "").strip()
                eircode = eircode_raw if eircode_raw else "NONE"  # explicit string if missing
                nfp = to_bool(get("not_full_market_price"))
                vat_exc = to_bool(get("vat_exclusive"))
                prop_desc = (get("property_description") or "").strip() or None
                size_desc = (get("property_size_description") or "").strip() or None

                county_norm = county.title().replace("Co.", "Co.").strip()

                rec = {
                    "id": hashlib.sha1("|".join([iso_date, address, county_norm, price_text]).encode("utf-8")).hexdigest(),
                    "date": iso_date,
                    "address": address,
                    "county": county_norm,
                    "eircode": eircode,
                    "price_eur": price_eur,
                    "price_text": price_text,
                    "not_full_market_price": nfp,
                    "vat_exclusive": vat_exc,
                    "property_description": prop_desc,
                    "property_size_description": size_desc,
                    "source": "Property Price Register",
                    "source_url": PPR_URL,
                    "row_number": idx
                }

                line = json.dumps(rec, ensure_ascii=False) + "\n"
                all_f.write(line)

                # shards/by-year
                ypath = SHARDS_DIR / f"{year}.ndjson"
                if year not in writers_by_year:
                    writers_by_year[year] = open_ndjson_writer(ypath)
                    counts_year[year] = 0
                writers_by_year[year].write(line)
                counts_year[year] += 1

                # api/v1/<md5>/by-county
                ckey = county_norm
                cpath = by_county_dir / f"{sanitize_filename(ckey)}.ndjson"
                if ckey not in writers_by_county:
                    writers_by_county[ckey] = open_ndjson_writer(cpath)
                    counts_county[ckey] = 0
                writers_by_county[ckey].write(line)
                counts_county[ckey] += 1

                summary["total_records"] += 1

        for f in writers_by_year.values():
            f.close()
        for f in writers_by_county.values():
            f.close()

        summary["by_year"] = {str(y): counts_year[y] for y in sorted(counts_year)}
        summary["by_county"] = dict(sorted(counts_county.items(), key=lambda kv: kv[0].lower()))

        with (snapshot_dir / "summary.json").open("w", encoding="utf-8") as sf:
            json.dump(summary, sf, ensure_ascii=False, indent=2)

    # Update meta.json
    meta = {}
    if META_PATH.exists():
        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    previous = meta.get("latest_version")
    meta.update({
        "latest_version": zip_md5,
        "generated_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        "source_url": PPR_URL,
        "total_records": summary["total_records"],
        "previous_version": previous,
    })
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # Prune old snapshots
    prune_old_snapshots(API_DIR, KEEP_SNAPSHOTS, zip_md5)

    # Cleanup
    try:
        tmp_zip.unlink()
    except FileNotFoundError:
        pass

    log("Done.")
    log(f"Snapshot: {snapshot_dir}")
    log(f"Year shards: {SHARDS_DIR}")

if __name__ == "__main__":
    main()
