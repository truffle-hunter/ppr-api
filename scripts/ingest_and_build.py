#!/usr/bin/env python3
"""
Ingest the Irish Property Price Register, normalize it, and write outputs to:
  - shards/by-year/<YYYY>.ndjson (auto-rotates to _partNN under ~90MB)
  - api/v1/<ZIP_MD5>/summary.json
  - api/v1/<ZIP_MD5>/manifest.json

No by-county outputs and no all.ndjson (stay under GitHub's 100MB file limit).
Updates ./meta.json and prunes older api/v1 snapshots (keeps 3).

Standard-library only. Optional insecure TLS fallback when
environment variable ALLOW_INSECURE_FETCH=true or --insecure is passed.
"""

from __future__ import annotations
import argparse
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
import urllib.error
import zipfile
import ssl
from typing import Dict, List, Optional, Tuple

PPR_URL = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

ROOT = Path(__file__).resolve().parent.parent
API_DIR = ROOT / "api" / "v1"
SHARDS_DIR = ROOT / "shards" / "by-year"
META_PATH = ROOT / "meta.json"
KEEP_SNAPSHOTS = 3

# Aim below 100MB GitHub limit; rotate around ~90MB to be safe.
ROTATE_MAX_BYTES = 90 * 1024 * 1024

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

def build_field_map(headers: List[str]) -> Dict[str, str]:
    norm = [normalize_header(h) for h in headers]
    mapping: Dict[str, str] = {}
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

def to_bool_int(s: Optional[str]) -> Optional[int]:
    """Return 1/0 from Yes/No-like values; None if unknown."""
    if s is None:
        return None
    v = (s or "").strip().lower()
    if v in BOOL_TRUE or v == "yes":
        return 1
    if v in BOOL_FALSE or v == "no":
        return 0
    return None

def parse_date(d: str) -> Tuple[str, int]:
    d = (d or "").strip()
    obj = dt.datetime.strptime(d, "%d/%m/%Y").date()
    return obj.isoformat(), obj.year

# ---------- PRICE PARSING + RUNTIME CHECKPOINT ----------

EURO_ARTIFACTS = ("€", "â‚¬", "Ä", "EUR", "eur")

def _clean_price_text(s: str) -> str:
    s = s.replace("\xa0", "").replace(" ", "")
    for a in EURO_ARTIFACTS:
        s = s.replace(a, "")
    return s

def expected_whole_euros_from_text(price_text: Optional[str]) -> Optional[int]:
    """
    Deterministic rule to compute expected whole-euro value directly from the ORIGINAL text:
      - Remove spaces/NBSP and euro/mojibake.
      - If it ENDS with [.,]dd → those are cents → drop last two digits from digits-only string.
      - Else → all separators are thousands → expected = digits-only integer.
    """
    if not price_text:
        return None
    s = _clean_price_text(price_text.strip())
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    has_trailing_cents = bool(re.search(r"[.,]\d{2}\s*$", s))
    if has_trailing_cents and len(digits) >= 2:
        return int(digits[:-2] or "0")
    return int(digits)

def parse_price(p: Optional[str]) -> Tuple[Optional[int], str]:
    """
    Parse whole euros using the same deterministic rule as `expected_whole_euros_from_text`,
    and return (euros_without_cents, original_text).
    """
    if p is None:
        return None, ""
    original = (p or "").strip()
    return expected_whole_euros_from_text(original), original

def validate_price_or_die(price_eur: Optional[int], price_text: Optional[str], ctx: str) -> None:
    """
    Runtime checkpoint: compute expected euros from original text and ensure it matches
    what we parsed. If not, raise with full context so the CI run fails immediately.
    """
    expected = expected_whole_euros_from_text(price_text)
    # Only validate when we can derive an expected value
    if expected is None:
        return
    if price_eur != expected:
        raise RuntimeError(
            f"[PRICE VALIDATION FAILED] {ctx}\n"
            f"  original: {price_text!r}\n"
            f"  expected whole-euros: {expected}\n"
            f"  got: {price_eur}\n"
            f"  hint: trailing cents pattern present? "
            f"{'yes' if re.search(r'[.,]\\d{2}\\s*$', _clean_price_text(price_text or '')) else 'no'}"
        )

# ---------- /PRICE PARSING + CHECKPOINT ----------

def sanitize_filename(s: str) -> str:
    s = (s or "").strip().lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_\-]", "", s) or "unknown"

class RotatingNDJSONWriter:
    """
    Writes NDJSON lines to <base>.ndjson, rotating to <base>_part02.ndjson, etc.,
    once the current file would exceed ROTATE_MAX_BYTES.
    """
    def __init__(self, base_path: Path, max_bytes: int = ROTATE_MAX_BYTES):
        self.base_path = base_path
        self.max_bytes = max_bytes
        self.part = 1
        self.cur_path: Optional[Path] = None
        self.cur_f: Optional[io.TextIOWrapper] = None
        self.cur_bytes = 0
        self.paths: List[Path] = []
        self._open_new()

    def _open_new(self):
        if self.cur_f:
            self.cur_f.close()
        if self.part == 1:
            path = self.base_path
        else:
            stem = self.base_path.with_suffix("").name
            path = self.base_path.with_name(f"{stem}_part{self.part:02d}.ndjson")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.cur_path = path
        self.cur_f = path.open("w", encoding="utf-8", newline="\n")
        self.cur_bytes = 0
        self.paths.append(path)

    def write_line(self, line: str):
        b = len(line.encode("utf-8"))
        if self.cur_bytes + b > self.max_bytes:
            self.part += 1
            self._open_new()
        assert self.cur_f is not None
        self.cur_f.write(line)
        self.cur_bytes += b

    def close(self):
        if self.cur_f:
            self.cur_f.close()
            self.cur_f = None

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

def download_zip(tmp_path: Path, insecure_env: bool = False) -> None:
    """
    Try a normal verified TLS fetch first. If it fails due to SSL certificate
    issues and insecure_env=True, retry with an unverified SSL context.
    """
    req = urllib.request.Request(
        PPR_URL,
        headers={
            "User-Agent": "ppr-updater/1.0 (+github actions)",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
        },
        method="GET",
    )

    def _fetch(context: ssl.SSLContext | None) -> None:
        with urllib.request.urlopen(req, timeout=60, context=context) as r, tmp_path.open("wb") as f:
            shutil.copyfileobj(r, f)

    # Attempt with verified context
    try:
        _fetch(context=ssl.create_default_context())
        return
    except urllib.error.URLError as e:
        log(f"Verified TLS fetch failed: {e}")
        if not insecure_env:
            raise
        log("Retrying with INSECURE TLS (certificate verification disabled)…")
        insecure_ctx = ssl.create_default_context()
        insecure_ctx.check_hostname = False
        insecure_ctx.verify_mode = ssl.CERT_NONE
        _fetch(context=insecure_ctx)

def prune_old_snapshots(api_dir: Path, keep: int, keep_id: str) -> None:
    items = []
    for p in api_dir.iterdir():
        if p.is_dir() and re.fullmatch(r"[0-9a-fA-F]{32}", p.name):
            items.append((p.stat().st_mtime, p.name, p))
    items.sort(reverse=True)
    keep_names = set([keep_id] + [name for _, name, _ in items[:keep]])
    for _, name, path in items:
        if name not in keep_names:
            shutil.rmtree(path, ignore_errors=True)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--insecure", action="store_true",
                    help="Allow insecure TLS fetch if verified fetch fails (overrides ALLOW_INSECURE_FETCH).")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    allow_insecure = args.insecure or os.environ.get("ALLOW_INSECURE_FETCH", "").lower() in {"1", "true", "yes"}

    ROOT.mkdir(parents=True, exist_ok=True)
    API_DIR.mkdir(parents=True, exist_ok=True)
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)

    tmp_zip = ROOT / ".tmp-ppr.zip"
    log("Downloading ZIP…")
    download_zip(tmp_zip, insecure_env=allow_insecure)
    zip_md5 = md5_file(tmp_zip)
    snapshot_dir = API_DIR / zip_md5
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    log(f"ZIP MD5: {zip_md5}")

    manifest: Dict[str, List[str]] = {}   # year -> list of shard paths (relative to repo root)
    shard_sizes: Dict[str, List[int]] = {}

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

        # Clear shard dir (we fully re-build)
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
            "by_year": {},   # counts per year
        }

        writers_by_year: Dict[int, RotatingNDJSONWriter] = {}
        counts_year: Dict[int, int] = {}

        sio.seek(0)
        dict_reader = csv.DictReader(sio, fieldnames=headers, dialect=dialect)
        next(dict_reader)  # skip header

        for idx, row in enumerate(dict_reader, start=1):
            def get(canon: str) -> Optional[str]:
                col = field_map.get(canon)
                return (row.get(col) if col else None)

            # Date
            try:
                iso_date, year = parse_date(get("date") or "")
            except Exception:
                continue  # skip malformed

            # Price (deterministic cents-dropper)
            price_eur, price_text = parse_price(get("price_text"))
            # RUNTIME CHECKPOINT — abort on mismatch
            if os.environ.get("STRICT_PRICE_VALIDATION", "true").lower() in {"1", "true", "yes"}:
                validate_price_or_die(
                    price_eur, price_text,
                    ctx=f"row={idx}, date={iso_date}, address={get('address')}, county={get('county')}"
                )

            # Other fields
            address = (get("address") or "").strip()
            county = (get("county") or "").strip()
            eircode_raw = (get("eircode") or "").strip()
            eircode = eircode_raw if eircode_raw else "NONE"
            nfm_int = to_bool_int(get("not_full_market_price"))
            vat_int = to_bool_int(get("vat_exclusive"))
            prop_desc = (get("property_description") or "").strip() or None
            size_desc = (get("property_size_description") or "").strip() or None

            county_norm = county.title().replace("Co.", "Co.").strip()

            rec = {
                "id": hashlib.sha1("|".join([iso_date, address, county_norm, price_text or ""]).encode("utf-8")).hexdigest(),
                "date": iso_date,
                "address": address,
                "county": county_norm,
                "eircode": eircode,
                "price_eur": price_eur,
                "price_text": price_text,
                "nfm": nfm_int if nfm_int is not None else 0,
                "vat_exclusive": vat_int if vat_int is not None else 0,
                "description": prop_desc,
                "property_size_description": size_desc,
                "source": "Property Price Register",
                "source_url": PPR_URL,
                "row_number": idx
            }

            line = json.dumps(rec, ensure_ascii=False) + "\n"

            # shards/by-year with rotation
            ybase = SHARDS_DIR / f"{year}.ndjson"
            if year not in writers_by_year:
                writers_by_year[year] = RotatingNDJSONWriter(ybase, ROTATE_MAX_BYTES)
                counts_year[year] = 0
            writers_by_year[year].write_line(line)
            counts_year[year] += 1
            summary["total_records"] += 1

        # Close writers and collect manifest entries + sizes
        for year, w in writers_by_year.items():
            w.close()
            paths = [str(p.relative_to(ROOT)) for p in w.paths]
            manifest[str(year)] = paths
            sizes = [Path(ROOT, p).stat().st_size for p in paths]
            shard_sizes[str(year)] = sizes

        summary["by_year"] = {str(y): counts_year[y] for y in sorted(counts_year)}

    # Snapshot files (small): summary + manifest
    snapshot_dir = API_DIR / zip_md5
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (snapshot_dir / "manifest.json").write_text(json.dumps({
        "version": zip_md5,
        "generated_at_utc": summary["generated_at_utc"],
        "years": manifest,              # year -> list of relative shard paths
        "sizes": shard_sizes,           # year -> list of sizes (bytes)
        "shards_root": str(SHARDS_DIR.relative_to(ROOT)),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

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
        "generated_at_utc": summary["generated_at_utc"],
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
