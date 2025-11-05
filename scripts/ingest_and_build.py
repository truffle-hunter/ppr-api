#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds a static API from Ireland's Property Price Register ZIP:
- Secure-first download of the ZIP (Requests + certifi), with optional curl fallback
  and an *explicitly opt-in* insecure mode via ALLOW_INSECURE_FETCH=1.
- Robust CSV header normalization (handles CP1252 euro symbol, NBSPs, spacing).
- Normalizes rows and writes per-year NDJSON shards.
- Emits meta.json with updated timestamp, shard list, and source provenance.
- Writes robots.txt and .nojekyll at the repo root.
- Prints a masked token hint to the GitHub Actions job summary.

Environment:
  ENDPOINT_TOKEN         - (recommended) stable token for the endpoint path
  ALLOW_INSECURE_FETCH   - set to "1" to allow curl -k fallback (TEMPORARY only)
"""

import os
import io
import re
import json
import glob
import time
import uuid
import hashlib
import zipfile
import tempfile
import subprocess
import unicodedata
from datetime import datetime, timezone

import pandas as pd
import requests
import certifi
from dateutil import parser as dateparser
from requests.exceptions import SSLError, RequestException

# -------------------------
# Config
# -------------------------

PPR_ZIP_URL = (
    "https://www.propertypriceregister.ie/website/npsra/ppr/"
    "npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"
)

ROOT = os.getcwd()
DATA_DIR = os.path.join(ROOT, "data")
API_DIR = os.path.join(ROOT, "api", "v1")
TOKEN_FILE = os.path.join(DATA_DIR, "endpoint_token.txt")

# Global flag for provenance in meta.json
TLS_VERIFIED = True


# -------------------------
# Utilities
# -------------------------

def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(API_DIR, exist_ok=True)


def safe_preview(tok: str) -> str:
    """Mask a token so it can be logged safely (first 6 + last 4 chars)."""
    if not tok or len(tok) < 12:
        return "hidden"
    return f"{tok[:6]}…{tok[-4:]}"


def load_or_make_token() -> str:
    """Prefer ENDPOINT_TOKEN env; otherwise persist a generated UUID hex to data/."""
    tok = os.environ.get("ENDPOINT_TOKEN")
    if tok:
        return tok.strip()
    if os.path.exists(TOKEN_FILE):
        return open(TOKEN_FILE, "r", encoding="utf-8").read().strip()
    t = uuid.uuid4().hex
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(t)
    return t


def canonicalize_column(name: str) -> str:
    """
    Normalize a raw CSV header and map it to our canonical keys.
    Handles CP1252/UTF-8 euro signs, NBSPs, spacing, and casing.

    Returns one of:
      date, address, eircode, county, price, nfm, vat_exclusive, description
    or "" if unknown.
    """
    if not name:
        return ""
    s = str(name)
    # Normalize Unicode, replace NBSP, collapse spaces, lowercase
    s = unicodedata.normalize("NFKC", s).replace("\xa0", " ").strip().lower()
    s = re.sub(r"\s+", " ", s)

    if "date of sale" in s or s == "date":
        return "date"
    if "address" in s:
        return "address"
    if "eircode" in s or "postal code" in s or "postcode" in s:
        return "eircode"
    if "county" in s:
        return "county"
    if "price" in s:  # matches "price (€)" / "price (�)" / "price"
        return "price"
    if "not full market" in s:
        return "nfm"
    if "vat exclusive" in s or "vat-exclusive" in s:
        return "vat_exclusive"
    if "description" in s:
        return "description"
    return ""


def fetch_zip() -> io.BytesIO:
    """
    Secure-first fetch with explicit CA bundle.
    Fallbacks:
      - curl --cacert <certifi>
      - if ALLOW_INSECURE_FETCH=1, curl -k (NOT recommended; temporary only)
    """
    global TLS_VERIFIED
    ca = certifi.where()
    headers = {"User-Agent": "ppr-ingest/1.0 (+https://github.com/your/repo)"}

    # Primary attempt: requests + certifi
    try:
        r = requests.get(
            PPR_ZIP_URL,
            headers=headers,
            verify=ca,
            timeout=180,
            allow_redirects=True,
        )
        r.raise_for_status()
        return io.BytesIO(r.content)
    except SSLError:
        print("requests SSLError; trying curl with CA bundle…", flush=True)
        # Secondary attempt: curl with CA bundle
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--retry",
                    "5",
                    "--retry-all-errors",
                    "--max-time",
                    "180",
                    "--cacert",
                    ca,
                    "-o",
                    tmp_path,
                    PPR_ZIP_URL,
                ],
                check=True,
            )
            data = open(tmp_path, "rb").read()
            os.unlink(tmp_path)
            return io.BytesIO(data)
        except subprocess.CalledProcessError:
            # Optional last resort: only if explicitly allowed via env
            if os.environ.get("ALLOW_INSECURE_FETCH") == "1":
                print("curl --insecure fallback enabled by ALLOW_INSECURE_FETCH=1")
                subprocess.run(
                    [
                        "curl",
                        "--fail",
                        "--location",
                        "--retry",
                        "5",
                        "--retry-all-errors",
                        "--max-time",
                        "180",
                        "-k",  # INSECURE: skip TLS verification
                        "-o",
                        tmp_path,
                        PPR_ZIP_URL,
                    ],
                    check=True,
                )
                data = open(tmp_path, "rb").read()
                os.unlink(tmp_path)
                TLS_VERIFIED = False
                return io.BytesIO(data)
            raise
    except RequestException as e:
        print(f"requests error: {e}")
        raise


def read_all_csvs(zbytes: io.BytesIO) -> pd.DataFrame:
    """Read all CSVs from the ZIP; return a unified DataFrame with canonical columns."""
    frames = []
    with zipfile.ZipFile(zbytes) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as f:
                # Keep as strings; avoid NA coercions that can eat values
                df = pd.read_csv(
                    f,
                    encoding="latin-1",
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                )

            # Build a rename map using our canonicalizer
            rename = {}
            for c in list(df.columns):
                canon = canonicalize_column(c)
                if canon and canon not in df.columns:
                    rename[c] = canon
            df = df.rename(columns=rename)

            # Ensure the expected schema exists
            keep = [
                "date",
                "address",
                "eircode",
                "county",
                "price",
                "nfm",
                "vat_exclusive",
                "description",
            ]
            for k in keep:
                if k not in df.columns:
                    df[k] = None
            df = df[keep]
            frames.append(df)

    if not frames:
        raise RuntimeError("No CSV files found in ZIP.")
    return pd.concat(frames, ignore_index=True)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and enrich fields; return a tidy DataFrame ready for emission."""

    def parse_date(x):
        try:
            return dateparser.parse(str(x), dayfirst=True).date().isoformat()
        except Exception:
            return None

    def parse_price(x):
        # Strip everything except digits
        s = re.sub(r"[^\d]", "", str(x))
        return int(s) if s else None

    def parse_bool(x):
        return str(x).strip().upper() in {"Y", "YES", "TRUE", "T", "1"}

    def norm_eir(x):
        s = str(x).strip().upper()
        return "NONE" if s in {"", "NAN", "NONE", "NULL", "NA"} else s

    df = df.copy()

    df["date"] = df["date"].map(parse_date)
    df["price_eur"] = df["price"].map(parse_price)

    df["county"] = df["county"].astype(str).str.strip().str.title()
    df["address"] = df["address"].astype(str).str.strip()
    df["eircode"] = df["eircode"].map(norm_eir)

    df["nfm"] = df["nfm"].map(parse_bool).astype(int)
    df["vat_exclusive"] = df["vat_exclusive"].map(parse_bool).astype(int)

    df["year"] = df["date"].str.slice(0, 4)
    cols = [
        "date",
        "address",
        "eircode",
        "county",
        "price_eur",
        "nfm",
        "vat_exclusive",
        "year",
        "description",
    ]
    df = df[cols]
    df = df.dropna(subset=["date"])
    return df


def write_ndjson(path: str, rows: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def sha256(path: str) -> tuple[str, int]:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest(), os.path.getsize(path)


# -------------------------
# Main
# -------------------------

def main() -> None:
    global TLS_VERIFIED
    ensure_dirs()

    token = load_or_make_token()
    base_dir = os.path.join(API_DIR, token)
    shards_dir = os.path.join(base_dir, "shards", "by-year")
    os.makedirs(shards_dir, exist_ok=True)

    # 1) Fetch the ZIP (with provenance)
    z = fetch_zip()

    # 2) Integrity check + checksum
    z_bytes = z.getvalue()
    if not zipfile.is_zipfile(io.BytesIO(z_bytes)):
        raise RuntimeError("Downloaded file is not a valid ZIP.")
    src_sha256 = hashlib.sha256(z_bytes).hexdigest()

    # 3) Parse + normalize
    raw = read_all_csvs(io.BytesIO(z_bytes))
    tidy = normalize(raw)

    # 4) Emit per-year NDJSON
    years = sorted([y for y in tidy["year"].dropna().unique() if y and y != "None"])
    manifest_shards = []
    for y in years:
        rows = (
            tidy[tidy["year"] == y]
            .drop(columns=["year"])
            .sort_values("date")
            .to_dict(orient="records")
        )
        out = os.path.join(shards_dir, f"{y}.ndjson")
        write_ndjson(out, rows)
        digest, size = sha256(out)
        manifest_shards.append(
            {"path": f"shards/by-year/{y}.ndjson", "sha256": digest, "bytes": size}
        )

    # 5) meta.json with provenance
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "updated": updated,
        "count_years": len(years),
        "years": years,
        "shards": manifest_shards,
        "source": {
            "url": PPR_ZIP_URL,
            "tls_verified": TLS_VERIFIED,
            "sha256": src_sha256,
        },
    }
    os.makedirs(base_dir, exist_ok=True)
    with open(os.path.join(base_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # 6) robots + .nojekyll at site root
    with open(os.path.join(ROOT, "robots.txt"), "w", encoding="utf-8") as f:
        f.write("User-agent: *\nDisallow: /api/\nCrawl-delay: 10\n")
    open(os.path.join(ROOT, ".nojekyll"), "a").close()

    # 7) Summary/log hint (masked token)
    preview = safe_preview(token)
    line = f"**Endpoint (masked)**: `api/v1/{preview}/meta.json`"
    print(line)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as s:
            s.write(line + "\n")
            s.write(f"- TLS verified: **{TLS_VERIFIED}**\n")
            s.write(f"- Years: {', '.join(years)}\n")

    print("DONE.")


if __name__ == "__main__":
    main()
