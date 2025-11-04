TLS_VERIFIED = False  # set to False if we use insecure fallback
import subprocess, tempfile
import certifi
from requests.exceptions import SSLError, RequestException
import os, io, json, re, zipfile, hashlib, uuid, glob, time
from datetime import datetime, timezone
import requests
import pandas as pd
from dateutil import parser as dateparser

PPR_ZIP_URL = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

ROOT = os.getcwd()
DATA_DIR = os.path.join(ROOT, "data")
API_DIR = os.path.join(ROOT, "api", "v1")
TOKEN_FILE = os.path.join(DATA_DIR, "endpoint_token.txt")

# columns mapping (handles old/new price labels)
COLMAP = {
    "Date of Sale (dd/mm/yyyy)": "date",
    "Address": "address",
    "Postal Code": "eircode",
    "County": "county",
    "Price (€)": "price",
    "Price (�)": "price",
    "Not Full Market Price": "nfm",
    "VAT Exclusive": "vat_exclusive",
    "Description of Property": "description"
}

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(API_DIR, exist_ok=True)

def load_or_make_token():
    tok = os.environ.get("ENDPOINT_TOKEN")
    if tok:
        return tok.strip()
    # fallback: generate and persist (note: visible in repo if committed)
    if os.path.exists(TOKEN_FILE):
        return open(TOKEN_FILE, "r").read().strip()
    t = uuid.uuid4().hex
    with open(TOKEN_FILE, "w") as f:
        f.write(t)
    return t

def fetch_zip():
    """Secure-first download with explicit CA and controlled fallback."""
    ca = certifi.where()
    headers = {"User-Agent": "ppr-ingest/1.0 (+https://github.com/<you>/ppr-api)"}

    # Primary: requests with explicit CA
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
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            subprocess.run(
                ["curl","--fail","--location","--retry","5","--retry-all-errors",
                 "--max-time","180","--cacert",ca,"-o",tmp_path,PPR_ZIP_URL],
                check=True
            )
            data = open(tmp_path,"rb").read()
            os.unlink(tmp_path)
            return io.BytesIO(data)
        except subprocess.CalledProcessError:
            if os.environ.get("ALLOW_INSECURE_FETCH") == "1":
                print("curl --insecure fallback enabled by ALLOW_INSECURE_FETCH=1")
                subprocess.run(
                    ["curl","--fail","--location","--retry","5","--retry-all-errors",
                     "--max-time","180","-k","-o",tmp_path,PPR_ZIP_URL],
                    check=True
                )
                data = open(tmp_path,"rb").read()
                os.unlink(tmp_path)
                return io.BytesIO(data)
            raise
    except RequestException as e:
        print(f"requests error: {e}")
        raise

def read_all_csvs(zbytes):
    frames = []
    with zipfile.ZipFile(zbytes) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                with zf.open(name) as f:
                    df = pd.read_csv(f, encoding="latin-1")
                df = df.rename(columns={c: COLMAP.get(c, c) for c in df.columns})
                keep = ["date","address","eircode","county","price","nfm","vat_exclusive","description"]
                for k in keep:
                    if k not in df.columns:
                        df[k] = None
                frames.append(df[keep])
    if not frames:
        raise RuntimeError("No CSV files found in ZIP.")
    return pd.concat(frames, ignore_index=True)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # date to ISO (yyyy-mm-dd)
    def parse_date(x):
        try:
            return dateparser.parse(str(x), dayfirst=True).date().isoformat()
        except Exception:
            return None
    df["date"] = df["date"].map(parse_date)

    # price -> int euros
    def parse_price(x):
        s = re.sub(r"[^\d]", "", str(x))
        return int(s) if s else None
    df["price_eur"] = df["price"].map(parse_price)

    df["county"] = df["county"].astype(str).str.strip().str.title()
    df["eircode"] = df["eircode"].astype(str).str.strip().str.upper().replace({"nan": None})
    df["address"] = df["address"].astype(str).str.strip()
    df["nfm"] = df["nfm"].astype(str).str.upper().isin(["Y","YES","TRUE","T"]).astype(int)
    df["vat_exclusive"] = df["vat_exclusive"].astype(str).str.upper().isin(["Y","YES","TRUE","T"]).astype(int)

    # add year for sharding
    df["year"] = df["date"].str.slice(0,4)
    # keep a compact set
    cols = ["date","address","eircode","county","price_eur","nfm","vat_exclusive","year","description"]
    return df[cols].dropna(subset=["date"])

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(1<<20), b''):
            h.update(chunk)
    return h.hexdigest(), os.path.getsize(path)

def main():
    ensure_dirs()
    token = load_or_make_token()
    base = os.path.join(API_DIR, token)
    shards_dir = os.path.join(base, "shards", "by-year")
    os.makedirs(shards_dir, exist_ok=True)

    # fetch + parse
    z = fetch_zip()
    raw = read_all_csvs(z)
    df = normalize(raw)

    # write per-year shards
    years = sorted([y for y in df["year"].dropna().unique() if y and y != "None"])
    manifest_shards = []
    for y in years:
        year_rows = df[df["year"] == y].drop(columns=["year"]).sort_values("date")
        out = os.path.join(shards_dir, f"{y}.ndjson")
        write_ndjson(out, year_rows.to_dict(orient="records"))
        digest, size = sha256(out)
        manifest_shards.append({"path": f"shards/by-year/{y}.ndjson", "sha256": digest, "bytes": size})

    # meta.json
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "updated": updated,
        "count_years": len(years),
        "years": years,
        "shards": manifest_shards
    }
    with open(os.path.join(base, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # robots + nojekyll at site root
    with open(os.path.join(ROOT, "robots.txt"), "w") as f:
        f.write("User-agent: *\nDisallow: /api/\nCrawl-delay: 10\n")
    open(os.path.join(ROOT, ".nojekyll"), "a").close()

    print(f"DONE. Endpoint token: {token}")
    print(f"META: api/v1/{token}/meta.json")

if __name__ == "__main__":
    main()
