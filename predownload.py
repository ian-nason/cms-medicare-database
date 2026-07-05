"""Pre-download all CMS CSVs into the layout build_database.py expects.

build_database.py's download_file() skips files that already exist, so this
lets the download (network-bound) overlap with other builds (memory-bound).
"""
import json
from pathlib import Path

import requests

DATA_DIR = Path(__file__).parent / "data"
URLS = json.loads((Path(__file__).parent / "download_urls.json").read_text())

TARGETS = [
    ("provider_and_service", "prov_svc_{y}.csv"),
    ("provider_aggregate", "prov_agg_{y}.csv"),
    ("geography_and_service", "geo_{y}.csv"),
]

for section, pattern in TARGETS:
    files = URLS.get(section, {}).get("files", {})
    for year, url in sorted(files.items()):
        dest = DATA_DIR / year / pattern.format(y=year)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.stat().st_size > 0:
            print(f"SKIP {dest.name} (exists)", flush=True)
            continue
        print(f"GET  {dest.name} ...", flush=True)
        tmp = dest.with_suffix(".part")
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            tmp.rename(dest)
            print(f"  -> {dest.stat().st_size / 1e9:.2f} GB", flush=True)
        except Exception as e:
            print(f"  FAILED: {e}", flush=True)
            tmp.unlink(missing_ok=True)

print("ALL DOWNLOADS DONE", flush=True)
