"""Re-ingest physician_summary and geography_service with correct typing
(July 2026 audit): the numeric auto-cast NULLed alphanumeric HCPCS codes and
stripped leading zeros from NPI/ZIP/FIPS. Re-downloads the source CSVs
(deleted after the original build), reloads both tables, and re-runs the
fixed cast_numeric_columns plus metadata/dictionary regeneration.
physician_services (116M rows) is untouched — its typing was correct."""
import json
from pathlib import Path

import duckdb

from build_database import (
    build_columns_table,
    build_metadata,
    cast_numeric_columns,
    download_file,
    export_dictionary,
    load_geography_csv,
    load_provider_aggregate,
)

DATA_DIR = Path("data")
DB_PATH = Path("cms_medicare.duckdb")

urls = json.loads(Path("download_urls.json").read_text())
con = duckdb.connect(str(DB_PATH))
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{DB_PATH.resolve()}.tmp'")
con.execute("SET memory_limit = '8GB'")

prev_summary = con.execute("SELECT COUNT(*) FROM physician_summary").fetchone()[0]
prev_geo = con.execute("SELECT COUNT(*) FROM geography_service").fetchone()[0]
print(f"Previous counts: summary {prev_summary:,}, geo {prev_geo:,}")

for key, loader, table, fname in [
    ("provider_aggregate", load_provider_aggregate, "physician_summary", "prov_agg"),
    ("geography_and_service", load_geography_csv, "geography_service", "geo"),
]:
    files = urls[key]["files"]
    # NBER supplements fill early years for the aggregate table
    nber = urls.get(f"{key}_nber", {}).get("files", {})
    years = sorted(set(files) | set(nber))
    first = True
    print(f"\nReloading {table} ({len(years)} years)")
    for year_str in years:
        url = files.get(year_str) or nber[year_str]
        year_dir = DATA_DIR / year_str
        year_dir.mkdir(parents=True, exist_ok=True)
        dest = year_dir / f"{fname}_{year_str}.csv"
        if not download_file(url, dest, dest.name):
            print(f"  CY{year_str}: SKIPPED (download failed)")
            continue
        try:
            n = loader(con, dest, int(year_str), first=first)
            first = False
            print(f"  CY{year_str}: {n:,} rows", flush=True)
        finally:
            if dest.exists():
                dest.unlink()
    cast_numeric_columns(con, table)

new_summary = con.execute("SELECT COUNT(*) FROM physician_summary").fetchone()[0]
new_geo = con.execute("SELECT COUNT(*) FROM geography_service").fetchone()[0]
print(f"\nNew counts: summary {new_summary:,} (was {prev_summary:,}), "
      f"geo {new_geo:,} (was {prev_geo:,})")
assert new_summary >= prev_summary and new_geo >= prev_geo

print("\nVerifying audit defects are gone:")
t = dict(con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = 'geography_service'"
).fetchall())
print(f"  geography_service.HCPCS_Cd type: {t.get('HCPCS_Cd')}")
assert t.get("HCPCS_Cd") == "VARCHAR"
null_codes = con.execute(
    "SELECT COUNT(*) FROM geography_service WHERE HCPCS_Cd IS NULL"
).fetchone()[0]
alpha = con.execute(
    "SELECT COUNT(*) FROM geography_service WHERE regexp_matches(HCPCS_Cd, '[A-Z]')"
).fetchone()[0]
lead0 = con.execute(
    "SELECT COUNT(*) FROM geography_service WHERE HCPCS_Cd LIKE '0%'"
).fetchone()[0]
print(f"  NULL HCPCS_Cd: {null_codes:,} (was 282,776); alphanumeric: {alpha:,}; leading-zero: {lead0:,}")
assert null_codes < 1000 and alpha > 100000 and lead0 > 50000

s = dict(con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = 'physician_summary'"
).fetchall())
for c in ("Rndrng_NPI", "Rndrng_Prvdr_Zip5", "Rndrng_Prvdr_State_FIPS", "Rndrng_Prvdr_RUCA"):
    print(f"  physician_summary.{c}: {s.get(c)}")
    assert s.get(c) in ("VARCHAR", None)
zip_ok = con.execute(
    "SELECT COUNT(*) FROM physician_summary WHERE Rndrng_Prvdr_Zip5 LIKE '0%'"
).fetchone()[0]
join_ok = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT s.Rndrng_NPI FROM physician_summary s
        JOIN physician_services p ON s.Rndrng_NPI = p.npi AND s.year = p.year
        LIMIT 1000
    )
""").fetchone()[0]
print(f"  leading-zero ZIPs: {zip_ok:,}; NPI join to services works: {join_ok} rows")
assert zip_ok > 100000 and join_ok == 1000

print("\nRegenerating metadata, _columns, DICTIONARY")
build_metadata(con, {"physician_services", "physician_summary", "geography_service"})
build_columns_table(con)
export_dictionary(con, Path("DICTIONARY.md"))
con.execute("CHECKPOINT")
con.close()
print("CMS TYPE REPAIR DONE")
