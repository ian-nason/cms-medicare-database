#!/usr/bin/env python3
"""
CMS Medicare Physician & Other Supplier Database Builder

Downloads CMS Physician & Other Practitioners Public Use Files (CY2012-CY2023),
harmonizes schemas across years, and produces a queryable DuckDB database.

Data source: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners
CY2012 mirror: https://data.nber.org/providerchargepuf/

Usage:
    uv run python build_database.py
    uv run python build_database.py --years 2020 2021 2022 2023
    uv run python build_database.py --skip-geo
    uv run python build_database.py --data-dir ./data
"""

import argparse
import json
import sys
import time
import zipfile
from pathlib import Path

import duckdb
import requests
from tqdm import tqdm

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DEFAULT_OUTPUT = "cms_medicare.duckdb"
URLS_FILE = "download_urls.json"

# ---------------------------------------------------------------------------
# Column mappings: source column name -> standardized column name
# ---------------------------------------------------------------------------

# data.cms.gov CSV columns (2013-2023) -> our schema
CMS_PROV_SVC_COLUMNS = {
    "Rndrng_NPI": "npi",
    "Rndrng_Prvdr_Last_Org_Name": "provider_last_name",
    "Rndrng_Prvdr_First_Name": "provider_first_name",
    "Rndrng_Prvdr_MI": "provider_mi",
    "Rndrng_Prvdr_Crdntls": "provider_credentials",
    "Rndrng_Prvdr_Gndr": "provider_gender",
    "Rndrng_Prvdr_Ent_Cd": "provider_entity_code",
    "Rndrng_Prvdr_St1": "provider_street1",
    "Rndrng_Prvdr_St2": "provider_street2",
    "Rndrng_Prvdr_City": "provider_city",
    "Rndrng_Prvdr_State_Abrvtn": "provider_state",
    "Rndrng_Prvdr_State_FIPS": "provider_state_fips",
    "Rndrng_Prvdr_Zip5": "provider_zip",
    "Rndrng_Prvdr_RUCA": "provider_ruca",
    "Rndrng_Prvdr_RUCA_Desc": "provider_ruca_desc",
    "Rndrng_Prvdr_Cntry": "provider_country",
    "Rndrng_Prvdr_Type": "provider_type",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "medicare_participation",
    "HCPCS_Cd": "hcpcs_code",
    "HCPCS_Desc": "hcpcs_description",
    "HCPCS_Drug_Ind": "hcpcs_drug_indicator",
    "Place_Of_Srvc": "place_of_service",
    "Tot_Benes": "bene_unique_cnt",
    "Tot_Srvcs": "line_srvc_cnt",
    "Tot_Bene_Day_Srvcs": "bene_day_srvc_cnt",
    "Avg_Sbmtd_Chrg": "avg_submitted_chrg_amt",
    "Avg_Mdcr_Alowd_Amt": "avg_medicare_allowed_amt",
    "Avg_Mdcr_Pymt_Amt": "avg_medicare_payment_amt",
    "Avg_Mdcr_Stdzd_Amt": "avg_medicare_standardized_amt",
}

# NBER 2012 CSV columns -> our schema
# The NBER phys2012.csv uses the old CMS naming convention
NBER_2012_COLUMNS = {
    "npi": "npi",
    "nppes_provider_last_org_name": "provider_last_name",
    "nppes_provider_first_name": "provider_first_name",
    "nppes_provider_mi": "provider_mi",
    "nppes_credentials": "provider_credentials",
    "nppes_provider_gender": "provider_gender",
    "nppes_entity_code": "provider_entity_code",
    "nppes_provider_street1": "provider_street1",
    "nppes_provider_street2": "provider_street2",
    "nppes_provider_city": "provider_city",
    "nppes_provider_state": "provider_state",
    "nppes_provider_zip": "provider_zip",
    "nppes_provider_country": "provider_country",
    "provider_type": "provider_type",
    "medicare_participation_indicator": "medicare_participation",
    "place_of_service": "place_of_service",
    "hcpcs_code": "hcpcs_code",
    "hcpcs_description": "hcpcs_description",
    "line_srvc_cnt": "line_srvc_cnt",
    "bene_unique_cnt": "bene_unique_cnt",
    "bene_day_srvc_cnt": "bene_day_srvc_cnt",
    "average_medicare_allowed_amt": "avg_medicare_allowed_amt",
    "average_submitted_chrg_amt": "avg_submitted_chrg_amt",
    "average_medicare_payment_amt": "avg_medicare_payment_amt",
}

# The canonical column order for physician_services
PHYSICIAN_SERVICES_SCHEMA = [
    ("npi", "VARCHAR"),
    ("provider_last_name", "VARCHAR"),
    ("provider_first_name", "VARCHAR"),
    ("provider_mi", "VARCHAR"),
    ("provider_credentials", "VARCHAR"),
    ("provider_gender", "VARCHAR"),
    ("provider_entity_code", "VARCHAR"),
    ("provider_street1", "VARCHAR"),
    ("provider_street2", "VARCHAR"),
    ("provider_city", "VARCHAR"),
    ("provider_state", "VARCHAR"),
    ("provider_state_fips", "VARCHAR"),
    ("provider_zip", "VARCHAR"),
    ("provider_ruca", "VARCHAR"),
    ("provider_ruca_desc", "VARCHAR"),
    ("provider_country", "VARCHAR"),
    ("provider_type", "VARCHAR"),
    ("medicare_participation", "VARCHAR"),
    ("hcpcs_code", "VARCHAR"),
    ("hcpcs_description", "VARCHAR"),
    ("hcpcs_drug_indicator", "VARCHAR"),
    ("place_of_service", "VARCHAR"),
    ("line_srvc_cnt", "DOUBLE"),
    ("bene_unique_cnt", "INTEGER"),
    ("bene_day_srvc_cnt", "INTEGER"),
    ("avg_submitted_chrg_amt", "DOUBLE"),
    ("avg_medicare_allowed_amt", "DOUBLE"),
    ("avg_medicare_payment_amt", "DOUBLE"),
    ("avg_medicare_standardized_amt", "DOUBLE"),
    ("year", "INTEGER"),
]

TABLE_DESCRIPTIONS = {
    "physician_services": "Provider-level Medicare Part B claims: one row per NPI per HCPCS code per place of service per year. Includes service counts, beneficiary counts, and average payment amounts.",
    "physician_summary": "Provider-level aggregate summary: one row per NPI per year with total services, beneficiaries, and payment amounts across all HCPCS codes.",
    "geography_service": "Geographic aggregate: Medicare utilization and payment by state/national level, HCPCS code, and place of service per year.",
}

JOIN_HINTS = {
    "npi": "National Provider Identifier, joins across all tables and to NPPES",
    "hcpcs_code": "HCPCS procedure code, joins to HCPCS/CPT lookup tables",
    "provider_state": "Two-letter state abbreviation",
    "provider_type": "Provider specialty derived from claims",
    "year": "Calendar year of service",
    "place_of_service": "F=Facility, O=Office/Non-facility",
    "provider_entity_code": "I=Individual, O=Organization",
    "provider_zip": "5-digit ZIP code",
    "provider_state_fips": "FIPS state code",
    "provider_ruca": "Rural-Urban Commuting Area code",
    "hcpcs_drug_indicator": "Y if HCPCS is on Part B Drug ASP file",
    "medicare_participation": "Y=participates in Medicare",
}


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def download_file(url: str, dest: Path, desc: str = "") -> bool:
    """Download a file with progress bar. Returns True on success."""
    if dest.exists() and dest.stat().st_size > 0:
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = requests.get(url, stream=True, timeout=120)
        if r.status_code == 404:
            print(f"    WARNING: 404 Not Found: {url}")
            return False
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f:
            with tqdm(
                total=total, unit="B", unit_scale=True,
                desc=f"    {desc or dest.name}", leave=False,
            ) as pbar:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
                    pbar.update(len(chunk))
        return True
    except Exception as e:
        print(f"    WARNING: Failed to download {url}: {e}")
        if dest.exists():
            dest.unlink()
        return False


def extract_csv_from_zip(zip_path: Path, dest_dir: Path) -> Path | None:
    """Extract the CSV/TXT file from a zip. Returns path to extracted file."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            # Find CSV or TXT file
            match = None
            for name in names:
                low = name.lower()
                if low.endswith(".csv") or low.endswith(".txt"):
                    match = name
                    break
            if not match and names:
                match = names[0]

            if match:
                dest = dest_dir / match.split("/")[-1]
                with zf.open(match) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
                return dest
    except Exception as e:
        print(f"    WARNING: Could not extract {zip_path.name}: {e}")
    return None


# ---------------------------------------------------------------------------
# Schema detection and loading
# ---------------------------------------------------------------------------


def detect_source_columns(csv_path: Path, con: duckdb.DuckDBPyConnection) -> list[str]:
    """Read the first row of a CSV to get column names."""
    result = con.execute(f"""
        SELECT * FROM read_csv('{csv_path}',
            header=true, all_varchar=true, sample_size=1
        ) LIMIT 0
    """)
    return [desc[0] for desc in result.description]


def build_rename_select(source_cols: list[str], col_mapping: dict, year: int) -> str:
    """Build a SELECT statement that renames source columns to our schema."""
    parts = []
    mapped = set()

    for src_col in source_cols:
        # Try exact match first
        target = col_mapping.get(src_col)
        if not target:
            # Try case-insensitive match
            for k, v in col_mapping.items():
                if k.lower() == src_col.lower():
                    target = v
                    break

        if target and target not in mapped:
            parts.append(f'"{src_col}" AS {target}')
            mapped.add(target)

    # Add NULL for any schema columns not present in source
    all_target_cols = {v for v in col_mapping.values()}
    # Also check our canonical schema for columns that might not be in the mapping
    for col_name, col_type in PHYSICIAN_SERVICES_SCHEMA:
        if col_name == "year":
            continue
        if col_name not in mapped:
            parts.append(f"NULL AS {col_name}")

    parts.append(f"{year}::INTEGER AS year")
    return ", ".join(parts)


def load_cms_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: Path,
    year: int,
    col_mapping: dict,
    create_table: bool = False,
) -> int:
    """Load a CMS CSV file into DuckDB with column renaming. Returns row count."""
    source_cols = detect_source_columns(csv_path, con)
    select_expr = build_rename_select(source_cols, col_mapping, year)

    if create_table:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT {select_expr}
            FROM read_csv('{csv_path}',
                header=true, all_varchar=true, ignore_errors=true,
                null_padding=true, parallel=true
            )
        """)
    else:
        con.execute(f"""
            INSERT INTO {table_name} BY NAME
            SELECT {select_expr}
            FROM read_csv('{csv_path}',
                header=true, all_varchar=true, ignore_errors=true,
                null_padding=true, parallel=true
            )
        """)

    count = con.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE year = {year}"
    ).fetchone()[0]
    return count


def load_provider_aggregate(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    year: int,
    first: bool = False,
) -> int:
    """Load a provider aggregate CSV into DuckDB. Returns row count."""
    # Read as-is with all_varchar, add year column
    if first:
        con.execute("DROP TABLE IF EXISTS physician_summary")
        con.execute(f"""
            CREATE TABLE physician_summary AS
            SELECT *, {year}::INTEGER AS year
            FROM read_csv('{csv_path}',
                header=true, all_varchar=true, ignore_errors=true,
                null_padding=true, parallel=true
            )
        """)
    else:
        # Use INSERT BY NAME to handle schema differences across years
        try:
            con.execute(f"""
                INSERT INTO physician_summary BY NAME
                SELECT *, {year}::INTEGER AS year
                FROM read_csv('{csv_path}',
                    header=true, all_varchar=true, ignore_errors=true,
                    null_padding=true, parallel=true
                )
            """)
        except Exception:
            # Schema mismatch — add missing columns
            temp_cols = con.execute(f"""
                SELECT column_name FROM (
                    DESCRIBE SELECT * FROM read_csv('{csv_path}',
                        header=true, all_varchar=true, ignore_errors=true,
                        null_padding=true, parallel=true
                    )
                )
            """).fetchall()
            existing = {
                r[0] for r in con.execute("DESCRIBE physician_summary").fetchall()
            }
            for (col,) in temp_cols:
                if col not in existing:
                    con.execute(
                        f'ALTER TABLE physician_summary ADD COLUMN "{col}" VARCHAR'
                    )
            # Retry
            con.execute(f"""
                INSERT INTO physician_summary BY NAME
                SELECT *, {year}::INTEGER AS year
                FROM read_csv('{csv_path}',
                    header=true, all_varchar=true, ignore_errors=true,
                    null_padding=true, parallel=true
                )
            """)

    count = con.execute(
        f"SELECT COUNT(*) FROM physician_summary WHERE year = {year}"
    ).fetchone()[0]
    return count


def load_geography_csv(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    year: int,
    first: bool = False,
) -> int:
    """Load a geography and service CSV into DuckDB. Returns row count."""
    if first:
        con.execute("DROP TABLE IF EXISTS geography_service")
        con.execute(f"""
            CREATE TABLE geography_service AS
            SELECT *, {year}::INTEGER AS year
            FROM read_csv('{csv_path}',
                header=true, all_varchar=true, ignore_errors=true,
                null_padding=true, parallel=true
            )
        """)
    else:
        try:
            con.execute(f"""
                INSERT INTO geography_service BY NAME
                SELECT *, {year}::INTEGER AS year
                FROM read_csv('{csv_path}',
                    header=true, all_varchar=true, ignore_errors=true,
                    null_padding=true, parallel=true
                )
            """)
        except Exception:
            temp_cols = con.execute(f"""
                SELECT column_name FROM (
                    DESCRIBE SELECT * FROM read_csv('{csv_path}',
                        header=true, all_varchar=true, ignore_errors=true,
                        null_padding=true, parallel=true
                    )
                )
            """).fetchall()
            existing = {
                r[0] for r in con.execute("DESCRIBE geography_service").fetchall()
            }
            for (col,) in temp_cols:
                if col not in existing:
                    con.execute(
                        f'ALTER TABLE geography_service ADD COLUMN "{col}" VARCHAR'
                    )
            con.execute(f"""
                INSERT INTO geography_service BY NAME
                SELECT *, {year}::INTEGER AS year
                FROM read_csv('{csv_path}',
                    header=true, all_varchar=true, ignore_errors=true,
                    null_padding=true, parallel=true
                )
            """)

    count = con.execute(
        f"SELECT COUNT(*) FROM geography_service WHERE year = {year}"
    ).fetchone()[0]
    return count


# ---------------------------------------------------------------------------
# Type casting
# ---------------------------------------------------------------------------


def cast_physician_services(con: duckdb.DuckDBPyConnection):
    """Cast physician_services columns from VARCHAR to proper types."""
    casts = {
        "line_srvc_cnt": "DOUBLE",
        "bene_unique_cnt": "INTEGER",
        "bene_day_srvc_cnt": "INTEGER",
        "avg_submitted_chrg_amt": "DOUBLE",
        "avg_medicare_allowed_amt": "DOUBLE",
        "avg_medicare_payment_amt": "DOUBLE",
        "avg_medicare_standardized_amt": "DOUBLE",
    }
    for col, dtype in casts.items():
        try:
            con.execute(f"""
                ALTER TABLE physician_services
                ALTER COLUMN {col} SET DATA TYPE {dtype}
                USING TRY_CAST({col} AS {dtype})
            """)
        except Exception as e:
            print(f"    WARNING: Could not cast {col} to {dtype}: {e}")


def cast_numeric_columns(con: duckdb.DuckDBPyConnection, table_name: str):
    """Auto-detect and cast numeric-looking VARCHAR columns."""
    cols = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND data_type = 'VARCHAR'
    """).fetchall()

    for col_name, _ in cols:
        if col_name == "year":
            continue
        # Sample values to detect if numeric
        try:
            result = con.execute(f"""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL) AS numeric_cnt
                FROM (SELECT "{col_name}" FROM {table_name} WHERE "{col_name}" IS NOT NULL LIMIT 1000)
            """).fetchone()
            total, numeric_cnt = result
            if total > 0 and numeric_cnt / total > 0.9:
                # Check if integer
                int_result = con.execute(f"""
                    SELECT COUNT(*) FILTER (
                        WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
                          AND TRY_CAST("{col_name}" AS DOUBLE) = ROUND(TRY_CAST("{col_name}" AS DOUBLE))
                    ) FROM (SELECT "{col_name}" FROM {table_name} WHERE "{col_name}" IS NOT NULL LIMIT 1000)
                """).fetchone()[0]
                target_type = "BIGINT" if int_result == numeric_cnt else "DOUBLE"
                con.execute(f"""
                    ALTER TABLE {table_name}
                    ALTER COLUMN "{col_name}" SET DATA TYPE {target_type}
                    USING TRY_CAST("{col_name}" AS {target_type})
                """)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Metadata and dictionary
# ---------------------------------------------------------------------------


def build_metadata(con: duckdb.DuckDBPyConnection, tables_built: set):
    """Build the _metadata table."""
    con.execute("DROP TABLE IF EXISTS _metadata")
    con.execute("""
        CREATE TABLE _metadata (
            table_name VARCHAR,
            description VARCHAR,
            row_count BIGINT,
            column_count INTEGER,
            source_url VARCHAR,
            license VARCHAR
        )
    """)

    for table_name in sorted(tables_built):
        row_count = con.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]
        col_count = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        ).fetchone()[0]
        desc = TABLE_DESCRIPTIONS.get(table_name, "")
        con.execute(
            "INSERT INTO _metadata VALUES (?, ?, ?, ?, ?, ?)",
            [
                table_name, desc, row_count, col_count,
                "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners",
                "Public domain (U.S. government work). HCPCS descriptions include AMA CPT content used under CMS license.",
            ],
        )

    # Print summary
    rows = con.execute(
        "SELECT table_name, row_count, column_count FROM _metadata ORDER BY row_count DESC"
    ).fetchall()
    total = sum(r[1] for r in rows)
    print(f"\n  {len(rows)} tables, {total:,} total rows\n")
    for name, rc, cc in rows:
        print(f"    {name:<25s} {rc:>14,} rows  ({cc} cols)")


def build_columns_table(con: duckdb.DuckDBPyConnection):
    """Build the _columns data dictionary table."""
    con.execute("DROP TABLE IF EXISTS _columns")

    con.execute("""
        CREATE TABLE _columns AS
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            NULL::VARCHAR AS source_file
        FROM information_schema.columns c
        WHERE c.table_schema = 'main'
          AND c.table_name NOT IN ('_metadata', '_columns')
    """)

    con.execute("ALTER TABLE _columns ADD COLUMN example_value VARCHAR")
    con.execute("ALTER TABLE _columns ADD COLUMN join_hint VARCHAR")
    con.execute("ALTER TABLE _columns ADD COLUMN null_pct DOUBLE")

    # Apply join hints
    for col, hint in JOIN_HINTS.items():
        con.execute(
            "UPDATE _columns SET join_hint = ? WHERE column_name = ?",
            [hint, col],
        )

    # Populate example_value and null_pct
    rows = con.execute(
        "SELECT table_name, column_name FROM _columns"
    ).fetchall()

    for table_name, column_name in tqdm(rows, desc="    Enriching _columns", leave=False):
        try:
            result = con.execute(
                f'SELECT CAST("{column_name}" AS VARCHAR) '
                f'FROM "{table_name}" '
                f'WHERE "{column_name}" IS NOT NULL LIMIT 1'
            ).fetchone()
            if result:
                val = result[0]
                if len(val) > 80:
                    val = val[:77] + "..."
                con.execute(
                    "UPDATE _columns SET example_value = ? "
                    "WHERE table_name = ? AND column_name = ?",
                    [val, table_name, column_name],
                )
        except Exception:
            pass

        try:
            result = con.execute(
                f'SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE "{column_name}" IS NULL) '
                f'/ COUNT(*), 1) FROM "{table_name}"'
            ).fetchone()
            if result and result[0] is not None:
                con.execute(
                    "UPDATE _columns SET null_pct = ? "
                    "WHERE table_name = ? AND column_name = ?",
                    [result[0], table_name, column_name],
                )
        except Exception:
            pass


def export_dictionary(con: duckdb.DuckDBPyConnection, output_path: Path):
    """Export _columns and _metadata as a readable DICTIONARY.md file."""
    lines = []
    lines.append("# Data Dictionary")
    lines.append("")
    lines.append("Source: [CMS Medicare Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)")
    lines.append("")

    tables = con.execute(
        "SELECT DISTINCT table_name FROM _columns ORDER BY table_name"
    ).fetchall()

    for (table_name,) in tables:
        meta = con.execute(
            "SELECT row_count, description FROM _metadata WHERE table_name = ?",
            [table_name],
        ).fetchone()

        lines.append(f"## {table_name}")
        lines.append("")
        if meta:
            row_count, description = meta
            if description:
                lines.append(f"{description}")
                lines.append("")
            if row_count:
                lines.append(f"Rows: {row_count:,}")
            lines.append("")

        lines.append("| Column | Type | Nulls | Example | Join |")
        lines.append("|--------|------|-------|---------|------|")

        cols = con.execute(
            "SELECT column_name, data_type, null_pct, example_value, join_hint "
            "FROM _columns WHERE table_name = ? ORDER BY rowid",
            [table_name],
        ).fetchall()

        for col_name, dtype, null_pct, example, join_hint in cols:
            null_str = f"{null_pct:.1f}%" if null_pct is not None else ""
            example_str = (example or "").replace("|", "\\|")
            join_str = join_hint or ""
            lines.append(
                f"| {col_name} | {dtype} | {null_str} | {example_str} | {join_str} |"
            )

        lines.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    print(f"  Exported to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Build DuckDB from CMS Medicare Physician & Other Supplier PUF data"
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT))
    parser.add_argument("--urls", type=Path, default=Path(URLS_FILE))
    parser.add_argument("--years", type=int, nargs="+",
                        help="Only build specific years (default: all available)")
    parser.add_argument("--skip-geo", action="store_true",
                        help="Skip geography_service table")
    parser.add_argument("--skip-summary", action="store_true",
                        help="Skip physician_summary table")
    args = parser.parse_args()

    t_start = time.time()

    print("=" * 60)
    print("CMS Medicare Physician & Other Supplier Database Builder")
    print("=" * 60)

    # Load URLs
    with open(args.urls) as f:
        urls = json.load(f)

    # Determine years to build
    prov_svc_urls = urls["provider_and_service"]["files"]
    nber_urls = urls.get("provider_and_service_nber", {}).get("files", {})
    all_years = sorted(set(list(prov_svc_urls.keys()) + list(nber_urls.keys())))

    if args.years:
        all_years = [str(y) for y in args.years if str(y) in all_years]

    print(f"  Years: {', '.join(all_years)}")
    print(f"  Output: {args.output}")

    # ------------------------------------------------------------------
    # Step 1: Download and load physician_services
    # ------------------------------------------------------------------
    print(f"\n[1/7] Building physician_services table")
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.output.unlink(missing_ok=True)
    con = duckdb.connect(str(args.output))
    tables_built = set()
    first_table = True

    for year_str in all_years:
        year = int(year_str)
        year_dir = args.data_dir / year_str
        year_dir.mkdir(parents=True, exist_ok=True)

        # Determine source: NBER for 2012, data.cms.gov for 2013+
        if year_str in nber_urls and year_str not in prov_svc_urls:
            # NBER ZIP source (2012)
            url = nber_urls[year_str]
            zip_dest = year_dir / f"phys{year_str}.csv.zip"
            print(f"\n  CY{year_str} (NBER mirror):")
            if not download_file(url, zip_dest, f"phys{year_str}.csv.zip"):
                print(f"    SKIPPED: download failed")
                continue
            csv_path = extract_csv_from_zip(zip_dest, year_dir)
            if not csv_path:
                print(f"    SKIPPED: extraction failed")
                continue
            col_mapping = NBER_2012_COLUMNS
        else:
            # data.cms.gov CSV source (2013+)
            url = prov_svc_urls[year_str]
            csv_dest = year_dir / f"prov_svc_{year_str}.csv"
            print(f"\n  CY{year_str} (data.cms.gov):")
            if not download_file(url, csv_dest, f"prov_svc_{year_str}.csv"):
                print(f"    SKIPPED: download failed")
                continue
            csv_path = csv_dest
            col_mapping = CMS_PROV_SVC_COLUMNS

        # Load into DuckDB
        try:
            n = load_cms_csv(
                con, "physician_services", csv_path, year,
                col_mapping, create_table=first_table,
            )
            first_table = False
            print(f"    -> {n:,} rows loaded")
        except Exception as e:
            print(f"    ERROR loading CY{year_str}: {e}")
            import traceback
            traceback.print_exc()
            continue
        finally:
            # Clean up downloaded file to save disk space
            if csv_path and csv_path.exists():
                csv_path.unlink()
            zip_dest_path = year_dir / f"phys{year_str}.csv.zip"
            if zip_dest_path.exists():
                zip_dest_path.unlink()

    if not first_table:
        tables_built.add("physician_services")
        total = con.execute("SELECT COUNT(*) FROM physician_services").fetchone()[0]
        print(f"\n  physician_services total: {total:,} rows")

    # ------------------------------------------------------------------
    # Step 2: Cast physician_services types
    # ------------------------------------------------------------------
    if "physician_services" in tables_built:
        print(f"\n[2/7] Casting physician_services column types")
        cast_physician_services(con)
        print("  Done")

    # ------------------------------------------------------------------
    # Step 3: Load physician_summary (aggregate by provider)
    # ------------------------------------------------------------------
    if not args.skip_summary:
        print(f"\n[3/7] Building physician_summary table")
        agg_urls = urls.get("provider_aggregate", {}).get("files", {})
        nber_agg_urls = urls.get("provider_aggregate_nber", {}).get("files", {})
        agg_years = sorted(set(list(agg_urls.keys()) + list(nber_agg_urls.keys())))
        if args.years:
            agg_years = [str(y) for y in args.years if str(y) in agg_years]

        first_agg = True
        for year_str in agg_years:
            year = int(year_str)
            year_dir = args.data_dir / year_str
            year_dir.mkdir(parents=True, exist_ok=True)

            if year_str in nber_agg_urls and year_str not in agg_urls:
                url = nber_agg_urls[year_str]
                csv_dest = year_dir / f"prov_agg_{year_str}.csv"
                print(f"  CY{year_str} (NBER):", end=" ")
            else:
                url = agg_urls[year_str]
                csv_dest = year_dir / f"prov_agg_{year_str}.csv"
                print(f"  CY{year_str}:", end=" ")

            if not download_file(url, csv_dest, f"prov_agg_{year_str}.csv"):
                print("SKIPPED")
                continue

            try:
                n = load_provider_aggregate(con, csv_dest, year, first=first_agg)
                first_agg = False
                print(f"{n:,} rows")
            except Exception as e:
                print(f"ERROR: {e}")
            finally:
                if csv_dest.exists():
                    csv_dest.unlink()

        if not first_agg:
            tables_built.add("physician_summary")
            print(f"\n  Casting physician_summary types")
            cast_numeric_columns(con, "physician_summary")
    else:
        print(f"\n[3/7] Skipping physician_summary (--skip-summary)")

    # ------------------------------------------------------------------
    # Step 4: Load geography_service
    # ------------------------------------------------------------------
    if not args.skip_geo:
        print(f"\n[4/7] Building geography_service table")
        geo_urls = urls.get("geography_and_service", {}).get("files", {})
        geo_years = sorted(geo_urls.keys())
        if args.years:
            geo_years = [str(y) for y in args.years if str(y) in geo_years]

        first_geo = True
        for year_str in geo_years:
            year = int(year_str)
            year_dir = args.data_dir / year_str
            year_dir.mkdir(parents=True, exist_ok=True)

            url = geo_urls[year_str]
            csv_dest = year_dir / f"geo_{year_str}.csv"
            print(f"  CY{year_str}:", end=" ")

            if not download_file(url, csv_dest, f"geo_{year_str}.csv"):
                print("SKIPPED")
                continue

            try:
                n = load_geography_csv(con, csv_dest, year, first=first_geo)
                first_geo = False
                print(f"{n:,} rows")
            except Exception as e:
                print(f"ERROR: {e}")
            finally:
                if csv_dest.exists():
                    csv_dest.unlink()

        if not first_geo:
            tables_built.add("geography_service")
            print(f"\n  Casting geography_service types")
            cast_numeric_columns(con, "geography_service")
    else:
        print(f"\n[4/7] Skipping geography_service (--skip-geo)")

    # ------------------------------------------------------------------
    # Step 5: Build _metadata
    # ------------------------------------------------------------------
    print(f"\n[5/7] Building _metadata")
    build_metadata(con, tables_built)

    # ------------------------------------------------------------------
    # Step 6: Build _columns data dictionary
    # ------------------------------------------------------------------
    print(f"\n[6/7] Building _columns data dictionary")
    build_columns_table(con)
    col_count = con.execute("SELECT COUNT(*) FROM _columns").fetchone()[0]
    print(f"  {col_count} columns cataloged")

    # ------------------------------------------------------------------
    # Step 7: Export DICTIONARY.md
    # ------------------------------------------------------------------
    print(f"\n[7/7] Exporting DICTIONARY.md")
    export_dictionary(con, args.output.parent / "DICTIONARY.md")

    con.close()

    elapsed = time.time() - t_start
    size_mb = args.output.stat().st_size / (1024**2)
    print(f"\nDone in {int(elapsed // 60)}m {int(elapsed % 60)}s")
    print(f"Database: {args.output.resolve()} ({size_mb:,.0f} MB / {size_mb/1024:.1f} GB)")


if __name__ == "__main__":
    main()
