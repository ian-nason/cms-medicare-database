#!/usr/bin/env python3
"""Validate the CMS Medicare Physician & Other Supplier DuckDB database.

Runs a series of checks to ensure the database was built correctly.

Usage:
    python validate_database.py
    python validate_database.py --db cms_medicare.duckdb
"""

import argparse
import sys
from pathlib import Path

import duckdb


def run_check(name, passed, detail=""):
    """Print a check result."""
    status = "PASS" if passed else "FAIL"
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{status}] {name}{suffix}")
    return passed


def main():
    parser = argparse.ArgumentParser(
        description="Validate the CMS Medicare DuckDB database"
    )
    parser.add_argument(
        "--db", type=Path, default=Path("cms_medicare.duckdb"),
        help="Path to the database file",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: Database not found at {args.db}")
        sys.exit(1)

    print(f"Validating {args.db}\n")

    con = duckdb.connect(str(args.db), read_only=True)
    failures = 0

    # 1. Check database opens and has tables
    all_tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    table_names = [t[0] for t in all_tables]
    data_tables = [t for t in table_names if not t.startswith("_")]

    passed = len(data_tables) > 0
    failures += 0 if run_check(
        "Database has data tables", passed, f"{len(data_tables)} tables"
    ) else 1

    # 2. Check expected tables exist
    expected = ["physician_services"]
    missing = [t for t in expected if t not in table_names]
    passed = len(missing) == 0
    detail = f"missing: {', '.join(missing)}" if missing else "all present"
    failures += 0 if run_check(
        "Required tables present", passed, detail
    ) else 1

    # 3. Check physician_services has data
    if "physician_services" in table_names:
        total = con.execute("SELECT COUNT(*) FROM physician_services").fetchone()[0]
        passed = total > 50_000_000
        failures += 0 if run_check(
            "physician_services has substantial data", passed, f"{total:,} rows"
        ) else 1

    # 4. Check all expected years present
    if "physician_services" in table_names:
        years = con.execute(
            "SELECT DISTINCT year FROM physician_services ORDER BY year"
        ).fetchall()
        year_list = [y[0] for y in years]
        passed = len(year_list) >= 10
        failures += 0 if run_check(
            "Multiple years present", passed,
            f"{len(year_list)} years: {min(year_list)}-{max(year_list)}"
        ) else 1

    # 5. Row counts per year (should be ~9-10M each)
    if "physician_services" in table_names:
        year_counts = con.execute(
            "SELECT year, COUNT(*) AS n FROM physician_services GROUP BY year ORDER BY year"
        ).fetchall()
        print("\n  Rows per year:")
        min_count = float("inf")
        max_count = 0
        for year, count in year_counts:
            print(f"    CY{year}: {count:>12,}")
            min_count = min(min_count, count)
            max_count = max(max_count, count)

        # Check counts aren't wildly different (within 3x)
        if min_count > 0:
            ratio = max_count / min_count
            passed = ratio < 3.0
            failures += 0 if run_check(
                "Cross-year row count consistency", passed,
                f"ratio {ratio:.1f}x (min {min_count:,}, max {max_count:,})"
            ) else 1

    # 6. Key columns not null
    if "physician_services" in table_names:
        for col in ["npi", "hcpcs_code", "year"]:
            null_count = con.execute(
                f"SELECT COUNT(*) FROM physician_services WHERE {col} IS NULL"
            ).fetchone()[0]
            passed = null_count == 0
            failures += 0 if run_check(
                f"No nulls in {col}", passed, f"{null_count:,} nulls"
            ) else 1

    # 7. Payment amounts are positive
    if "physician_services" in table_names:
        neg_count = con.execute("""
            SELECT COUNT(*) FROM physician_services
            WHERE avg_medicare_payment_amt < 0
        """).fetchone()[0]
        total_payments = con.execute(
            "SELECT COUNT(*) FROM physician_services WHERE avg_medicare_payment_amt IS NOT NULL"
        ).fetchone()[0]
        pct = (neg_count / total_payments * 100) if total_payments > 0 else 0
        passed = pct < 1.0  # Less than 1% negative is acceptable
        failures += 0 if run_check(
            "Payment amounts mostly positive", passed,
            f"{neg_count:,} negative ({pct:.2f}%)"
        ) else 1

    # 8. Top 10 provider types are reasonable
    if "physician_services" in table_names:
        print("\n  Top 10 provider types:")
        top_types = con.execute("""
            SELECT provider_type, COUNT(*) AS n
            FROM physician_services
            GROUP BY provider_type
            ORDER BY n DESC LIMIT 10
        """).fetchall()
        for ptype, count in top_types:
            print(f"    {ptype:<45s} {count:>12,}")
        passed = len(top_types) >= 5
        failures += 0 if run_check(
            "Multiple provider types", passed, f"{len(top_types)} types in top 10"
        ) else 1

    # 9. Check _metadata table
    passed = "_metadata" in table_names
    meta_count = 0
    if passed:
        meta_count = con.execute("SELECT COUNT(*) FROM _metadata").fetchone()[0]
        passed = meta_count > 0
    failures += 0 if run_check(
        "_metadata table exists and populated", passed,
        f"{meta_count} entries" if meta_count else "",
    ) else 1

    # 10. Check _columns table
    passed = "_columns" in table_names
    col_count = 0
    if passed:
        col_count = con.execute("SELECT COUNT(*) FROM _columns").fetchone()[0]
        passed = col_count > 0
    failures += 0 if run_check(
        "_columns table exists and populated", passed,
        f"{col_count} entries" if col_count else "",
    ) else 1

    # 11. Check join hints populated
    if "_columns" in table_names:
        hint_count = con.execute(
            "SELECT COUNT(*) FROM _columns WHERE join_hint IS NOT NULL"
        ).fetchone()[0]
        passed = hint_count > 0
        failures += 0 if run_check(
            "Join hints populated in _columns", passed,
            f"{hint_count} columns with hints",
        ) else 1

    # 12. Check physician_summary if present
    if "physician_summary" in table_names:
        summary_count = con.execute(
            "SELECT COUNT(*) FROM physician_summary"
        ).fetchone()[0]
        passed = summary_count > 5_000_000
        failures += 0 if run_check(
            "physician_summary has data", passed, f"{summary_count:,} rows"
        ) else 1

    # 13. Check geography_service if present
    if "geography_service" in table_names:
        geo_count = con.execute(
            "SELECT COUNT(*) FROM geography_service"
        ).fetchone()[0]
        passed = geo_count > 100_000
        failures += 0 if run_check(
            "geography_service has data", passed, f"{geo_count:,} rows"
        ) else 1

    # Summary
    total_rows = con.execute(
        "SELECT SUM(row_count) FROM _metadata"
    ).fetchone()[0] or 0
    print(f"\n  Total rows: {total_rows:,}")
    print(f"  Data tables: {len(data_tables)}")

    db_size = args.db.stat().st_size / (1024**3)
    print(f"  Database size: {db_size:.2f} GB")

    print(f"\n  {'='*40}")
    if failures == 0:
        print("  All checks passed.")
    else:
        print(f"  {failures} check(s) FAILED.")

    con.close()
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
