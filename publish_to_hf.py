#!/usr/bin/env python3
"""Upload cms_medicare.duckdb to Hugging Face for remote access.

Usage:
    python publish_to_hf.py
    python publish_to_hf.py --db cms_medicare.duckdb --repo Nason/cms-medicare-database
    HF_TOKEN=hf_xxx python publish_to_hf.py
"""

import argparse
import sys
from pathlib import Path

import duckdb
from huggingface_hub import HfApi, create_repo


def generate_dataset_card(db_path: str) -> str:
    """Generate a HF-compatible README with YAML frontmatter."""
    con = duckdb.connect(db_path, read_only=True)
    metadata = con.sql(
        "SELECT table_name, description, row_count "
        "FROM _metadata ORDER BY row_count DESC"
    ).fetchdf()
    con.close()

    table_rows = "\n".join(
        f"| `{row['table_name']}` | {row['description'][:80]} | {row['row_count']:,} |"
        for _, row in metadata.iterrows()
    )
    total_rows = int(metadata["row_count"].sum())
    n_tables = len(metadata)

    return f"""---
license: other
license_name: public-domain-with-cpt
license_link: LICENSE
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - medicare
  - cms
  - healthcare
  - physician
  - duckdb
  - government-data
  - medical-billing
pretty_name: CMS Medicare Physician & Other Supplier Database
size_categories:
  - 100M<n<1B
---

# CMS Medicare Physician & Other Supplier Database

A clean, queryable DuckDB database built from the [CMS Medicare Physician & Other Practitioners Public Use Files](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners) -- provider-level Medicare Part B claims data from CY2012 through CY2023.

**{total_rows:,} rows** across **{n_tables} tables** covering what every physician billed, what Medicare paid, and how many services and beneficiaries per NPI per HCPCS code.

Built with [cms-medicare-database](https://github.com/ian-nason/cms-medicare-database).

## Quick Start

### DuckDB CLI

```sql
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://huggingface.co/datasets/Nason/cms-medicare-database/resolve/main/cms_medicare.duckdb' AS cms (READ_ONLY);

-- Total Medicare spending by year
SELECT year, ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / 1e9, 2) AS total_spending_billions
FROM cms.physician_services
GROUP BY year ORDER BY year;
```

### Python

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(\"\"\"
    ATTACH 'https://huggingface.co/datasets/Nason/cms-medicare-database/resolve/main/cms_medicare.duckdb'
    AS cms (READ_ONLY)
\"\"\")
con.sql("SELECT * FROM cms.physician_services LIMIT 5").show()
```

DuckDB uses HTTP range requests, so only the pages needed for your query are downloaded.

## Tables

| Table | Description | Rows |
|-------|-------------|------|
{table_rows}

## Data Source

[CMS Medicare Physician & Other Practitioners PUF](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners) -- maintained by CMS. Updated annually. Public domain U.S. government data. HCPCS descriptions include AMA CPT content used under CMS license.

## License

Database build code: MIT. Underlying data: public domain (U.S. government work). Note: HCPCS descriptions contain AMA CPT content included as provided by CMS in the PUF.

## GitHub

Full source code, build instructions, and data dictionary: [github.com/ian-nason/cms-medicare-database](https://github.com/ian-nason/cms-medicare-database)
"""


def main():
    parser = argparse.ArgumentParser(
        description="Upload cms_medicare.duckdb to Hugging Face"
    )
    parser.add_argument("--db", type=Path, default=Path("cms_medicare.duckdb"))
    parser.add_argument("--repo", default="Nason/cms-medicare-database")
    parser.add_argument("--token", help="HF token (or set HF_TOKEN env var)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: Database not found at {args.db}")
        sys.exit(1)

    api = HfApi(token=args.token)

    print(f"Creating dataset repo: {args.repo}")
    create_repo(args.repo, repo_type="dataset", exist_ok=True, token=args.token)

    print(f"Generating dataset card from {args.db}")
    card = generate_dataset_card(str(args.db))

    print("Uploading dataset card...")
    api.upload_file(
        path_or_fileobj=card.encode(),
        path_in_repo="README.md",
        repo_id=args.repo,
        repo_type="dataset",
    )

    size_gb = args.db.stat().st_size / (1024**3)
    print(f"Uploading {args.db} ({size_gb:.1f} GB)...")
    api.upload_file(
        path_or_fileobj=str(args.db),
        path_in_repo="cms_medicare.duckdb",
        repo_id=args.repo,
        repo_type="dataset",
    )

    print(f"\nUploaded to https://huggingface.co/datasets/{args.repo}")
    print(f"\nUsers can now query remotely:")
    print(
        f"  ATTACH 'https://huggingface.co/datasets/{args.repo}"
        f"/resolve/main/cms_medicare.duckdb' AS cms (READ_ONLY);"
    )


if __name__ == "__main__":
    main()
