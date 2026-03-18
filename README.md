# CMS Medicare Physician & Other Supplier Database

A clean, queryable DuckDB database built from the [CMS Medicare Physician & Other Practitioners Public Use Files](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners). Contains provider-level Medicare Part B fee-for-service claims data: what every physician billed, what Medicare paid, how many services and beneficiaries per NPI per HCPCS code.

**121.7M rows** across **3 tables** covering CY2013 through CY2023.

## Quick Start

### With datapond

```python
import datapond
cms = datapond.attach("cms-medicare")
cms.sql("SELECT * FROM physician_services LIMIT 5").show()
```

### Raw DuckDB ATTACH

```sql
INSTALL httpfs;
LOAD httpfs;
ATTACH 'https://huggingface.co/datasets/Nason/cms-medicare-database/resolve/main/cms_medicare.duckdb' AS cms (READ_ONLY);

SELECT * FROM cms.physician_services LIMIT 5;
```

### Python

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("""
    ATTACH 'https://huggingface.co/datasets/Nason/cms-medicare-database/resolve/main/cms_medicare.duckdb'
    AS cms (READ_ONLY)
""")
con.sql("SELECT * FROM cms.physician_services LIMIT 5").show()
```

DuckDB uses HTTP range requests so only the pages needed for your query are fetched.

## Tables

### physician_services

The main table. One row per provider (NPI) per HCPCS code per place of service per year.

| Column | Type | Description |
|--------|------|-------------|
| npi | VARCHAR | National Provider Identifier |
| provider_last_name | VARCHAR | Last name or organization name |
| provider_first_name | VARCHAR | First name (blank for orgs) |
| provider_mi | VARCHAR | Middle initial |
| provider_credentials | VARCHAR | Provider credentials (MD, DO, etc.) |
| provider_gender | VARCHAR | M or F (blank for orgs) |
| provider_entity_code | VARCHAR | I=Individual, O=Organization |
| provider_street1 | VARCHAR | Street address line 1 |
| provider_street2 | VARCHAR | Street address line 2 |
| provider_city | VARCHAR | City |
| provider_state | VARCHAR | Two-letter state abbreviation |
| provider_state_fips | VARCHAR | FIPS state code (2014+) |
| provider_zip | VARCHAR | 5-digit ZIP code |
| provider_ruca | VARCHAR | Rural-Urban Commuting Area code (2014+) |
| provider_ruca_desc | VARCHAR | RUCA description (2014+) |
| provider_country | VARCHAR | Country code (US for domestic) |
| provider_type | VARCHAR | Provider specialty |
| medicare_participation | VARCHAR | Y=participates in Medicare |
| hcpcs_code | VARCHAR | HCPCS/CPT procedure code |
| hcpcs_description | VARCHAR | HCPCS code description |
| hcpcs_drug_indicator | VARCHAR | Y if Part B drug (2014+) |
| place_of_service | VARCHAR | F=Facility, O=Office |
| line_srvc_cnt | DOUBLE | Number of services provided |
| bene_unique_cnt | INTEGER | Distinct beneficiaries |
| bene_day_srvc_cnt | INTEGER | Distinct beneficiary/day services |
| avg_submitted_chrg_amt | DOUBLE | Average submitted charge |
| avg_medicare_allowed_amt | DOUBLE | Average Medicare allowed amount |
| avg_medicare_payment_amt | DOUBLE | Average Medicare payment |
| avg_medicare_standardized_amt | DOUBLE | Average standardized payment (2014+) |
| year | INTEGER | Calendar year of service |

### physician_summary

One row per NPI per year with aggregate totals across all HCPCS codes. Columns vary by year but generally include total services, total beneficiaries, total payment amounts, and beneficiary demographics.

### geography_service

State/national HCPCS aggregate. Utilization and payment amounts by geography, HCPCS code, and place of service per year.

## Example Queries

### Total Medicare spending by year

```sql
SELECT year,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / 1e9, 2) AS spending_billions
FROM physician_services
GROUP BY year ORDER BY year;
```

### Top 10 highest-billing providers (most recent year)

```sql
SELECT npi, provider_last_name, provider_first_name, provider_type,
       ROUND(SUM(line_srvc_cnt * avg_submitted_chrg_amt), 2) AS total_charges,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt), 2) AS total_payments
FROM physician_services
WHERE year = (SELECT MAX(year) FROM physician_services)
GROUP BY ALL
ORDER BY total_payments DESC
LIMIT 10;
```

### Average payment per service by specialty

```sql
SELECT provider_type,
       COUNT(DISTINCT npi) AS providers,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / SUM(line_srvc_cnt), 2) AS avg_payment_per_svc
FROM physician_services
WHERE year = 2022
GROUP BY provider_type
HAVING COUNT(DISTINCT npi) >= 100
ORDER BY avg_payment_per_svc DESC
LIMIT 20;
```

### Geographic variation in a specific procedure

```sql
-- E/M office visit (99213) average payment by state
SELECT provider_state,
       COUNT(DISTINCT npi) AS providers,
       ROUND(AVG(avg_medicare_payment_amt), 2) AS avg_payment,
       SUM(bene_unique_cnt) AS total_benes
FROM physician_services
WHERE hcpcs_code = '99213' AND year = 2022
  AND provider_country = 'US'
GROUP BY provider_state
ORDER BY avg_payment DESC;
```

### Drug vs non-drug spending trends

```sql
SELECT year,
       hcpcs_drug_indicator,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / 1e9, 2) AS spending_billions,
       SUM(bene_unique_cnt) AS total_benes
FROM physician_services
WHERE hcpcs_drug_indicator IS NOT NULL
GROUP BY year, hcpcs_drug_indicator
ORDER BY year, hcpcs_drug_indicator;
```

## Known Limitations

- **Fee-for-service only**: Does not include Medicare Advantage (Part C) or Part D prescription drugs. Only covers traditional Medicare Part B.
- **Privacy redaction**: CMS suppresses records with fewer than 11 beneficiaries, so low-volume provider/procedure combinations are excluded.
- **No quality data**: Contains utilization and payment data only. No MIPS scores, patient outcomes, or quality metrics.
- **Standardized amounts**: `avg_medicare_standardized_amt` is only available from 2014 onward.
- **HCPCS drug indicator**: Only available from 2014 onward.
- **Schema changes**: Some columns like `provider_gender` were removed by CMS in recent PUF releases and will be NULL.
- **2012 data**: Not currently available. The NBER mirror (which hosted CY2012) returns 404 as of March 2026. The data.cms.gov portal only has 2013+.
- **Sequestration**: Medicare payments from April 2013 onward reflect a 2% sequestration reduction. Be cautious comparing 2012 payment levels to later years.

## Data Source

[CMS Medicare Physician & Other Practitioners Public Use Files](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

- **Provider and Service**: One row per NPI per HCPCS per place of service
- **By Provider**: Aggregate summary per NPI
- **By Geography and Service**: State/national HCPCS aggregates
- **CY2012 mirror**: [NBER](https://data.nber.org/providerchargepuf/)

Full data dictionary: [DICTIONARY.md](DICTIONARY.md)

## Build Instructions

### Requirements

- Python 3.10+
- `duckdb`, `requests`, `tqdm`

### Install dependencies

```bash
uv pip install duckdb requests tqdm
```

### Build

```bash
# Build all years (downloads ~25GB, final DB ~8-15GB)
python build_database.py

# Build specific years
python build_database.py --years 2020 2021 2022 2023

# Skip optional tables
python build_database.py --skip-geo --skip-summary
```

### Validate

```bash
python validate_database.py
```

### Publish to Hugging Face

```bash
HF_TOKEN=hf_xxx python publish_to_hf.py
```

## License

Build code: [MIT](LICENSE). Underlying data: public domain (U.S. government work).

HCPCS descriptions contain AMA CPT content included as provided by CMS in the PUF. CPT is copyright American Medical Association. See [LICENSE](LICENSE) for details.
