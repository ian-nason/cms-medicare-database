# CMS Medicare Physician & Other Supplier Database

Centers for Medicare & Medicaid Services (CMS) published Medicare Part B fee-for-service utilization and payment summaries for calendar years 2013–2024. Use provider-year summaries for provider totals, or provider–procedure–place-of-service–year records to examine published service patterns and average payments. The service table omits cells with fewer than 11 beneficiaries, so summing its payments does not give total Medicare spending. Medicare Advantage, Part A and Part D are outside this dataset, and beneficiaries can appear in multiple service cells.

<a id="quick-start"></a>

## Start querying

Preview published provider-year payment totals for New York in 2023. The service table is suppressed and cannot establish total Medicare spending.

`Rndrng_NPI` is the provider's National Provider Identifier (NPI); `Tot_Mdcr_Pymt_Amt` is the published provider-year Medicare payment amount in U.S. dollars; `year` is calendar year.

Choose R or Python. Install datapond once: use `install.packages("datapond")` in R, or run `python -m pip install datapond` in a terminal.

### R

```r
library(datapond)
con <- dp_connect("cms-medicare")
tryCatch({
  DBI::dbExecute(con, "SET threads=1")
  DBI::dbExecute(con, "SET memory_limit='128MiB'")
  DBI::dbExecute(con, "SET max_temp_directory_size='0B'")
  print(DBI::dbGetQuery(con, "SELECT year,Rndrng_NPI,Tot_Mdcr_Pymt_Amt FROM physician_summary WHERE year=2023 AND Rndrng_Prvdr_State_Abrvtn='NY' LIMIT 5"))
}, finally = dp_disconnect(con))
```

### Python

```python
import datapond
con = datapond.connect('cms-medicare')
try:
    con.execute("SET threads=1")
    con.execute("SET memory_limit='128MiB'")
    con.execute("SET max_temp_directory_size='0B'")
    print(con.execute("""SELECT year,Rndrng_NPI,Tot_Mdcr_Pymt_Amt FROM physician_summary WHERE year=2023 AND Rndrng_Prvdr_State_Abrvtn='NY' LIMIT 5""").fetchall())
finally:
    con.close()
```

Queries run on your computer and read the byte ranges they need. A small result can still require a large scan. These examples limit DuckDB to one thread and a 128 MiB buffer pool with disk spill disabled; that is not a limit on the entire R or Python process. Larger analyses may need more memory or a local download with enough disk space.

<a id="additional-dataset-notes"></a>

<a id="tables"></a>

## Tables and coverage

**132.9M rows across three tables**, calendar years (CY) 2013–2024. NPI means National Provider Identifier. HCPCS means Healthcare Common Procedure Coding System; CPT means Current Procedural Terminology, a procedure-coding system included in HCPCS descriptions.

### physician_summary

One row per NPI per year with aggregate totals across all HCPCS codes. Columns vary by year but generally include total services, total beneficiaries, total payment amounts, and beneficiary demographics.

### physician_services

One row per provider (NPI) per HCPCS code per place of service per year.

| Column | Type | Description |
|--------|------|-------------|
| npi | VARCHAR | National Provider Identifier |
| provider_last_name | VARCHAR | Last name or organization name |
| provider_first_name | VARCHAR | First name (blank for orgs) |
| provider_mi | VARCHAR | Middle initial |
| provider_credentials | VARCHAR | Provider credentials (MD, DO, etc.) |
| provider_gender | VARCHAR | Retained field; wholly NULL in this snapshot |
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
| hcpcs_drug_indicator | VARCHAR | Y if Part B drug; populated from 2013 in this snapshot |
| place_of_service | VARCHAR | F=Facility, O=Office |
| line_srvc_cnt | DOUBLE | Number of services provided |
| bene_unique_cnt | INTEGER | Distinct beneficiaries within this provider-procedure-place-year cell; not additive across cells |
| bene_day_srvc_cnt | INTEGER | Distinct beneficiary/day services |
| avg_submitted_chrg_amt | DOUBLE | Average submitted charge per service, U.S. dollars |
| avg_medicare_allowed_amt | DOUBLE | Average Medicare allowed amount per service, U.S. dollars |
| avg_medicare_payment_amt | DOUBLE | Average Medicare payment per service, U.S. dollars |
| avg_medicare_standardized_amt | DOUBLE | Average standardized payment per service, U.S. dollars; populated from 2013 in this snapshot |
| year | INTEGER | Calendar year of service |

### geography_service

State/national HCPCS aggregate. Utilization and payment amounts by geography, HCPCS code, and place of service per year.

<a id="coverage-fields-and-sources"></a>

See the [data dictionary](https://github.com/ian-nason/cms-medicare-database/blob/main/DICTIONARY.md) for column definitions.

## Known Limitations — read before publishing numbers

- **Suppression makes `physician_services` a systematic undercount.** CMS
  suppresses any provider/procedure/place-of-service cell with fewer than 11
  beneficiaries, and low-volume cells are the majority of the missing mass:
  summing `physician_services` payments recovers only ~83% of the true
  provider totals (2023: $93.7B from services vs $112.9B in
  `physician_summary`; per-NPI mean undercount ~21%). **Use
  `physician_summary` for provider-level totals** and treat any total or
  market-share figure computed from `physician_services` as a floor, biased
  hardest against low-volume providers and procedures.
- **Suppression indicators in `physician_summary`:** where a Drug_ subtotal
  covers 1-10 beneficiaries, CMS blanks it and sets `Drug_Sprsn_Ind = '*'`;
  the Med_ counterpart is then counter-suppressed with `Med_Sprsn_Ind = '#'`
  (and vice versa). About 11% of rows are affected; `Tot_` columns remain
  complete. Drug-vs-medical splits undercount unless you handle these.
- **Fee-for-service only**: no Medicare Advantage (Part C), no Part D drugs,
  no Part A. "Total Medicare revenue" per provider is not knowable from this
  data.
- **`provider_gender` is 100% NULL in every year** — CMS removed it
  retroactively from all years in the current PUF re-releases. Link NPPES if
  you need provider sex.
- **Chronic-condition percentage columns** (`Bene_CC_*`) are NULL for
  2013-2016 and top-coded at 75 by CMS; race/dual-status beneficiary counts
  are heavily suppression-NULLed and sum to less than `Tot_Benes` by
  construction.
- **`geography_service` mixes National and State rows** — always filter
  `Rndrng_Prvdr_Geo_Lvl`. State rows are separately suppressed, so state sums
  run ~4% below the National row.
- **State columns include territories and codes like ZZ/XX** (PR, GU, VI,
  military AE/AP/AA, foreign) — filter explicitly for "50 states + DC"
  analyses.
- **No quality data**: utilization and payment only; no MIPS, outcomes, or
  quality metrics.
- **2012 data**: not available (the NBER mirror that hosted CY2012 returns
  404; data.cms.gov has 2013+ only).
- **Sequestration**: payments from April 2013 onward reflect a 2%
  sequestration reduction.

Note: `avg_medicare_standardized_amt` and `hcpcs_drug_indicator` are
populated for **all** years including 2013 — the current CMS re-releases
backfilled them (earlier README versions said 2014+).

## Example Queries

### Payments represented by published service cells, by year

```sql
SELECT year,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / 1e9, 2) AS published_service_cell_payments_billions
FROM physician_services
GROUP BY year ORDER BY year;
```

### Preview a provider ranking within 10,000 published service cells

This example uses at most 10,000 matching rows in the preview; the engine may scan more rows or read more bytes to find them. The extract is neither random nor representative; its groups and rankings do not describe the full year or national population.

These sums omit suppressed service cells and do not establish complete provider totals. The ranking is by published service-cell payments, not submitted charges. Use physician_summary for published provider-year totals.

```sql
WITH preview AS MATERIALIZED (
  SELECT npi, provider_last_name, provider_first_name, provider_type, line_srvc_cnt, avg_submitted_chrg_amt, avg_medicare_payment_amt
  FROM physician_services
  WHERE year = 2024
  LIMIT 10000
)
SELECT npi, provider_last_name, provider_first_name, provider_type,
       ROUND(SUM(line_srvc_cnt * avg_submitted_chrg_amt), 2) AS published_service_cell_charges,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt), 2) AS published_service_cell_payments
FROM preview
GROUP BY ALL
ORDER BY published_service_cell_payments DESC
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

### Preview procedure payments by state within a bounded extract

This example uses at most 10,000 matching rows in the preview; the engine may scan more rows or read more bytes to find them. The extract is neither random nor representative; its groups and rankings do not describe the full year or national population.

The average below gives equal weight to each published service cell. Summed service-cell beneficiary counts can count the same beneficiary repeatedly; they are not unique beneficiaries.

```sql
WITH preview AS MATERIALIZED (
  SELECT provider_state, npi, avg_medicare_payment_amt, bene_unique_cnt, hcpcs_code, provider_country
  FROM physician_services
  WHERE year = 2022
  LIMIT 10000
)
-- E/M office visit (99213) average payment by state
SELECT provider_state,
       COUNT(DISTINCT npi) AS providers,
       ROUND(AVG(avg_medicare_payment_amt), 2) AS avg_payment,
       SUM(bene_unique_cnt) AS summed_service_cell_beneficiaries
FROM preview
WHERE hcpcs_code = '99213'
  AND provider_country = 'US'
GROUP BY provider_state
ORDER BY avg_payment DESC;
```

### Preview drug and non-drug payments within a bounded extract

This example uses at most 10,000 matching rows in the preview; the engine may scan more rows or read more bytes to find them. The extract is neither random nor representative; its groups and rankings do not describe the full year or national population.

Suppressed cells are absent. Summed service-cell beneficiary counts are not unique beneficiaries across providers or procedures.

```sql
WITH preview AS MATERIALIZED (
  SELECT year, hcpcs_drug_indicator, line_srvc_cnt, avg_medicare_payment_amt, bene_unique_cnt
  FROM physician_services
  WHERE year = 2024
  LIMIT 10000
)
SELECT year,
       hcpcs_drug_indicator,
       ROUND(SUM(line_srvc_cnt * avg_medicare_payment_amt) / 1e9, 2) AS published_service_cell_payments_billions,
       SUM(bene_unique_cnt) AS summed_service_cell_beneficiaries
FROM preview
WHERE hcpcs_drug_indicator IS NOT NULL
GROUP BY year, hcpcs_drug_indicator
ORDER BY year, hcpcs_drug_indicator;
```

<a id="advanced-access-quick-start"></a>

## Other ways to connect

### With datapond

```python
import datapond
cms = datapond.connect("cms-medicare")
try:
    cms.sql("SELECT * FROM physician_services LIMIT 5").show()
finally:
    cms.close()
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

## Sources and maintenance

### Source and license reference

See the [data dictionary](https://github.com/ian-nason/cms-medicare-database/blob/main/DICTIONARY.md) for column definitions and the [Centers for Medicare & Medicaid Services](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners) for the source. License: Public domain (HCPCS descriptions include AMA CPT content).

### Data Source

[CMS Medicare Physician & Other Practitioners Public Use Files](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

- **Provider and Service**: One row per NPI per HCPCS per place of service
- **By Provider**: Aggregate summary per NPI
- **By Geography and Service**: State/national HCPCS aggregates
- **CY2012 mirror**: [NBER](https://data.nber.org/providerchargepuf/)

Full data dictionary: [DICTIONARY.md](DICTIONARY.md)

### Build Instructions

#### Requirements

- Python 3.10+
- `duckdb`, `requests`, `tqdm`

#### Install dependencies

```bash
uv pip install duckdb requests tqdm
```

#### Build

```bash
# Build all years (downloads ~25GB, final DB ~8-15GB)
python build_database.py

# Build specific years
python build_database.py --years 2020 2021 2022 2023

# Skip optional tables
python build_database.py --skip-geo --skip-summary
```

#### Validate

```bash
python validate_database.py
```

#### Publish to Hugging Face

```bash
HF_TOKEN=hf_xxx python publish_to_hf.py
```

### License

Build code: [MIT](LICENSE). Underlying data: public domain (U.S. government work).

HCPCS descriptions contain AMA CPT content included as provided by CMS in the PUF. CPT is copyright American Medical Association. See [LICENSE](LICENSE) for details.
