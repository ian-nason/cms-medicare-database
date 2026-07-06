# Changelog

## 2026-07-06 — Full refresh + data-quality audit

Rebuilt from the current CMS PUF releases, adding CY2024, and repaired after
an independent SQL-verified audit.

**Data changes**
- CY2024 added to all three tables: 132,947,347 rows total
  (physician_services 116.2M, physician_summary 13.5M,
  geography_service 3.2M), CY2013-CY2024.

**Fixes**
- `geography_service.HCPCS_Cd` re-ingested as VARCHAR: numeric auto-casting
  had NULLed all 282,776 alphanumeric codes (G/J/lab codes) and stripped
  leading zeros from 137,781 anesthesia codes. Geography-level procedure
  analysis works again, including joins to `physician_services.hcpcs_code`.
- `physician_summary` identifiers re-typed as VARCHAR: `Rndrng_NPI` (joins
  to services now work), `Rndrng_Prvdr_Zip5` (1.35M leading-zero ZIPs
  restored), `State_FIPS`, `RUCA`.

**Known caveats** (see README for the full list)
- CMS suppression makes `physician_services` a systematic ~17-21% payment
  undercount vs `physician_summary` — use the summary table for provider
  totals.
- `provider_gender` is 100% NULL in every year (removed retroactively by
  CMS); chronic-condition columns are NULL before 2017 and top-coded at 75.
- `geography_service` mixes National and State rows — filter
  `Rndrng_Prvdr_Geo_Lvl`.
