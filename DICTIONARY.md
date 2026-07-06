# Data Dictionary

Source: [CMS Medicare Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

## geography_service

Geographic aggregate: Medicare utilization and payment by state/national level, HCPCS code, and place of service per year.

Rows: 3,228,031

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Rndrng_Prvdr_Geo_Lvl | VARCHAR | 0.0% | National |  |
| Rndrng_Prvdr_Geo_Cd | VARCHAR | 4.8% | 26 |  |
| Rndrng_Prvdr_Geo_Desc | VARCHAR | 0.0% | National |  |
| HCPCS_Cd | VARCHAR | 0.0% | 78195 |  |
| HCPCS_Desc | VARCHAR | 0.0% | Anesthesia for procedure on salivary gland with biopsy |  |
| HCPCS_Drug_Ind | VARCHAR | 0.0% | N |  |
| Place_Of_Srvc | VARCHAR | 0.0% | F |  |
| Tot_Rndrng_Prvdrs | BIGINT | 0.0% | 9678 |  |
| Tot_Benes | BIGINT | 0.0% | 98 |  |
| Tot_Srvcs | BIGINT | 0.0% | 38 |  |
| Tot_Bene_Day_Srvcs | BIGINT | 0.0% | 184 |  |
| Avg_Sbmtd_Chrg | DOUBLE | 0.0% | 1463.4581263 |  |
| Avg_Mdcr_Alowd_Amt | DOUBLE | 0.0% | 217.1103125 |  |
| Avg_Mdcr_Pymt_Amt | DOUBLE | 0.0% | 180.74465496 |  |
| Avg_Mdcr_Stdzd_Amt | DOUBLE | 0.0% | 184.70256623 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |

## physician_services

Provider-level Medicare Part B claims: one row per NPI per HCPCS code per place of service per year. Includes service counts, beneficiary counts, and average payment amounts.

Rows: 116,190,383

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| npi | VARCHAR | 0.0% | 1538426671 | National Provider Identifier, joins across all tables and to NPPES |
| provider_last_name | VARCHAR | 0.0% | Cowey |  |
| provider_first_name | VARCHAR | 4.8% | Vijay |  |
| provider_mi | VARCHAR | 30.9% | R |  |
| provider_credentials | VARCHAR | 8.4% | M.D. |  |
| provider_gender | VARCHAR | 100.0% |  |  |
| provider_entity_code | VARCHAR | 0.0% | I | I=Individual, O=Organization |
| provider_street1 | VARCHAR | 0.0% | 1001 Sam Perry Blvd |  |
| provider_street2 | VARCHAR | 65.1% | Suite 425 |  |
| provider_city | VARCHAR | 0.0% | Turners Falls |  |
| provider_state | VARCHAR | 0.0% | MA | Two-letter state abbreviation |
| provider_state_fips | VARCHAR | 0.1% | 25 | FIPS state code |
| provider_zip | VARCHAR | 0.0% | 65807 | 5-digit ZIP code |
| provider_ruca | VARCHAR | 0.1% | 1 | Rural-Urban Commuting Area code |
| provider_ruca_desc | VARCHAR | 0.1% | Metropolitan area core: primary flow within an urbanized area of 50,000 and g... |  |
| provider_country | VARCHAR | 0.0% | US |  |
| provider_type | VARCHAR | 0.0% | Diagnostic Radiology | Provider specialty derived from claims |
| medicare_participation | VARCHAR | 0.0% | Y | Y=participates in Medicare |
| hcpcs_code | VARCHAR | 0.0% | 96372 | HCPCS procedure code, joins to HCPCS/CPT lookup tables |
| hcpcs_description | VARCHAR | 0.0% | Injection of different drug or substance into a vein for therapy |  |
| hcpcs_drug_indicator | VARCHAR | 0.0% | N | Y if HCPCS is on Part B Drug ASP file |
| place_of_service | VARCHAR | 0.0% | F | F=Facility, O=Office/Non-facility |
| line_srvc_cnt | DOUBLE | 0.0% | 62.0 |  |
| bene_unique_cnt | INTEGER | 0.0% | 16 |  |
| bene_day_srvc_cnt | INTEGER | 0.0% | 20 |  |
| avg_submitted_chrg_amt | DOUBLE | 0.0% | 368.62676056 |  |
| avg_medicare_allowed_amt | DOUBLE | 0.0% | 161.50877551 |  |
| avg_medicare_payment_amt | DOUBLE | 0.0% | 17.217631579 |  |
| avg_medicare_standardized_amt | DOUBLE | 0.0% | 75.835762712 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |

## physician_summary

Provider-level aggregate summary: one row per NPI per year with total services, beneficiaries, and payment amounts across all HCPCS codes.

Rows: 13,528,933

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Rndrng_NPI | VARCHAR | 0.0% | 1235139759 |  |
| Rndrng_Prvdr_Last_Org_Name | VARCHAR | 0.0% | PISARELLO |  |
| Rndrng_Prvdr_First_Name | VARCHAR | 5.5% | ELMER |  |
| Rndrng_Prvdr_MI | VARCHAR | 34.0% | B |  |
| Rndrng_Prvdr_Crdntls | VARCHAR | 11.1% | MD |  |
| Rndrng_Prvdr_Ent_Cd | VARCHAR | 0.0% | I |  |
| Rndrng_Prvdr_St1 | VARCHAR | 0.0% | 1455 E GOLF RD |  |
| Rndrng_Prvdr_St2 | VARCHAR | 69.4% | SUITE #401 |  |
| Rndrng_Prvdr_City | VARCHAR | 0.0% | DUBLIN |  |
| Rndrng_Prvdr_State_Abrvtn | VARCHAR | 0.0% | IL |  |
| Rndrng_Prvdr_State_FIPS | VARCHAR | 0.0% | 17 |  |
| Rndrng_Prvdr_Zip5 | VARCHAR | 0.0% | 77030 |  |
| Rndrng_Prvdr_RUCA | VARCHAR | 0.3% | 1 |  |
| Rndrng_Prvdr_RUCA_Desc | VARCHAR | 0.1% | Metropolitan area core: primary flow within an urbanized area of 50,000 and g... |  |
| Rndrng_Prvdr_Cntry | VARCHAR | 0.0% | US |  |
| Rndrng_Prvdr_Type | VARCHAR | 0.0% | Podiatry |  |
| Rndrng_Prvdr_Mdcr_Prtcptg_Ind | VARCHAR | 0.0% | Y |  |
| Tot_HCPCS_Cds | BIGINT | 0.0% | 27 |  |
| Tot_Benes | BIGINT | 0.0% | 134 |  |
| Tot_Srvcs | DOUBLE | 0.0% | 601.0 |  |
| Tot_Sbmtd_Chrg | DOUBLE | 0.0% | 79903.0 |  |
| Tot_Mdcr_Alowd_Amt | DOUBLE | 0.0% | 43807.1 |  |
| Tot_Mdcr_Pymt_Amt | DOUBLE | 0.0% | 32023.51 |  |
| Tot_Mdcr_Stdzd_Amt | DOUBLE | 0.0% | 30398.59 |  |
| Drug_Sprsn_Ind | VARCHAR | 89.0% | * |  |
| Drug_Tot_HCPCS_Cds | BIGINT | 11.0% | 0 |  |
| Drug_Tot_Benes | BIGINT | 11.0% | 0 |  |
| Drug_Tot_Srvcs | DOUBLE | 11.0% | 0.0 |  |
| Drug_Sbmtd_Chrg | DOUBLE | 11.0% | 0.0 |  |
| Drug_Mdcr_Alowd_Amt | DOUBLE | 11.0% | 0.0 |  |
| Drug_Mdcr_Pymt_Amt | DOUBLE | 11.0% | 0.0 |  |
| Drug_Mdcr_Stdzd_Amt | DOUBLE | 11.0% | 0.0 |  |
| Med_Sprsn_Ind | VARCHAR | 89.0% | # |  |
| Med_Tot_HCPCS_Cds | BIGINT | 11.0% | 33 |  |
| Med_Tot_Benes | BIGINT | 11.0% | 687 |  |
| Med_Tot_Srvcs | DOUBLE | 11.0% | 4758.0 |  |
| Med_Sbmtd_Chrg | DOUBLE | 11.0% | 138671.0 |  |
| Med_Mdcr_Alowd_Amt | DOUBLE | 11.0% | 114422.28 |  |
| Med_Mdcr_Pymt_Amt | DOUBLE | 11.0% | 89482.72 |  |
| Med_Mdcr_Stdzd_Amt | DOUBLE | 11.0% | 163049.52 |  |
| Bene_Avg_Age | BIGINT | 0.0% | 70 |  |
| Bene_Age_LT_65_Cnt | BIGINT | 40.3% | 21 |  |
| Bene_Age_65_74_Cnt | BIGINT | 16.6% | 57 |  |
| Bene_Age_75_84_Cnt | BIGINT | 28.5% | 39 |  |
| Bene_Age_GT_84_Cnt | BIGINT | 44.1% | 17 |  |
| Bene_Feml_Cnt | BIGINT | 11.8% | 78 |  |
| Bene_Male_Cnt | BIGINT | 11.8% | 56 |  |
| Bene_Race_Wht_Cnt | BIGINT | 33.5% | 108 |  |
| Bene_Race_Black_Cnt | BIGINT | 67.5% | 61 |  |
| Bene_Race_API_Cnt | BIGINT | 82.1% | 32 |  |
| Bene_Race_Hspnc_Cnt | BIGINT | 74.4% | 13 |  |
| Bene_Race_NatInd_Cnt | BIGINT | 68.6% | 0 |  |
| Bene_Race_Othr_Cnt | BIGINT | 83.1% | 0 |  |
| Bene_Dual_Cnt | BIGINT | 28.0% | 25 |  |
| Bene_Ndual_Cnt | BIGINT | 28.0% | 109 |  |
| Bene_Avg_Risk_Scre | DOUBLE | 0.0% | 3.5146 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |
| Bene_CC_BH_ADHD_OthCD_V1_Pct | BIGINT | 75.7% | 0 |  |
| Bene_CC_BH_Alcohol_Drug_V1_Pct | BIGINT | 60.9% | 25 |  |
| Bene_CC_BH_Tobacco_V1_Pct | BIGINT | 54.7% | 0 |  |
| Bene_CC_BH_Alz_NonAlzdem_V2_Pct | BIGINT | 58.3% | 37 |  |
| Bene_CC_BH_Anxiety_V1_Pct | BIGINT | 41.2% | 67 |  |
| Bene_CC_BH_Bipolar_V1_Pct | BIGINT | 67.6% | 59 |  |
| Bene_CC_BH_Mood_V2_Pct | BIGINT | 40.4% | 75 |  |
| Bene_CC_BH_Depress_V1_Pct | BIGINT | 42.1% | 59 |  |
| Bene_CC_BH_PD_V1_Pct | BIGINT | 75.2% | 0 |  |
| Bene_CC_BH_PTSD_V1_Pct | BIGINT | 76.2% | 0 |  |
| Bene_CC_BH_Schizo_OthPsy_V1_Pct | BIGINT | 69.9% | 44 |  |
| Bene_CC_PH_Asthma_V2_Pct | BIGINT | 53.9% | 13 |  |
| Bene_CC_PH_Afib_V2_Pct | BIGINT | 48.3% | 0 |  |
| Bene_CC_PH_Cancer6_V2_Pct | BIGINT | 49.9% | 0 |  |
| Bene_CC_PH_CKD_V2_Pct | BIGINT | 45.0% | 75 |  |
| Bene_CC_PH_COPD_V2_Pct | BIGINT | 48.0% | 51 |  |
| Bene_CC_PH_Diabetes_V2_Pct | BIGINT | 40.6% | 41 |  |
| Bene_CC_PH_HF_NonIHD_V2_Pct | BIGINT | 49.3% | 35 |  |
| Bene_CC_PH_Hyperlipidemia_V2_Pct | BIGINT | 33.2% | 67 |  |
| Bene_CC_PH_Hypertension_V2_Pct | BIGINT | 32.8% | 56 |  |
| Bene_CC_PH_IschemicHeart_V2_Pct | BIGINT | 43.3% | 16 |  |
| Bene_CC_PH_Osteoporosis_V2_Pct | BIGINT | 51.1% | 0 |  |
| Bene_CC_PH_Parkinson_V2_Pct | BIGINT | 72.2% | 2 |  |
| Bene_CC_PH_Arthritis_V2_Pct | BIGINT | 36.3% | 33 |  |
| Bene_CC_PH_Stroke_TIA_V2_Pct | BIGINT | 56.8% | 0 |  |
