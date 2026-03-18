# Data Dictionary

Source: [CMS Medicare Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

## geography_service

Geographic aggregate: Medicare utilization and payment by state/national level, HCPCS code, and place of service per year.

Rows: 2,959,681

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Rndrng_Prvdr_Geo_Lvl | VARCHAR | 0.0% | National |  |
| Rndrng_Prvdr_Geo_Cd | BIGINT | 5.2% | 1 |  |
| Rndrng_Prvdr_Geo_Desc | VARCHAR | 0.0% | National |  |
| HCPCS_Cd | BIGINT | 8.6% | 100 |  |
| HCPCS_Desc | VARCHAR | 0.0% | Anesthesia for procedure on salivary gland with biopsy |  |
| HCPCS_Drug_Ind | VARCHAR | 0.0% | N |  |
| Place_Of_Srvc | VARCHAR | 0.0% | F |  |
| Tot_Rndrng_Prvdrs | BIGINT | 0.0% | 9678 |  |
| Tot_Benes | BIGINT | 0.0% | 8629 |  |
| Tot_Srvcs | DOUBLE | 0.0% | 12419.0 |  |
| Tot_Bene_Day_Srvcs | BIGINT | 0.0% | 12415 |  |
| Avg_Sbmtd_Chrg | DOUBLE | 0.0% | 1463.4581263 |  |
| Avg_Mdcr_Alowd_Amt | DOUBLE | 0.0% | 228.00370239 |  |
| Avg_Mdcr_Pymt_Amt | DOUBLE | 0.0% | 180.74465496 |  |
| Avg_Mdcr_Stdzd_Amt | DOUBLE | 0.0% | 184.70256623 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |

## physician_services

Provider-level Medicare Part B claims: one row per NPI per HCPCS code per place of service per year. Includes service counts, beneficiary counts, and average payment amounts.

Rows: 106,515,734

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| npi | VARCHAR | 0.0% | 1003000126 | National Provider Identifier, joins across all tables and to NPPES |
| provider_last_name | VARCHAR | 0.0% | Enkeshafi |  |
| provider_first_name | VARCHAR | 4.7% | Ardalan |  |
| provider_mi | VARCHAR | 30.5% | L |  |
| provider_credentials | VARCHAR | 8.2% | M.D. |  |
| provider_gender | VARCHAR | 100.0% |  |  |
| provider_entity_code | VARCHAR | 0.0% | I | I=Individual, O=Organization |
| provider_street1 | VARCHAR | 0.0% | 900 Seton Dr |  |
| provider_street2 | VARCHAR | 63.9% | Evanston Hospital |  |
| provider_city | VARCHAR | 0.0% | Cumberland |  |
| provider_state | VARCHAR | 0.0% | MD | Two-letter state abbreviation |
| provider_state_fips | VARCHAR | 0.1% | 24 | FIPS state code |
| provider_zip | VARCHAR | 0.0% | 21502 | 5-digit ZIP code |
| provider_ruca | VARCHAR | 0.1% | 1 | Rural-Urban Commuting Area code |
| provider_ruca_desc | VARCHAR | 0.1% | Metropolitan area core: primary flow within an urbanized area of 50,000 and g... |  |
| provider_country | VARCHAR | 0.0% | US |  |
| provider_type | VARCHAR | 0.0% | Internal Medicine | Provider specialty derived from claims |
| medicare_participation | VARCHAR | 0.0% | Y | Y=participates in Medicare |
| hcpcs_code | VARCHAR | 0.0% | 99222 | HCPCS procedure code, joins to HCPCS/CPT lookup tables |
| hcpcs_description | VARCHAR | 0.0% | Initial hospital inpatient care, typically 50 minutes per day |  |
| hcpcs_drug_indicator | VARCHAR | 0.0% | N | Y if HCPCS is on Part B Drug ASP file |
| place_of_service | VARCHAR | 0.0% | F | F=Facility, O=Office/Non-facility |
| line_srvc_cnt | DOUBLE | 0.0% | 142.0 |  |
| bene_unique_cnt | INTEGER | 0.0% | 138 |  |
| bene_day_srvc_cnt | INTEGER | 0.0% | 142 |  |
| avg_submitted_chrg_amt | DOUBLE | 0.0% | 368.62676056 |  |
| avg_medicare_allowed_amt | DOUBLE | 0.0% | 132.17007042 |  |
| avg_medicare_payment_amt | DOUBLE | 0.0% | 104.29971831 |  |
| avg_medicare_standardized_amt | DOUBLE | 0.0% | 107.21112676 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |

## physician_summary

Provider-level aggregate summary: one row per NPI per year with total services, beneficiaries, and payment amounts across all HCPCS codes.

Rows: 12,232,194

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Rndrng_NPI | BIGINT | 0.0% | 1003000126 |  |
| Rndrng_Prvdr_Last_Org_Name | VARCHAR | 0.0% | ENKESHAFI |  |
| Rndrng_Prvdr_First_Name | VARCHAR | 5.6% | ARDALAN |  |
| Rndrng_Prvdr_MI | VARCHAR | 33.6% | L |  |
| Rndrng_Prvdr_Crdntls | VARCHAR | 10.8% | M.D. |  |
| Rndrng_Prvdr_Ent_Cd | VARCHAR | 0.0% | I |  |
| Rndrng_Prvdr_St1 | VARCHAR | 0.0% | 900 SETON DR |  |
| Rndrng_Prvdr_St2 | VARCHAR | 68.1% | EVANSTON HOSPITAL |  |
| Rndrng_Prvdr_City | VARCHAR | 0.0% | CUMBERLAND |  |
| Rndrng_Prvdr_State_Abrvtn | VARCHAR | 0.0% | MD |  |
| Rndrng_Prvdr_State_FIPS | BIGINT | 0.0% | 24 |  |
| Rndrng_Prvdr_Zip5 | BIGINT | 0.0% | 21502 |  |
| Rndrng_Prvdr_RUCA | DOUBLE | 0.4% | 1.0 |  |
| Rndrng_Prvdr_RUCA_Desc | VARCHAR | 0.1% | Metropolitan area core: primary flow within an urbanized area of 50,000 and g... |  |
| Rndrng_Prvdr_Cntry | VARCHAR | 0.0% | US |  |
| Rndrng_Prvdr_Type | VARCHAR | 0.0% | Internal Medicine |  |
| Rndrng_Prvdr_Mdcr_Prtcptg_Ind | VARCHAR | 0.0% | Y |  |
| Tot_HCPCS_Cds | BIGINT | 0.0% | 22 |  |
| Tot_Benes | BIGINT | 0.0% | 665 |  |
| Tot_Srvcs | DOUBLE | 0.0% | 1648.0 |  |
| Tot_Sbmtd_Chrg | DOUBLE | 0.0% | 395335.0 |  |
| Tot_Mdcr_Alowd_Amt | DOUBLE | 0.0% | 146521.84 |  |
| Tot_Mdcr_Pymt_Amt | DOUBLE | 0.0% | 116332.66 |  |
| Tot_Mdcr_Stdzd_Amt | DOUBLE | 0.0% | 118271.4 |  |
| Drug_Sprsn_Ind | VARCHAR | 89.1% | * |  |
| Drug_Tot_HCPCS_Cds | BIGINT | 10.9% | 0 |  |
| Drug_Tot_Benes | BIGINT | 10.9% | 0 |  |
| Drug_Tot_Srvcs | DOUBLE | 10.9% | 0.0 |  |
| Drug_Sbmtd_Chrg | DOUBLE | 10.9% | 0.0 |  |
| Drug_Mdcr_Alowd_Amt | DOUBLE | 10.9% | 0.0 |  |
| Drug_Mdcr_Pymt_Amt | DOUBLE | 10.9% | 0.0 |  |
| Drug_Mdcr_Stdzd_Amt | DOUBLE | 10.9% | 0.0 |  |
| Med_Sprsn_Ind | VARCHAR | 89.1% | # |  |
| Med_Tot_HCPCS_Cds | BIGINT | 10.9% | 22 |  |
| Med_Tot_Benes | BIGINT | 10.9% | 665 |  |
| Med_Tot_Srvcs | DOUBLE | 10.9% | 1648.0 |  |
| Med_Sbmtd_Chrg | DOUBLE | 10.9% | 395335.0 |  |
| Med_Mdcr_Alowd_Amt | DOUBLE | 10.9% | 146521.84 |  |
| Med_Mdcr_Pymt_Amt | DOUBLE | 10.9% | 116332.66 |  |
| Med_Mdcr_Stdzd_Amt | DOUBLE | 10.9% | 118271.4 |  |
| Bene_Avg_Age | BIGINT | 0.0% | 74 |  |
| Bene_Age_LT_65_Cnt | BIGINT | 39.0% | 120 |  |
| Bene_Age_65_74_Cnt | BIGINT | 16.4% | 186 |  |
| Bene_Age_75_84_Cnt | BIGINT | 28.5% | 205 |  |
| Bene_Age_GT_84_Cnt | BIGINT | 43.2% | 154 |  |
| Bene_Feml_Cnt | BIGINT | 11.6% | 359 |  |
| Bene_Male_Cnt | BIGINT | 11.6% | 306 |  |
| Bene_Race_Wht_Cnt | BIGINT | 33.3% | 639 |  |
| Bene_Race_Black_Cnt | BIGINT | 67.1% | 14 |  |
| Bene_Race_API_Cnt | BIGINT | 82.2% | 41 |  |
| Bene_Race_Hspnc_Cnt | BIGINT | 74.2% | 70 |  |
| Bene_Race_NatInd_Cnt | BIGINT | 69.0% | 0 |  |
| Bene_Race_Othr_Cnt | BIGINT | 83.3% | 39 |  |
| Bene_Dual_Cnt | BIGINT | 27.1% | 199 |  |
| Bene_Ndual_Cnt | BIGINT | 27.1% | 466 |  |
| Bene_Avg_Risk_Scre | DOUBLE | 0.0% | 2.1114 |  |
| year | INTEGER | 0.0% | 2013 | Calendar year of service |
| Bene_CC_BH_ADHD_OthCD_V1_Pct | BIGINT | 76.6% | 0 |  |
| Bene_CC_BH_Alcohol_Drug_V1_Pct | BIGINT | 62.3% | 10 |  |
| Bene_CC_BH_Tobacco_V1_Pct | BIGINT | 56.2% | 21 |  |
| Bene_CC_BH_Alz_NonAlzdem_V2_Pct | BIGINT | 60.0% | 25 |  |
| Bene_CC_BH_Anxiety_V1_Pct | BIGINT | 43.8% | 42 |  |
| Bene_CC_BH_Bipolar_V1_Pct | BIGINT | 68.8% | 7 |  |
| Bene_CC_BH_Mood_V2_Pct | BIGINT | 42.9% | 45 |  |
| Bene_CC_BH_Depress_V1_Pct | BIGINT | 44.6% | 43 |  |
| Bene_CC_BH_PD_V1_Pct | BIGINT | 76.2% | 3 |  |
| Bene_CC_BH_PTSD_V1_Pct | BIGINT | 77.0% | 3 |  |
| Bene_CC_BH_Schizo_OthPsy_V1_Pct | BIGINT | 71.0% | 8 |  |
| Bene_CC_PH_Asthma_V2_Pct | BIGINT | 55.7% | 14 |  |
| Bene_CC_PH_Afib_V2_Pct | BIGINT | 50.6% | 32 |  |
| Bene_CC_PH_Cancer6_V2_Pct | BIGINT | 52.0% | 17 |  |
| Bene_CC_PH_CKD_V2_Pct | BIGINT | 47.5% | 53 |  |
| Bene_CC_PH_COPD_V2_Pct | BIGINT | 50.0% | 40 |  |
| Bene_CC_PH_Diabetes_V2_Pct | BIGINT | 43.1% | 51 |  |
| Bene_CC_PH_HF_NonIHD_V2_Pct | BIGINT | 51.5% | 44 |  |
| Bene_CC_PH_Hyperlipidemia_V2_Pct | BIGINT | 36.2% | 75 |  |
| Bene_CC_PH_Hypertension_V2_Pct | BIGINT | 35.8% | 75 |  |
| Bene_CC_PH_IschemicHeart_V2_Pct | BIGINT | 45.8% | 48 |  |
| Bene_CC_PH_Osteoporosis_V2_Pct | BIGINT | 53.3% | 13 |  |
| Bene_CC_PH_Parkinson_V2_Pct | BIGINT | 73.3% | 4 |  |
| Bene_CC_PH_Arthritis_V2_Pct | BIGINT | 39.1% | 51 |  |
| Bene_CC_PH_Stroke_TIA_V2_Pct | BIGINT | 58.5% | 20 |  |
