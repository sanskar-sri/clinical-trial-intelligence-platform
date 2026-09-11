## Silver Layer

The Silver layer transforms Bronze master and reference datasets into
cleaned, standardized, deduplicated, and analytics-ready datasets.

The Silver layer is implemented using a separate Databricks Lakeflow
Declarative Pipeline and publishes curated materialized views to:

clinical_trial_intelligence.silver


### Silver Pipeline

Pipeline:
clinical-trial-silver

Source directory:

src/
└── pipelines/
    └── silver/
        └── dimensions.sql

Validation:

Validation/
└── silver/
    └── silver_validation.sql


### Silver Processing Logic

The Silver transformation layer currently performs:

- Data standardization using TRIM() on textual attributes
- Business-key validation
- NULL business-key filtering
- Deduplication using ROW_NUMBER()
- Latest-record selection based on `_ingestion_ts`
- Preservation of source lineage metadata
- Preservation of ingestion metadata
- Standardization of master datasets
- Standardization of reference/mapping datasets
- Bronze-to-Silver transformation through Lakeflow materialized views


### Silver Materialized Views

The current Silver pipeline defines 12 materialized views derived from
Bronze master, operational, and reference datasets.

| Silver Object | Bronze Source | Purpose |
|---|---|---|
| `dim_institution` | `bronze.master_institutions` | Standardized institution dimension |
| `dim_sponsor` | `bronze.master_sponsors` | Standardized sponsor dimension |
| `dim_product` | `bronze.master_products` | Standardized clinical product dimension |
| `dim_site` | `bronze.ctms_sites` | Standardized clinical trial site dimension |
| `dim_study` | `bronze.ctms_studies` | Standardized clinical study dimension |
| `dim_study_arm` | `bronze.protocol_study_arms` | Standardized protocol study-arm dimension |
| `ref_country_region` | `bronze.ref_country_region` | Standardized country-to-region mapping |
| `ref_diagnosis_mapping` | `bronze.ref_diagnosis_mapping` | Standardized diagnosis mapping |
| `ref_lab_test` | `bronze.ref_lab_test` | Standardized laboratory-test reference |
| `ref_severity_mapping` | `bronze.ref_severity_mapping` | Standardized severity mapping |
| `ref_sex_mapping` | `bronze.ref_sex_mapping` | Standardized sex mapping |
| `ref_unit_mapping` | `bronze.ref_unit_mapping` | Standardized laboratory-unit conversion mapping |


### Deduplication Strategy

Master and operational datasets can contain multiple versions of the
same business entity because new source files may arrive over time.

The Silver layer retains the latest record for each business key using:

ROW_NUMBER() OVER (
    PARTITION BY <business_key>
    ORDER BY _ingestion_ts DESC
)

Only records where:

rn = 1

are retained.

This makes the Silver dimension tables represent the latest available
version of each business entity.


### Data Lineage

Technical metadata from the Bronze layer is retained in Silver wherever
applicable:

- `_source_file`
- `_source_file_name`
- `_source_file_modification_ts`
- `_ingestion_ts`
- `_ingestion_date`

This provides traceability from a Silver record back to the source file
and ingestion event that produced it.


### Silver Data Flow

AWS S3 Landing
      |
      v
Bronze Lakeflow Pipeline
      |
      v
clinical_trial_intelligence.bronze
      |
      +------------------------------+
      |                              |
      v                              v
Master / Operational             Reference Data
Datasets                         Datasets
      |                              |
      +--------------+---------------+
                     |
                     v
             Silver Lakeflow Pipeline
                     |
               dimensions.sql
                     |
       +-------------+-------------+
       |                           |
       v                           v
   Dimensions                 Reference Views
       |                           |
       +-------------+-------------+
                     |
                     v
       clinical_trial_intelligence.silver


### Silver Validation

Silver validation queries are maintained separately in:

Validation/silver/silver_validation.sql

The validation framework checks:

- Record counts
- Business-key NULL values
- Duplicate business keys
- Sample records
- Expected Silver schema and output

For example, dimension validation verifies that business keys such as
`institution_id` and `sponsor_id` are populated and unique after
deduplication.


### Current Silver Status

- [x] Separate Silver Lakeflow pipeline created
- [x] Silver pipeline connected to Git-controlled source
- [x] Bronze-to-Silver transformations implemented
- [x] Institution dimension implemented
- [x] Sponsor dimension implemented
- [x] Product dimension implemented
- [x] Site dimension implemented
- [x] Study dimension implemented
- [x] Study-arm dimension implemented
- [x] Country-region reference implemented
- [x] Diagnosis mapping implemented
- [x] Lab-test reference implemented
- [x] Severity mapping implemented
- [x] Sex mapping implemented
- [x] Unit mapping implemented
- [x] 12 Silver materialized-view definitions successfully executed
- [x] `dim_institution` validation completed
- [x] `dim_sponsor` validation completed
- [ ] Validate remaining Silver dimensions
- [ ] Validate remaining Silver reference datasets
- [ ] Implement Silver transactional/clinical event transformations
- [ ] Add production data-quality expectations
- [ ] Build Gold analytical layer


### Current Medallion Architecture Status

Landing Layer
    |
    | COMPLETE
    v
Bronze Layer
    |
    | COMPLETE
    v
Silver Layer
    |
    | Dimension & Reference Layer IMPLEMENTED
    | Remaining validation IN PROGRESS
    | Transactional transformations PENDING
    v
Gold Layer
    |
    | PENDING
    v
BI / Analytics


# Clinical Trial Intelligence Platform

A production-style clinical trial data engineering platform built on
Databricks, Lakeflow Declarative Pipelines, Delta Lake and Unity Catalog.

The platform follows a medallion architecture to ingest raw clinical
operational data, standardize and validate it, quarantine defective
records, and produce business-ready analytical datasets.


## Architecture

Data Sources
     |
     v
Cloud Landing Zone
     |
     v
+----------------------+
|       BRONZE         |
| Raw ingestion layer  |
+----------------------+
     |
     | Streaming Tables
     | Materialized Views
     |
     v
Bronze Validation
     |
     v
+-------------------------------+
|            SILVER             |
| Clean + Standardize + Dedupe   |
+-------------------------------+
     |
     +--> Dimensions / References
     |       |
     |       +--> dim_institution
     |       +--> dim_sponsor
     |       +--> dim_product
     |       +--> dim_site
     |       +--> dim_study
     |       +--> dim_study_arm
     |       +--> ref_country_region
     |       +--> ref_geography
     |       +--> ref_diagnosis
     |       +--> ref_lab_test
     |       +--> ref_severity
     |       +--> ref_sex
     |       +--> ref_unit
     |
     +--> Clinical Entities [NEXT]
             |
             +--> subjects
             +--> visits
             +--> lab_results
             +--> adverse_events
             |
             +------ invalid records ------+
                                          |
                                          v
                                 +----------------+
                                 |   QUARANTINE   |
                                 | DQ violations |
                                 +----------------+
     |
     v
+----------------------+
|        GOLD          |
| Analytics / KPIs     |
+----------------------+
     |
     v
Dashboards / Reporting


## Unity Catalog Organization

The platform uses the following Unity Catalog namespace:

clinical_trial_intelligence
|
+-- bronze
|   Raw ingested source datasets
|
+-- silver
|   Cleaned, standardized and validated datasets
|
+-- quarantine
|   Records rejected by Silver data-quality rules
|
+-- gold
    Business-ready analytical datasets


Schema creation is maintained as code in:

src/setup/create_schemas.sql


## Bronze Layer

The Bronze layer preserves source data with minimal transformation while
adding ingestion and source-lineage metadata.

Two ingestion patterns are used depending on source characteristics.


### Streaming Sources

Append-oriented operational feeds are ingested as streaming tables:

- edc_subjects
- edc_visits
- lab_results
- safety_adverse_events


### Master and Reference Sources

Smaller master/reference datasets are maintained as materialized views:

- ctms_sites
- ctms_studies
- master_institutions
- master_products
- master_sponsors
- protocol_study_arms
- ref_country_region
- ref_diagnosis_mapping
- ref_geography
- ref_lab_test
- ref_severity_mapping
- ref_sex_mapping
- ref_unit_mapping


### Bronze Lineage Metadata

Bronze datasets preserve ingestion metadata including:

- _source_file
- _source_file_name
- _source_file_modification_ts
- _ingestion_ts
- _ingestion_date

This provides traceability from downstream records back to their
ingestion source.


## Bronze Validation

Bronze validation is maintained in:

src/Validation/bronze/bronze_validation.sql

Validation currently covers:

- dataset inventory
- row counts
- source-file lineage
- ingestion metadata completeness
- business-key inspection
- duplicate/version inspection

Bronze intentionally preserves source-level records. Deduplication and
business-rule enforcement are performed downstream in Silver.


## Silver Layer

The Silver layer converts Bronze source data into standardized,
deduplicated and reusable clinical data products.

The first completed Silver component is the dimension/reference layer.


### Silver Dimension Layer

The following 13 Silver datasets are currently implemented:

| Dataset | Business Key | Purpose |
|---|---|---|
| dim_institution | institution_id | Institution master |
| dim_sponsor | sponsor_id | Sponsor master |
| dim_product | product_id | Investigational/product master |
| dim_site | site_id | Clinical trial site |
| dim_study | study_id | Clinical study |
| dim_study_arm | study_id + arm_code | Protocol study arms |
| ref_country_region | country | Country-region mapping |
| ref_geography | city + country | Geographic hierarchy |
| ref_diagnosis | diagnosis_code | Diagnosis standardization |
| ref_lab_test | lab_test_code | Laboratory reference data |
| ref_severity | raw_severity | Severity standardization |
| ref_sex | raw_sex | Sex-value standardization |
| ref_unit | lab_test_code + raw_unit | Laboratory unit conversion |


### Silver Transformations

The dimension layer currently performs:

- whitespace normalization
- business-key validation
- deterministic deduplication using ROW_NUMBER()
- explicit numeric/date type establishment
- preservation of Bronze lineage metadata
- reference-data standardization
- geography enrichment


### Geographic Enrichment

`dim_site` is enriched using the governed Silver geography references.

Resolution follows:

city + country
      |
      v
ref_geography
      |
      | no city match
      v
ref_country_region
      |
      | no country match
      v
UNMAPPED

This prevents unresolved geography from silently disappearing from
regional analytical rollups.


## Silver Validation

Silver validation is maintained in:

src/Validation/silver/silver_validation.sql

The dimension health gate currently validates:

- unmapped site regions
- duplicate site business keys
- duplicate geography business keys

The final dimension health gate returned:

0 failed checks

Therefore, the Silver dimension/reference layer has passed its current
validation gate.


## Data Quality and Quarantine Design

The platform uses a dedicated Unity Catalog schema:

clinical_trial_intelligence.quarantine

The quarantine layer will contain records that cannot safely enter
validated Silver datasets because of data-quality or business-rule
violations.

The intended pattern is:

Bronze record
     |
     v
Standardization
     |
     v
Data Quality Rules
     |
     +---- PASS ----> Silver
     |
     +---- FAIL ----> Quarantine

This preserves defective records for investigation instead of silently
dropping them.


## Repository Structure

src/
|
+-- setup/
|   +-- create_schemas.sql
|
+-- pipelines/
|   |
|   +-- bronze/
|   |   +-- materialized/
|   |   +-- streaming/
|   |
|   +-- silver/
|       +-- dimensions.sql
|
+-- Utility/
|   +-- bronze_common.py
|   +-- bronze_materialized_common.py
|   +-- silver_common.py
|
+-- Validation/
    |
    +-- bronze/
    |   +-- bronze_validation.sql
    |
    +-- silver/
        +-- silver_validation.sql


## Current Project Status

Completed:

- Unity Catalog medallion structure
- Bronze ingestion framework
- Bronze streaming ingestion
- Bronze master/reference ingestion
- ingestion lineage metadata
- Bronze validation
- Silver dimension/reference layer
- business-key deduplication
- explicit Silver type standardization
- geographic reference ingestion
- site geographic enrichment
- Silver dimension health gate

In Progress:

- Silver clinical entity transformations

Next:

1. Create and verify quarantine schema
2. Build subjects transformation
3. Build visits transformation
4. Build laboratory results transformation
5. Build adverse-event transformation
6. Route invalid records to quarantine
7. Execute full Silver data-quality gate
8. Build Gold analytical models



## Silver Layer

The Silver layer transforms Bronze clinical-trial data into cleaned,
standardized, deduplicated, typed, and analytically reusable datasets.

The Silver pipeline is implemented using Databricks Lakeflow Declarative
Pipelines and follows two processing patterns:

1. Master/reference data → Silver dimensions and reference datasets
2. Clinical transactional data → validated Silver entities with quarantine
   handling and current-state processing

---

### Silver Dimension and Reference Layer

Status: COMPLETE

The dimension layer processes relatively stable master and reference datasets
from the Bronze layer.

Bronze sources are cleaned by:

- trimming business keys and descriptive attributes
- rejecting blank business keys
- deduplicating records using business keys
- retaining the latest ingested master/reference record
- establishing appropriate Silver data types
- standardizing reusable reference mappings
- retaining ingestion lineage metadata

The following Silver datasets are currently produced:

| Silver Dataset | Bronze Source | Business Key |
|---|---|---|
| `dim_institution` | `master_institutions` | `institution_id` |
| `dim_sponsor` | `master_sponsors` | `sponsor_id` |
| `dim_product` | `master_products` | `product_id` |
| `dim_site` | `ctms_sites` | `site_id` |
| `dim_study` | `ctms_studies` | `study_id` |
| `dim_study_arm` | `protocol_study_arms` | `study_id + arm_code` |
| `ref_country_region` | `ref_country_region` | `country` |
| `ref_diagnosis` | `ref_diagnosis_mapping` | `diagnosis_code` |
| `ref_lab_test` | `ref_lab_test` | `lab_test_code` |
| `ref_severity` | `ref_severity_mapping` | `raw_severity` |
| `ref_sex` | `ref_sex_mapping` | `raw_sex` |
| `ref_unit` | `ref_unit_mapping` | `lab_test_code + raw_unit` |
| `ref_geography` | `ref_geography` | `city + country` |

---

### Geography Enrichment

`dim_site` is enriched with a standardized region during Silver processing.

Region resolution follows:

1. city + country lookup against `ref_geography`
2. country-level fallback against `ref_country_region`
3. unresolved geography is explicitly represented as `UNMAPPED`

This prevents NULL geography from silently disappearing from downstream
regional aggregations.

Conceptually:

Bronze CTMS Site
        |
        +---- ref_geography (city + country)
        |
        +---- ref_country_region (country fallback)
        |
        v
Silver dim_site
        |
        +---- city
        +---- country
        +---- region

The final dimension quality gate verifies:

- no unmapped site regions
- no duplicate `site_id`
- no duplicate city/country keys in `ref_geography`

The completed validation returned zero failed checks.

---

### Silver Schema Setup

The project catalog contains the following processing schemas:

- `bronze` — raw and minimally transformed source data
- `silver` — cleaned, standardized and validated data
- `quarantine` — records rejected by Silver data-quality rules
- `gold` — business-ready analytical data products

The schemas are reproducibly created through:

`src/setup/create_schemas.sql`

The quarantine schema is intentionally separated from Silver so invalid
clinical records remain observable and auditable rather than being silently
discarded.

---

## Subject CDC Investigation

Status: COMPLETE

The EDC subject feed contains repeated versions of the same `subject_id`
across multiple source files.

Example:

subject_id
    |
    +---- subjects_20260825.csv
    +---- subjects_20260828.csv
    +---- subjects_20260904.csv

Therefore, the EDC subject feed cannot be treated as a simple append-only
dataset.

### CDC Ordering Investigation

`_ingestion_ts` was evaluated first as a possible sequencing field.

It was rejected because records from multiple source files received the same
Bronze ingestion timestamp.

`_source_file_modification_ts` was also evaluated.

It was rejected because file modification order did not consistently match
the logical snapshot order encoded in the filenames.

For example, an older logical snapshot could have a later filesystem
modification timestamp than a newer snapshot.

The logical source date is therefore extracted from the source filename:

`subjects_YYYYMMDD.csv`

Example:

`subjects_20260904.csv` → `2026-09-04`

This produces:

`source_snapshot_date`

which represents the logical ordering of EDC subject snapshots.

### CDC Sequence Validation

A uniqueness test was performed on:

`subject_id + source_snapshot_date`

Expected result:

`ZERO ROWS`

Actual result:

`ZERO ROWS`

Therefore, each subject has at most one record for a given logical snapshot
date.

The Subject processing contract is consequently:

| Property | Value |
|---|---|
| Business key | `subject_id` |
| Sequence field | `source_snapshot_date` |
| Current-state strategy | SCD Type 1 |
| Source | `bronze.edc_subjects` |
| Valid target | `silver.subjects` |
| Invalid target | `quarantine.subjects` |

---

## Current Silver Architecture

Landing / Source Files
        |
        v
Bronze
        |
        +---------------------------+
        |                           |
        v                           v
Master / Reference              Clinical Feeds
        |                           |
        v                           |
Silver Dimensions                  |
and References                     |
        |                           |
        +-------------+-------------+
                      |
                      v
              Standardization
              Reference Validation
              Business/DQ Rules
                      |
                +-----+-----+
                |           |
                v           v
              Valid       Invalid
                |           |
                v           v
             Silver     Quarantine
                |
                v
              Gold

---

## Silver Development Status

Completed:

- Bronze ingestion
- Bronze validation
- Silver schema setup
- Quarantine schema setup
- Silver dimensions
- Silver reference mappings
- geography enrichment
- dimension deduplication
- Silver type standardization
- dimension quality gate
- Subject CDC investigation
- Subject sequencing strategy

Next:

1. implement `subjects.py`
2. validate `silver.subjects`
3. validate `quarantine.subjects`
4. implement visits
5. implement laboratory results
6. implement adverse events
7. run final Silver reconciliation and health gate
8. proceed to Gold analytical models