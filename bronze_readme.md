# Clinical Trial Intelligence Platform

A production-oriented clinical trial data platform built using AWS S3,
Databricks, Unity Catalog, Delta Lake, and Lakeflow Spark Declarative
Pipelines.

The platform follows a Medallion Architecture:

S3 Landing
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Clinical Trial Analytics / BI

---

# Bronze Layer

## Purpose

The Bronze layer provides the raw ingestion layer of the Clinical Trial
Intelligence Platform.

Source data is delivered to Amazon S3 under the `Landing/` prefix and
ingested into the Unity Catalog schema:

`clinical_trial_intelligence.bronze`

The Bronze layer preserves source records while adding operational
metadata required for lineage, auditing, and downstream processing.

---

## Architecture

Amazon S3 Landing
        |
        +-----------------------------+
        |                             |
        v                             v
 Streaming / Incremental          Reference / Master
      Sources                         Sources
        |                             |
   Auto Loader                    Batch Read
        |                             |
        v                             v
 Streaming Tables              Materialized Views
        |                             |
        +-------------+---------------+
                      |
                      v
        clinical_trial_intelligence.bronze

---

## Source Classification

### Streaming / Incremental Sources

The following datasets represent operational/event data and are ingested
incrementally using Databricks Auto Loader.

| Dataset | Bronze Table |
|---|---|
| EDC Subjects | edc_subjects |
| EDC Visits | edc_visits |
| Laboratory Results | lab_results |
| Safety Adverse Events | safety_adverse_events |

Auto Loader provides incremental file discovery and maintains ingestion
state so newly arriving files can be processed without rebuilding the
entire dataset.

### Materialized Reference / Master Sources

Relatively small master, CTMS, protocol, and reference datasets are
processed using batch semantics and exposed as materialized views.

Examples include:

- ctms_sites
- ctms_studies
- master_institutions
- master_products
- master_sponsors
- protocol_study_arms
- ref_country_region
- ref_diagnosis_mapping
- ref_lab_test
- ref_severity_mapping
- ref_sex_mapping
- ref_unit_mapping

---

## Bronze Metadata

Operational metadata is added during ingestion.

| Column | Purpose |
|---|---|
| `_source_file` | Full source object path |
| `_source_file_name` | Source file name |
| `_source_file_modification_ts` | Source file modification timestamp |
| `_ingestion_ts` | Databricks ingestion timestamp |
| `_ingestion_date` | Databricks ingestion date |

These columns provide traceability between Bronze records and their
original S3 source files.

---

## Current Bronze Validation

### Streaming Tables

| Table | Rows |
|---|---:|
| edc_subjects | 3,829 |
| edc_visits | 18,262 |
| lab_results | 65,006 |
| safety_adverse_events | 1,952 |

### Streaming File Lineage

| Table | Source Files Seen |
|---|---:|
| edc_subjects | 9 |
| edc_visits | 10 |
| lab_results | 10 |
| safety_adverse_events | 10 |

Source metadata validation confirmed that Bronze records contain the
required ingestion and source lineage information.

---

## Bronze Design Principles

The Bronze layer intentionally performs minimal business transformation.

Responsibilities:

- Incremental ingestion
- Raw data preservation
- Source-file lineage
- Ingestion timestamping
- Schema capture
- Replayability
- Auditability

Data cleansing, standardization, deduplication, business validation,
reference mapping, and cross-source integration are intentionally
performed in the Silver layer.

---

# Silver Layer

The Silver layer converts Bronze data into validated, standardized,
deduplicated, and analytically usable clinical datasets.

Planned Silver responsibilities include:

- Data type standardization
- Null handling
- Date and timestamp normalization
- String normalization
- Duplicate detection/removal
- Business-key validation
- Referential integrity validation
- Standardization using reference datasets
- Clinical data-quality rules
- Quarantine/rejection handling
- Cross-source entity reconciliation

The Silver layer reads from Bronze rather than directly from S3.

---

# Technology Stack

- Amazon S3
- Databricks
- Apache Spark
- PySpark
- Lakeflow Spark Declarative Pipelines
- Auto Loader
- Delta Lake
- Unity Catalog
- SQL
- Git