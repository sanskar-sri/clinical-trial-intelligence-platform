# Clinical Trial Intelligence Platform

An end-to-end clinical trial data engineering platform built using **AWS S3, Databricks, Apache Spark / PySpark, Delta Lake, Unity Catalog, Lakeflow Declarative Pipelines, and Databricks AI/BI**.

The platform ingests synthetic operational data from multiple clinical-trial source systems, processes it through a governed Bronze → Silver → Gold medallion architecture, applies reference standardisation and clinical data-quality controls, preserves historical subject changes using AUTO CDC / SCD Type 2, quarantines records that cannot be trusted, and publishes analytical datasets for study, site, subject, visit, safety, laboratory, and data-quality monitoring.

> **Data disclaimer.** All data in this repository is synthetically generated. No real patient, clinical-trial subject, site, sponsor, institution, or organisation data is used.

---

## Contents

| # | Section | # | Section |
|---|---|---|---|
| 1 | [Why this project exists](#1-why-this-project-exists) | 16 | [Clinical modelling boundaries](#16-clinical-modelling-boundaries) |
| 2 | [End-to-end architecture](#2-end-to-end-architecture) | 17 | [Dashboard](#17-dashboard) |
| 3 | [Technology stack](#3-technology-stack) | 18 | [Exploration and design proof](#18-exploration-and-design-proof) |
| 4 | [AWS → Databricks security and storage](#4-aws--databricks-security-and-storage-configuration) | 19 | [Validation strategy](#19-validation-strategy) |
| 5 | [Unity Catalog governance](#5-unity-catalog-governance) | 20 | [Orchestration](#20-orchestration) |
| 6 | [Source landscape](#6-source-landscape) | 21 | [Repository structure](#21-repository-structure) |
| 7 | [Bronze layer](#7-bronze-layer) | 22 | [Current project status](#22-current-project-status) |
| 8 | [Silver layer](#8-silver-layer) | 23 | [Remaining engineering work](#23-remaining-engineering-work) |
| 9 | [Business-key normalisation](#9-business-key-normalisation) | 24 | [Scope and limitations](#24-scope-and-limitations) |
| 10 | [Reference standardisation](#10-reference-standardisation) | 25 | [Reproducing the platform](#25-reproducing-the-platform) |
| 11 | [Subject historical processing](#11-subject-historical-processing--auto-cdc--scd-type-2) | 26 | [Security principles demonstrated](#26-security-principles-demonstrated) |
| 12 | [Visits, lab results, adverse events](#12-visits-laboratory-results-and-adverse-events) | 27 | [Demo](#27-demo) |
| 13 | [Referential integrity](#13-referential-integrity) | 28 | [Roadmap](#28-roadmap) |
| 14 | [Data quality and quarantine](#14-data-quality-and-quarantine) | 29 | [License](#29-license) |
| 15 | [Gold analytical layer](#15-gold-analytical-layer) | | |

---

## 1. Why this project exists

Clinical trial operations generate data across multiple independent systems:

- **EDC** captures subject and visit activity.
- **CTMS** contains study and site operational data.
- **Laboratory** systems provide clinical measurements.
- **Safety** systems capture adverse events.
- **Master and reference** datasets provide sponsors, products, institutions, geography, mappings, and controlled terminology.

These sources arrive with different delivery patterns, schemas, terminology, and data-quality characteristics.

The engineering challenge is therefore not simply moving CSV files into tables. The objective is to build a governed platform where:

- incoming source data remains traceable to its original delivery;
- AWS access is controlled without embedding credentials in pipeline code;
- business keys are standardised consistently;
- reference data is resolved deterministically;
- invalid records remain visible through quarantine rather than being silently dropped;
- historical subject changes can be reconstructed;
- downstream analytical datasets use trusted data;
- and analytical outputs can be reconciled through Silver and Bronze to their source.

---

## 2. End-to-end architecture

```text
    EDC        CTMS      Laboratory      Safety     Master / Reference
     │           │            │             │                │
     └───────────┴────────────┴─────────────┴────────────────┘
                              │
                              ▼
                      AWS S3 Landing Zone
                              │
                              ▼
                         AWS IAM Role
                              │
                      Trust Relationship
                        + External ID
                              │
                              ▼
                        Unity Catalog
                              │
                ┌─────────────┴─────────────┐
                │                           │
        Storage Credential          External Location
                │                           │
                └─────────────┬─────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │             BRONZE            │
              │                               │
              │  Auto Loader +                │
              │  Materialized Views           │
              │                               │
              │  Source preservation          │
              │  Ingestion lineage            │
              └───────────────┬───────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │             SILVER            │
              │                               │
              │  Type standardisation         │
              │  Key normalisation            │
              │  Reference resolution         │
              │  Referential integrity        │
              │  Clinical DQ rules            │
              │  AUTO CDC / SCD Type 2        │
              └───────┬───────────────┬───────┘
                      │               │
                    VALID          INVALID
                      │               │
                      ▼               ▼
                   SILVER         QUARANTINE
                      │
                      ▼
              ┌───────────────────────────────┐
              │              GOLD             │
              │                               │
              │  Business-ready analytical    │
              │  datasets                     │
              └───────────────┬───────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │        Databricks AI/BI       │
              │                               │
              │  Clinical Trial Intelligence  │
              │  & Risk Monitoring            │
              └───────────────────────────────┘
```

**Cross-layer execution:**

```text
Bronze Pipeline
      │
      ▼
Silver Pipeline
      │
      ▼
Gold Pipeline
      │
      ▼
AI/BI Dashboard
```

Transformation logic and workflow orchestration are deliberately separated. Individual Lakeflow Declarative Pipelines define transformations and dataset dependencies within each medallion layer, while a Lakeflow Job coordinates execution across Bronze, Silver, and Gold.

---

## 3. Technology stack

| Area | Technology |
|---|---|
| Cloud | AWS |
| Object storage | Amazon S3 |
| Cloud access | AWS IAM role |
| Governed storage access | Unity Catalog storage credential |
| Storage abstraction | Unity Catalog external location |
| Data platform | Databricks |
| Processing | Apache Spark / PySpark / SQL |
| Ingestion | Databricks Auto Loader (`cloudFiles`) |
| Pipeline framework | Lakeflow Declarative Pipelines |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Historical processing | AUTO CDC / SCD Type 2 |
| Orchestration | Lakeflow Jobs |
| Analytics | Databricks AI/BI |
| Version control | Git / GitHub |
| Deployment as code | Declarative Automation Bundles — *in progress* |
| CI/CD | GitHub Actions — *in progress* |

---

## 4. AWS → Databricks security and storage configuration

A major design requirement was to allow Databricks to access Amazon S3 without embedding AWS access keys or secret access keys in notebooks or pipeline code.

The integration uses:

```text
AWS S3
   ▲
   │
AWS IAM Role
   ▲
   │  AssumeRole
   │  + External ID
   │
Unity Catalog Storage Credential
   ▲
   │
Unity Catalog External Location
   ▲
   │
Databricks Workloads
```

### 4.1 S3 storage layout

The project separates externally landed source data from Unity Catalog-managed analytical storage.

```text
s3://<project-bucket>/
│
├── Landing/                 externally delivered source files
│   ├── CTMS/
│   ├── EDC/
│   ├── Lab/
│   ├── Safety/
│   ├── master/
│   ├── protocol/
│   └── reference/
│
└── UnityManaged/            governed analytical storage
    ├── bronze/
    ├── silver/
    ├── quarantine/
    └── gold/
```

### 4.2 AWS IAM role

An AWS IAM role provides Databricks with controlled access to the required S3 resources.

```text
AWS IAM Role
     │
     ├── Permissions Policy
     │        │
     │        └── Required S3 permissions
     │
     └── Trust Policy
              │
              ├── Databricks-authorised principal
              ├── Self-assumption configuration
              └── External ID condition
```

The IAM permissions define which S3 resources the role can access. The trust relationship controls who is allowed to assume the role. No long-lived AWS access key or secret access key is stored in transformation code.

### 4.3 Trust relationship and external ID

The IAM role contains a trust relationship that allows the appropriate Databricks identity to assume the role. The Unity Catalog storage credential generates an external ID, which is included in the IAM role trust relationship.

```text
Databricks / Unity Catalog
          │
          │  sts:AssumeRole
          │  + External ID
          ▼
      AWS IAM Role
          │
          ▼
   Authorised S3 Paths
```

The external ID strengthens the cross-account trust relationship by associating role assumption with the intended storage credential configuration.

> Actual AWS account IDs, external IDs, credentials, and other security-sensitive identifiers are not published in this repository.

### 4.4 Unity Catalog storage credential

The IAM role is represented inside Unity Catalog through a storage credential.

```text
AWS IAM Role
      │
      ▼
Unity Catalog
Storage Credential
```

The storage credential defines the cloud identity Unity Catalog uses when accessing authorised S3 locations. This keeps AWS identity configuration separate from application and pipeline code.

### 4.5 Unity Catalog external location

The S3 storage path is registered with Unity Catalog through an external location.

```text
External Location
       │
       ├── S3 Path
       │
       └── Storage Credential
                 │
                 ▼
              IAM Role
```

This separates the physical cloud-storage path, the cloud identity authorised to access it, and the Unity Catalog privileges controlling which workloads and users can use that location.

### 4.6 End-to-end access flow

```text
Databricks Workload
        │
        ▼
   Unity Catalog
        │
        ▼
 External Location
        │
        ▼
 Storage Credential
        │
        ▼
   AWS IAM Role
        │
   AssumeRole +
   External ID
        │
        ▼
      AWS S3
```

This creates a governed access boundary between Databricks workloads and AWS storage.

---

## 5. Unity Catalog governance

The platform uses the catalog `clinical_trial_intelligence` with four logical schemas:

```text
clinical_trial_intelligence
│
├── bronze
├── silver
├── quarantine
└── gold
```

Consumers interact with governed objects such as `clinical_trial_intelligence.bronze.edc_subjects` rather than depending on physical storage paths. Unity Catalog therefore provides the governed interface between cloud storage, transformation pipelines, and downstream consumers.

---

## 6. Source landscape

The S3 landing-zone exploration identified seven source families:

```text
Landing/
│
├── CTMS/
├── EDC/
├── Lab/
├── Safety/
├── master/
├── protocol/
└── reference/
```

At the latest completed Bronze landscape exploration:

- 7 source families were accessible;
- 18 dataset-level source objects were identified;
- 55 physical source files were present.

The number of physical landing objects should not be confused with the number of production Bronze datasets, because supporting landing artifacts can exist without becoming independent Bronze tables.

### Source systems

| Source | Data |
|---|---|
| EDC | Subjects, visits |
| CTMS | Studies, sites |
| Laboratory | Laboratory results |
| Safety | Adverse events |
| Master | Institutions, products, sponsors |
| Protocol | Study arms |
| Reference | Geography and clinical/reference mappings |

Recurring clinical feeds use date-versioned filenames.

The exploration also showed that S3 object modification timestamps should not automatically be treated as logical source-delivery order. Dates embedded in recurring source filenames provide an important logical delivery indicator.

---

## 7. Bronze layer

> **Bronze answers:** what did the source system send?

Bronze preserves source representation and ingestion lineage rather than performing business harmonisation during ingestion.

### Incremental Auto Loader feeds

Four recurring feeds are processed incrementally:

```text
edc_subjects
edc_visits
lab_results
safety_adverse_events
```

### Materialized supporting sources

CTMS, master, protocol, and reference datasets are processed as relatively small supporting datasets: studies, sites, institutions, products, sponsors, study arms, geography mappings, diagnosis mappings, laboratory-test references, severity mappings, sex mappings, unit mappings, and country/region mappings.

### Bronze lineage

Every Bronze record retains ingestion metadata:

```text
_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date
```

This enables downstream records to be traced back to the physical source delivery that produced them.

Bronze deliberately avoids business correction so that source anomalies remain distinguishable from transformation behaviour.

---

## 8. Silver layer

> **Silver answers:** what is the standardised and trustworthy representation of the source data?

```text
Type handling
      ↓
Business-key normalisation
      ↓
Reference standardisation
      ↓
Referential-integrity validation
      ↓
Clinical business rules
      ↓
Valid / Quarantine separation
      ↓
Historical processing where required
```

---

## 9. Business-key normalisation

Primary and foreign business keys use a shared transformation contract implemented in the Silver utility layer.

```text
TRIM
  ↓
blank / "-" → NULL
  ↓
UPPER
```

Examples:

```text
" sub-001 "  →  "SUB-001"
""           →  NULL
"-"          →  NULL
```

Centralising this behaviour ensures that the same business key receives identical normalisation semantics across subjects, visits, laboratory results, adverse events, dimensions, and reference joins.

---

## 10. Reference standardisation

Reference datasets can contain semantically equivalent values with different physical representations, for example `M`, `m`, `Male`, `MALE`.

Joining clinical data against an unresolved reference dataset after normalisation can create one-to-many matches. The Silver design therefore follows:

```text
Raw Reference
      ↓
Key Normalisation
      ↓
Deterministic Resolution
      ↓
Unique Reference Representation
      ↓
Clinical Fact Join
```

Unresolved values remain visible through controlled representations such as `UNMAPPED` where appropriate, rather than silently disappearing from downstream processing.

---

## 11. Subject historical processing — AUTO CDC / SCD Type 2

The subject feed behaves differently from the other clinical feeds. Observed source deliveries consist of an initial subject population followed by smaller incremental change files rather than independent full snapshots. Subject processing therefore uses AUTO CDC with SCD Type 2.

```text
Bronze Subject Deliveries
          │
          ▼
  Standardisation + DQ
          │
          ▼
       AUTO CDC
          │
          ▼
      SCD Type 2
          │
          ▼
Historical Subject State
```

**Business key:** `subject_id`

### Deterministic source ordering

Source changes are sequenced using logical source-delivery information rather than relying solely on ingestion time:

```python
struct(
    source_snapshot_date,
    _source_file_name
)
```

where `source_snapshot_date` is derived from the source filename. This matters because multiple files can be processed during the same pipeline execution, making ingestion timestamp alone unsuitable for deterministic source ordering.

### History semantics

Technical ingestion metadata should not create false clinical history. A subject should not accumulate:

```text
ENROLLED
   ↓
ENROLLED
   ↓
ENROLLED
```

simply because the same business state appeared in several source deliveries. History therefore represents meaningful source/business-state change rather than ingestion-file noise.

---

## 12. Visits, laboratory results and adverse events

Each clinical entity was evaluated according to its observed source behaviour instead of automatically copying the subject CDC strategy.

### Visits

**Grain:** 1 row = 1 subject visit

Observed `visit_id` behaviour supports append-oriented processing. Silver validation includes subject relationships, study/site consistency, status-aware date validation, and relevant clinical rules.

### Laboratory results

**Grain:** 1 row = 1 laboratory measurement

Measurements can arrive in different units and are standardised through laboratory-test and unit reference data:

```text
standardized_result_value  =  result_value × conversion_factor
```

Both source and standardised representations are retained. This allows derived abnormality to be compared with the source-provided abnormality indicator rather than silently replacing source information.

### Adverse events

**Grain:** 1 row = 1 adverse event

Severity and seriousness are modelled separately. Severity represents event intensity, while seriousness is a separate clinical/regulatory characteristic. The platform therefore does not infer regulatory seriousness solely from severity.

---

## 13. Referential integrity

Clinical fact records are validated against trusted Silver entities rather than raw Bronze data.

```text
Bronze Subject
      │
      ▼
Silver Validation
      │
      ├── Valid ──────► Silver Subject
      │
      └── Invalid ────► Quarantine


Visit / Lab / AE
      │
      ▼
Validate relationships against
    trusted Silver entities
```

This prevents an invalid raw subject record from legitimising downstream clinical facts. For historical subjects, downstream relationships use the appropriate trusted/current Silver representation where required.

---

## 14. Data quality and quarantine

The platform distinguishes between records that cannot safely participate in trusted analytics and records that are unusual but still clinically plausible.

```text
DQ Failure                 Clinical Warning
    │                             │
    ▼                             ▼
QUARANTINE                  SILVER + FLAG
```

Blocking rules can include:

- missing critical business keys;
- unknown study/site/subject relationships;
- referential-integrity failures;
- cross-entity inconsistencies;
- invalid ranges;
- unmappable required reference values;
- impossible date sequences.

Warnings represent unusual but potentially legitimate conditions that should remain available for review.

### Quarantine design

Quarantined records preserve:

```text
Original business attributes
          +
Source lineage
          +
All triggered DQ rules
          +
Human-readable failure information
          +
Quarantine timestamp
```

The `_dq_failures` representation preserves all applicable failed rules rather than only the first failure encountered. This makes quarantine an auditable data product rather than a discarded-record bucket.

---

## 15. Gold analytical layer

> **Gold answers:** what does the business need from trusted clinical data?

The Gold layer contains business-ready analytical datasets supporting:

- subject analytical spine;
- subject-level summary;
- subject disposition;
- discontinuation analysis;
- enrolment trends;
- site performance;
- visit compliance;
- safety monitoring;
- laboratory monitoring;
- data-quality monitoring;
- Bronze → Silver → quarantine reconciliation.

Gold datasets are built from trusted Silver representations rather than directly from raw Bronze data.

---

## 16. Clinical modelling boundaries

The platform deliberately avoids deriving clinical variables that cannot be supported by the available source data.

**TEAE.** A defensible treatment-emergent adverse-event flag requires an appropriate treatment/exposure start timestamp. Randomisation alone should not automatically be interpreted as first treatment exposure.

**Safety population.** A safety-population flag should not automatically be interpreted as "received treatment" without appropriate exposure information.

**Exposure-adjusted event rates.** These require a defensible exposure or follow-up denominator.

Where CDISC concepts influence the analytical model, they are treated as design inspiration rather than being presented as evidence of regulatory compliance.

> **Modelling principle:** do not manufacture precision that the source data cannot support.

---

## 17. Dashboard

The Gold layer powers the **Clinical Trial Intelligence & Risk Monitoring** Databricks AI/BI dashboard, containing five analytical areas:

1. Study Overview
2. Enrolment & Site Performance
3. Patient & Visit Monitoring
4. Safety & Lab Monitoring
5. Data Quality & Operational Risk

The dashboard demonstrates how governed engineering outputs support operational clinical-trial monitoring rather than functioning as an isolated visualisation layer.

---

## 18. Exploration and design proof

Important pipeline decisions are supported by source exploration rather than assumptions about source behaviour. The exploration architecture deliberately separates three questions:

```text
BRONZE
"How does this data arrive?"
        │
        ▼
SILVER
"What does this data mean?"
        │
        ▼
GOLD
"How should trusted data become analytical metrics?"
```

Target exploration structure:

```text
src/notebooks/exploration/
│
├── bronze/
│   ├── 00_bronze_source_landscape
│   │
│   ├── streaming/
│   │   ├── 01_edc_source_exploration
│   │   ├── 02_lab_source_exploration
│   │   └── 03_safety_source_exploration
│   │
│   ├── materialized/
│   │   ├── 04_ctms_source_exploration
│   │   └── 05_master_reference_source_exploration
│   │
│   └── 06_bronze_ingestion_design
│
├── silver/
│   ├── 00_dimensions_reference_exploration
│   ├── 01_subject_exploration
│   ├── 02_visits_exploration
│   ├── 03_lab_results_exploration
│   └── 04_adverse_events_exploration
│
└── gold/
    └── 00_gold_metric_design
```

These notebooks make engineering decisions reviewable instead of leaving their justification implicit inside production code.

---

## 19. Validation strategy

Validation is separated from transformation logic. It covers:

```text
Bronze → Silver reconciliation
Business-key uniqueness
SCD current-record uniqueness
Reference mapping integrity
Cross-entity referential integrity
Quarantine volumes
DQ failure distribution
NULL behaviour
Source-vs-standardised measurements
Gold reconciliation
```

Final numerical results are published only after being reproduced by the corresponding exploration or validation workflow. This prevents stale pipeline-run metrics from being presented as the current platform state.

---

## 20. Orchestration

The medallion layers execute as independent Lakeflow Declarative Pipelines.

```text
clinical-trial-bronze
        │
        │  all-succeeded
        ▼
clinical-trial-silver
        │
        │  all-succeeded
        ▼
clinical-trial-gold
```

A Lakeflow Job manages the cross-pipeline dependency. This separation is deliberate:

| Object | Expresses |
|---|---|
| Pipeline | Dataset transformation / dependency graph |
| Job | Operational task / workflow dependency graph |

Transformation logic therefore remains independent from workflow orchestration.

---

## 21. Repository structure

The repository is being consolidated toward the following production-oriented structure:

```text
clinical-trial-intelligence-platform/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
│
├── dashboard/
│   └── Clinical Trial Intelligence & Risk Monitoring.lvdash.json
│
├── data/
│   └── landing/
│       ├── ctms/
│       ├── edc/
│       ├── lab/
│       ├── safety/
│       ├── master/
│       ├── protocol/
│       └── reference/
│
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   └── deployment.md
│
├── resources/
│   ├── bronze.pipeline.yml
│   ├── silver.pipeline.yml
│   ├── gold.pipeline.yml
│   ├── orchestration.job.yml
│   └── dashboard.yml
│
├── src/
│   ├── __init__.py
│   │
│   ├── setup/
│   │   └── unity_catalog_schema_setup.ipynb
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── bronze_common.py
│   │   ├── bronze_materialized_common.py
│   │   └── silver_common.py
│   │
│   ├── pipelines/
│   │   ├── bronze/
│   │   │   ├── streaming/
│   │   │   └── materialized/
│   │   ├── silver/
│   │   └── gold/
│   │
│   └── notebooks/
│       ├── exploration/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       │
│       └── validation/
│           ├── bronze/
│           ├── silver/
│           ├── gold/
│           └── demo/
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── test_silver_common.py
│
├── .gitignore
├── LICENSE
├── README.md
├── databricks.yml
├── pyproject.toml
└── requirements-dev.txt
```

The repository separates three engineering concerns:

| Directory | Question it answers |
|---|---|
| `notebooks/exploration/` | What does the source data demonstrate? |
| `pipelines/` | What should the production platform do to it? |
| `notebooks/validation/` | Did the implementation produce the expected result? |

---

## 22. Current project status

### Core platform — implemented

```text
AWS IAM / Unity Catalog
          ↓
        AWS S3
          ↓
        Bronze
          ↓
 Silver + Quarantine
          ↓
         Gold
          ↓
   AI/BI Dashboard
```

Implemented components:

- synthetic multi-source clinical-trial data;
- AWS S3 landing architecture;
- AWS IAM role-based S3 access;
- IAM trust relationship and external-ID configuration;
- Unity Catalog storage credential;
- Unity Catalog external location;
- governed Bronze / Silver / Quarantine / Gold schemas;
- incremental Auto Loader ingestion;
- materialized supporting datasets;
- ingestion lineage;
- shared business-key normalisation;
- deterministic reference resolution;
- referential-integrity validation;
- clinical data-quality rules;
- quarantine processing;
- subject AUTO CDC / SCD Type 2;
- entity-specific visit / lab / AE processing;
- Gold analytical datasets;
- Bronze → Silver → Gold orchestration;
- Databricks AI/BI dashboard;
- validation and reconciliation logic.

### Repository hardening — in progress

```text
Complete Exploration
        ↓
Final Validation
        ↓
Freeze Repository Structure
        ↓
Repository Cleanup
        ↓
Automated Tests
        ↓
Declarative Automation Bundle
        ↓
GitHub Actions CI/CD
        ↓
Clean Deployment Validation
        ↓
Final Documentation
```

---

## 23. Remaining engineering work

**Exploration.** Complete the structured Bronze, Silver, and Gold exploration notebooks and ensure important implementation decisions are backed by reproducible evidence.

**Final validation.** Reproduce final Bronze volumes, Silver valid/quarantine counts, SCD current/history checks, reference conflict checks, Gold reconciliation, and source-to-target balance.

**Repository hygiene.** Remove temporary development artifacts; standardise directory naming; remove system-generated files; add `.gitignore`; freeze package paths; ensure documentation matches the actual filesystem.

**Automated testing.** Add automated tests around reusable transformation contracts, particularly:

```text
" subj-001 "  →  "SUBJ-001"
"-"           →  NULL
""            →  NULL
```

**Deployment as code.** Represent Databricks resources using Declarative Automation Bundles:

```text
databricks.yml
       │
       └── resources/
              ├── bronze.pipeline.yml
              ├── silver.pipeline.yml
              ├── gold.pipeline.yml
              ├── orchestration.job.yml
              └── dashboard.yml
```

**CI/CD.** Add GitHub Actions:

```text
Feature Branch
      │
      ▼
 Pull Request
      │
      ▼
Lint + Tests
      │
      ▼
Bundle Validation
      │
      ▼
    Merge
      │
      ▼
Controlled Deployment
```

**Final reproducibility.** Validate the project from a clean repository checkout so that documented setup and deployment instructions match the actual implementation.

---

## 24. Scope and limitations

- All source data is synthetic.
- No real patient or clinical-trial participant data is used.
- No AWS access keys or secret keys are stored in transformation code.
- Security-sensitive AWS account identifiers, external IDs, credentials, and trust-policy values are not published.
- The subject source feed does not currently provide explicit delete semantics.
- Visits, laboratory results, and adverse events are processed according to their observed source behaviour; this assumption should be revalidated if the source contract changes.
- Clinical variables requiring genuine treatment/exposure information are not fabricated from weaker proxy fields.
- Final numerical results will be refreshed after completion of the current exploration and validation pass.
- Automated CI/CD and deployment-as-code are part of the current repository-hardening phase.

---

## 25. Reproducing the platform

1. Create an AWS S3 bucket for landing and governed storage.
2. Upload the synthetic landing datasets.
3. Create an AWS IAM role with the required S3 permissions.
4. Configure the IAM trust relationship for Databricks role assumption.
5. Create the Unity Catalog storage credential using the IAM role ARN.
6. Obtain the storage credential external ID.
7. Update the IAM trust policy with the generated external ID and required self-assumption configuration.
8. Validate the storage credential.
9. Create the Unity Catalog external location using the storage credential and S3 path.
10. Configure the catalog and Bronze, Silver, Quarantine, and Gold schemas.
11. Configure the Bronze Lakeflow Declarative Pipeline.
12. Configure the Silver Lakeflow Declarative Pipeline.
13. Configure the Gold Lakeflow Declarative Pipeline.
14. Create the Lakeflow Job that sequences Bronze → Silver → Gold.
15. Import and configure the Databricks AI/BI dashboard.
16. Run the validation and reconciliation workflows.

Conceptually:

```text
AWS S3
   ↓
IAM Permissions
   ↓
IAM Trust Policy
   ↓
Unity Catalog Storage Credential
   ↓
Generated External ID
   ↓
Updated IAM Trust Relationship
   ↓
Storage Credential Validation
   ↓
Unity Catalog External Location
   ↓
Catalog + Schemas
   ↓
Bronze
   ↓
Silver + Quarantine
   ↓
Gold
   ↓
Lakeflow Orchestration
   ↓
AI/BI Dashboard
```

> No long-lived AWS credentials should be committed to the repository.

---

## 26. Security principles demonstrated

- IAM role-based cloud access instead of embedded AWS credentials;
- least-privilege-oriented S3 access;
- separation of storage permissions from transformation logic;
- trust-policy-based role assumption;
- external ID-based cross-account trust configuration;
- Unity Catalog-governed storage credentials;
- Unity Catalog external locations;
- separation of landing and managed analytical storage;
- governed catalog / schema / table interfaces;
- ingestion lineage and auditability;
- quarantine instead of silent data loss;
- secret-free source code.

---

## 27. Demo

**End-to-end architecture and platform walkthrough** — YouTube: `https://youtu.be/u93M0RE6SAw?si=-M4UhF6xnoGzfmMv`

The narrated walkthrough demonstrates AWS S3 source architecture, AWS IAM and Unity Catalog integration, Bronze ingestion, Silver standardisation and data quality, quarantine handling, AUTO CDC / SCD Type 2, Gold analytics, Lakeflow orchestration, validation and reconciliation, and the Clinical Trial Intelligence & Risk Monitoring AI/BI dashboard.

---

## 28. Roadmap

After repository hardening, possible extensions include:

- automated pipeline-failure alerts;
- data-quality threshold notifications;
- development / staging / production environment separation;
- protocol-document processing;
- PDF extraction and document chunking;
- embeddings and retrieval-augmented generation for protocol-aware analysis;
- Databricks Genie natural-language querying over governed Gold datasets.

---

## 29. License

This project is intended to use the MIT License. The final `LICENSE` file will be included during repository hardening.
