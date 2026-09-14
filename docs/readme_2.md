# Clinical Trial Intelligence Platform

## 1. Project Overview

The Clinical Trial Intelligence Platform is an end-to-end data engineering
platform designed to ingest, govern, harmonize, validate, and analyze
multi-source clinical-trial data.

Clinical-trial operations typically generate data across several independent
operational systems, including:

- Electronic Data Capture (EDC)
- Clinical Trial Management Systems (CTMS)
- Central laboratory systems
- Safety / adverse-event systems
- Master and reference datasets
- Study protocol documents

These systems represent different aspects of the same clinical study but
frequently arrive independently, with different schemas, delivery frequencies,
terminologies, and data-quality characteristics.

The objective of this platform is therefore not simply to ingest files.
It is to create a governed lakehouse in which clinical, operational,
laboratory, and safety data can eventually be reconciled at study, site,
subject, and visit level.

The platform follows a Medallion Architecture:

S3 Landing
    ↓
Bronze — raw/incrementally ingested source data
    ↓
Silver — standardized, validated, harmonized data
    ↓
Gold — analytical and business-ready datasets
    ↓
Analytics / Dashboard / Clinical Intelligence

A separate document-intelligence path is planned for study protocol PDFs,
using retrieval-augmented generation for protocol-aware analysis.


## 2. High-Level Architecture

                        SOURCE SYSTEMS
                              │
             ┌────────────────┼────────────────┐
             │                │                │
            EDC              CTMS          Lab / Safety
             │                │                │
             └────────────────┼────────────────┘
                              ▼
                         Amazon S3
                       Landing Zone
                              │
                              ▼
                     Unity Catalog
                     External Location
                              │
                              ▼
                  Databricks Lakeflow
                       ETL Pipeline
                              │
                 ┌────────────┴────────────┐
                 │                         │
          Incremental ingestion       Batch/reference
            using Auto Loader           ingestion
                 │                         │
                 └────────────┬────────────┘
                              ▼
                           BRONZE
                              │
                     Unity Catalog
                      Managed Tables
                              │
                              ▼
                  AWS S3 Managed Storage
             UnityManaged/bronze/__unitystorage
                              │
                              ▼
                           SILVER
                 Standardization + DQ
                              │
                  ┌───────────┴───────────┐
                  ▼                       ▼
             Valid records            Quarantine
                  │
                  ▼
                         GOLD
                  Analytical models
                              │
                              ▼
               BI / KPI / Clinical Intelligence


## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Cloud | AWS | Cloud infrastructure |
| Object Storage | Amazon S3 | Landing and managed physical storage |
| Data Platform | Databricks | Lakehouse processing environment |
| Governance | Unity Catalog | Cataloging, permissions and governance |
| Ingestion | Databricks Auto Loader | Incremental cloud-file ingestion |
| Orchestration | Lakeflow ETL Pipelines | Declarative pipeline execution |
| Table Format | Delta Lake | Transactional lakehouse tables |
| Processing | Apache Spark / PySpark | Distributed transformations |
| Architecture | Medallion | Bronze → Silver → Gold |
| Future AI Layer | AWS Bedrock / RAG | Protocol document intelligence |


# 4. Storage Architecture

The platform deliberately separates the raw landing area from
Unity Catalog-managed analytical storage.

Amazon S3 bucket:

s3://clinical-trial-intelligence-platform-sk/


## 4.1 Landing Zone

Landing contains source-system files before lakehouse processing.

s3://clinical-trial-intelligence-platform-sk/Landing/

This location represents externally delivered/raw source data.

Databricks accesses the Landing location through a Unity Catalog external
location rather than embedding AWS credentials in transformation code.


## 4.2 Managed Storage

A separate location is used as the storage root for Unity Catalog-managed
datasets:

s3://clinical-trial-intelligence-platform-sk/UnityManaged/

For the Bronze schema, the configured managed storage root is:

s3://clinical-trial-intelligence-platform-sk/UnityManaged/bronze

Unity Catalog does not store managed tables directly as human-readable
directories such as:

UnityManaged/bronze/edc_subjects/

Instead, Unity Catalog owns the physical layout and generates internal
identifiers.

The observed physical structure is approximately:

UnityManaged/
└── bronze/
    └── __unitystorage/
        └── schemas/
            └── <schema-uuid>/
                └── tables/
                    ├── <table-uuid>/
                    ├── <table-uuid>/
                    ├── <table-uuid>/
                    └── ...

This separation is intentional:

Landing/
    = files controlled as source/ingestion data

UnityManaged/
    = data whose physical lifecycle is controlled by Unity Catalog


# 5. Unity Catalog Design

Catalog:

clinical_trial_intelligence

Current schema:

clinical_trial_intelligence.bronze

The catalog provides the governance namespace for the platform.

The Bronze schema represents source-aligned datasets after ingestion but
before business harmonization.

Three-level naming therefore follows:

<catalog>.<schema>.<table>

Example:

clinical_trial_intelligence.bronze.edc_subjects

The Bronze schema was explicitly configured with:

MANAGED LOCATION
's3://clinical-trial-intelligence-platform-sk/UnityManaged/bronze'

This makes the S3 location the storage root for managed objects created
inside the Bronze schema.


# 6. Why Managed Tables Were Used

Bronze datasets are implemented as Unity Catalog managed datasets rather
than exposing the S3 directories themselves as the analytical interface.

Applications and transformations therefore reference:

clinical_trial_intelligence.bronze.edc_subjects

rather than directly referencing a physical path such as:

s3://.../__unitystorage/.../<uuid>

This provides a separation between:

Logical identity:
clinical_trial_intelligence.bronze.edc_subjects

and

Physical implementation:
S3 / Unity Catalog-managed UUID directories

This allows Unity Catalog to control the storage lifecycle, metadata,
permissions, and governance of the tables.


# 7. External Locations

Two logical S3 access patterns are configured.

## Landing External Location

Purpose:
Allow Databricks to read incoming source-system files.

Conceptually:

clinical_trial_s3
    ↓
s3://clinical-trial-intelligence-platform-sk/Landing


## Managed Storage External Location

Purpose:
Authorize Unity Catalog to create managed storage.

Conceptually:

clinical_trial_managed
    ↓
s3://clinical-trial-intelligence-platform-sk/UnityManaged

The managed external location has the CREATE MANAGED STORAGE privilege
required for defining Unity Catalog managed storage beneath this path.


# 8. Bronze Layer

## Objective

Bronze is the source-aligned ingestion layer.

Its primary responsibilities are:

1. Preserve incoming source information.
2. Ingest cloud files reliably.
3. Maintain source-level traceability.
4. Avoid premature business transformations.
5. Establish governed Delta datasets.
6. Provide a reproducible input for Silver processing.

Bronze is intentionally not the layer where clinical terminology,
cross-system identifiers, laboratory units, or business KPIs are fully
harmonized.


# 9. Bronze Datasets

The Bronze pipeline currently produces ten datasets:

1. ctms_sites
2. ctms_studies
3. edc_subjects
4. edc_visits
5. lab_results
6. master_institutions
7. master_products
8. master_sponsors
9. protocol_study_arms
10. safety_adverse_events

These datasets represent multiple operational domains rather than one
monolithic clinical-trial table.


# 10. Source-System Domains

## EDC

Electronic Data Capture data represents subject-level clinical activity.

Important datasets include:

edc_subjects
edc_visits

These datasets establish the subject and visit grain required for later
clinical reconciliation.


## CTMS

Clinical Trial Management System data provides operational study/site
information.

Datasets include:

ctms_sites
ctms_studies


## Laboratory

lab_results

Contains laboratory observations associated with clinical-trial activity.

Laboratory harmonization is deliberately deferred to Silver because
different sources may represent measurements, units, dates, or test names
differently.


## Safety

safety_adverse_events

Contains adverse-event information.

Severity and safety terminology harmonization is performed downstream rather
than destructively modifying the source representation during ingestion.


## Master / Reference Data

master_institutions
master_products
master_sponsors
protocol_study_arms

These datasets provide controlled/reference information used later for
cross-system harmonization and analytical enrichment.


# 11. Incremental Ingestion Strategy

Operational feeds can arrive repeatedly over time.

For suitable continuously arriving file-based sources, Databricks Auto Loader
is used rather than repeatedly reading the complete Landing directory as a
new batch.

Conceptually:

Day 1
file_001
file_002

        ↓

Auto Loader

        ↓

Bronze

Later:

Day 2
file_003

        ↓

Auto Loader identifies new input

        ↓

Only new data is processed through the incremental ingestion path.

This design is important because production ingestion must distinguish:

"newly arrived data"

from

"data that has already been processed."


# 12. Why Auto Loader

A naive ingestion implementation could execute:

spark.read.csv(...)

against an entire source directory on every run.

That approach becomes problematic as the landing zone grows because the
pipeline repeatedly scans previously processed input.

Auto Loader provides stateful incremental file ingestion.

The design therefore improves:

- scalability
- restartability
- incremental processing
- operational reliability
- ingestion state management

Lakeflow manages the streaming state/checkpointing required by Auto Loader
when it is used inside the pipeline.


# 13. Lakeflow Pipeline

Bronze ingestion is orchestrated as a Databricks Lakeflow ETL pipeline.

Pipeline:

clinical-trial-bronze

Pipeline source code is maintained separately from the pipeline runtime
configuration.

The pipeline root represents the project source-code context, while the
configured source-code path points to the Bronze pipeline definitions.

Conceptually:

clinical-trial-intelligence-platform/
│
├── src/
│   ├── pipelines/
│   │   └── bronze/
│   │       ├── ...
│   │       └── ...
│   └── ...
│
├── data/
│
└── README.md


# 14. Declarative Pipeline Design

The pipeline source files define the datasets and transformations.

Lakeflow resolves dependencies between those datasets and executes the
resulting DAG.

This avoids manually orchestrating every table in a procedural sequence.

Conceptually:

Source declarations
       ↓
Dataset dependencies
       ↓
Lakeflow DAG
       ↓
Parallel/dependency-aware execution
       ↓
Unity Catalog datasets


# 15. Bronze Data Provenance

Source-level traceability is an explicit design requirement.

The ingestion design preserves provenance information such as:

- source file
- source system
- delivery date
- content/file hash

This becomes important when downstream discrepancies are identified.

For example, rather than only knowing:

"this subject record is wrong"

the system should eventually allow investigation of:

record
   ↓
Bronze dataset
   ↓
source system
   ↓
delivery
   ↓
source file

This supports reconciliation, auditability, troubleshooting, and controlled
reprocessing.


# 16. Data-Grain Philosophy

The platform does NOT immediately flatten every system into one giant table.

Different datasets naturally exist at different grains.

Examples:

Study
  ↓
Site
  ↓
Subject
  ↓
Visit
  ↓
Lab Result

Safety events may independently exist at:

Subject
  ↓
Adverse Event

Maintaining the correct grain is critical because an incorrect join can
multiply records and corrupt analytical metrics.

Cross-system joins therefore belong primarily in Silver/Gold after keys,
cardinality, terminology, and data quality have been validated.


# 17. Bronze Validation

After the Bronze pipeline completed successfully, SQL validation was
performed directly against Unity Catalog.

Validated examples:

SELECT COUNT(*)
FROM clinical_trial_intelligence.bronze.edc_subjects;

Result:

3,829


SELECT COUNT(*)
FROM clinical_trial_intelligence.bronze.edc_visits;

Result:

18,262


SELECT COUNT(*)
FROM clinical_trial_intelligence.bronze.ctms_sites;

Result:

45

These counts were also consistent with the corresponding pipeline output
shown by the successful Bronze pipeline execution.


# 18. Physical Storage Validation

Logical table creation alone was not considered sufficient validation.

The backing S3 storage was also inspected.

The Bronze schema reports its managed root as:

s3://clinical-trial-intelligence-platform-sk/UnityManaged/bronze

AWS S3 inspection confirmed that Unity Catalog created:

UnityManaged/
└── bronze/
    └── __unitystorage/
        └── schemas/
            └── <schema-uuid>/
                └── tables/
                    └── <table-uuid>/

This confirms both sides of the architecture:

CONTROL / GOVERNANCE PLANE
        ↓
Unity Catalog
clinical_trial_intelligence.bronze.*

and

PHYSICAL DATA PLANE
        ↓
Amazon S3
UnityManaged/bronze/__unitystorage/...


# 19. Bronze Completion Criteria

The Bronze milestone is considered complete only when all of the following
conditions are satisfied:

[✓] Source data exists in S3 Landing
[✓] S3 access is governed through Unity Catalog
[✓] Bronze schema exists
[✓] Bronze schema has the intended managed storage root
[✓] Bronze pipeline executes successfully
[✓] Ten Bronze datasets are produced
[✓] Incremental ingestion is implemented for appropriate operational feeds
[✓] Tables can be queried through Unity Catalog
[✓] Row-count validation succeeds
[✓] Managed Delta storage is physically present in the intended S3 hierarchy


# 20. Important Design Decision: Landing vs Bronze

A key architectural distinction is:

Landing != Bronze

Landing contains source files.

Bronze contains governed lakehouse datasets created from those source files.

Therefore:

Vendor/source
      ↓
S3 Landing
      ↓
Ingestion
      ↓
Bronze Delta dataset

This separation allows raw source delivery and lakehouse table management
to evolve independently.


# 21. Important Design Decision: Logical vs Physical Access

Consumers should NOT query:

s3://clinical-trial-intelligence-platform-sk/UnityManaged/bronze/
__unitystorage/.../<uuid>

They should query:

clinical_trial_intelligence.bronze.edc_subjects

The UUID-based storage hierarchy is an implementation detail controlled by
Unity Catalog.

The table name is the governed interface.


# 22. Failure Recovery / Idempotency

Incremental ingestion must tolerate retries and pipeline restarts.

Auto Loader maintains ingestion state so previously processed files are not
blindly treated as new input on every execution.

This provides the foundation for an idempotent ingestion architecture:

same previously processed input
        +
pipeline restart
        ↓
should not create uncontrolled duplicate ingestion

Business-level duplicate handling is still a separate concern.

For example, two different source files can legitimately contain the same
business key.

Therefore:

File ingestion idempotency
        !=
Business-record deduplication

Business-key deduplication and reconciliation are handled downstream.


# 23. Why Data Quality Is Not Fully Applied in Bronze

Bronze should preserve source truth.

Aggressively correcting or dropping records during ingestion would make it
difficult to determine whether an anomaly originated in:

- the source system
- ingestion
- transformation
- business logic

Therefore Bronze performs ingestion-oriented controls while semantic
standardization and business-quality enforcement belong in Silver.

The design principle is:

Bronze:
"what did the source send?"

Silver:
"what is the standardized and trustworthy representation?"

Gold:
"what information does the business need?"


# 24. Planned Silver Layer

The next implementation milestone is Silver.

Silver will consume Bronze tables and perform:

- schema/type standardization
- date normalization
- null handling
- business-key validation
- duplicate detection
- terminology harmonization
- laboratory-unit harmonization
- severity normalization
- referential-integrity checks
- cross-system key reconciliation
- data-quality rule evaluation
- quarantine routing

Bad records will not simply disappear.

The intended pattern is:

                 Bronze
                    │
                    ▼
             Silver rules
                    │
           ┌────────┴────────┐
           ▼                 ▼
       VALID DATA       INVALID DATA
           │                 │
           ▼                 ▼
        Silver          Quarantine
                             │
                             ▼
                     violated_rule
                     source context


# 25. Planned Gold Layer

Gold will expose business-oriented analytical models rather than raw
source-system structures.

Planned examples include:

study_performance
site_performance
safety_signal_monthly

Gold datasets will support cross-system clinical-trial intelligence,
including study progress, site performance, safety monitoring, and
operational reconciliation.


# 26. Planned Clinical Intelligence Layer

The longer-term platform objective is to combine structured operational
data with protocol knowledge.

Structured path:

EDC + CTMS + Lab + Safety
          ↓
Bronze
          ↓
Silver
          ↓
Gold

Unstructured path:

Protocol PDFs
       ↓
Document processing / embeddings
       ↓
AWS Bedrock / RAG
       ↓
Protocol-aware intelligence

Together these layers are intended to support governed clinical-trial
analysis rather than isolated source-system reporting.


# 27. Engineering Principles Demonstrated

This project intentionally demonstrates:

- incremental ingestion
- cloud object storage
- Delta Lake
- lakehouse architecture
- declarative data pipelines
- data provenance
- idempotency
- medallion architecture
- Unity Catalog governance
- managed vs external storage
- schema-level storage isolation
- data quality
- quarantine design
- multi-source reconciliation
- dimensional/analytical modelling
- clinical-domain data engineering
- RAG integration for unstructured documents


# 28. Current Project Status

COMPLETED:

[✓] Synthetic multi-source clinical-trial dataset
[✓] AWS S3 Landing architecture
[✓] Unity Catalog connectivity
[✓] Landing external location
[✓] Managed-storage external location
[✓] clinical_trial_intelligence catalog
[✓] Bronze schema
[✓] Bronze schema-level S3 managed storage
[✓] Bronze Lakeflow pipeline
[✓] Ten Bronze datasets
[✓] Auto Loader-based incremental ingestion
[✓] Bronze row-count validation
[✓] Physical S3 managed-storage validation

CURRENT NEXT PHASE:

[ ] Silver schema
[ ] Standardization
[ ] Cross-system harmonization
[ ] Data-quality framework
[ ] Quarantine datasets
[ ] Referential-integrity validation

FUTURE:

[ ] Gold analytical models
[ ] KPI layer
[ ] Dashboard
[ ] Protocol RAG
[ ] Clinical intelligence / copilot layer