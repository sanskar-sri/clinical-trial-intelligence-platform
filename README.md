Clinical Trial Intelligence Platform

An end-to-end clinical trial data engineering platform built using AWS S3, Databricks, Apache Spark / PySpark, Delta Lake, Unity Catalog, Lakeflow Declarative Pipelines, and Databricks AI/BI.

The platform ingests synthetic operational data from multiple clinical-trial source systems, processes it through a governed Bronze → Silver → Gold Medallion Architecture, applies reference standardisation and clinical data-quality controls, preserves historical subject changes using AUTO CDC / SCD Type 2, quarantines records that cannot be trusted, and publishes analytical datasets for study, site, subject, visit, safety, laboratory, and data-quality monitoring.

Data Disclaimer: All data in this repository is synthetically generated. No real patient, clinical-trial subject, site, sponsor, institution, or organisation data is used.

⸻

1. Why This Project Exists

Clinical trial operations generate data across multiple independent systems:

* EDC captures subject and visit activity.
* CTMS contains study and site operational data.
* Laboratory systems provide clinical measurements.
* Safety systems capture adverse events.
* Master and reference datasets provide sponsors, products, institutions, geography, mappings, and controlled terminology.

These sources arrive with different delivery patterns, schemas, terminology, and data-quality characteristics.

The engineering challenge is therefore not simply moving CSV files into tables. The objective is to build a governed platform where:

* incoming source data remains traceable to its original delivery;
* AWS access is controlled without embedding credentials in pipeline code;
* business keys are standardised consistently;
* reference data is resolved deterministically;
* invalid records remain visible through quarantine rather than being silently dropped;
* historical subject changes can be reconstructed;
* downstream analytical datasets use trusted data;
* and analytical outputs can be reconciled through Silver and Bronze to their source.

⸻

2. End-to-End Architecture

EDC       CTMS       Laboratory       Safety       Master / Reference
 │          │             │              │                 │
 └──────────┴─────────────┴──────────────┴─────────────────┘
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
                 ┌────────────┴────────────┐
                 │                         │
          Storage Credential        External Location
                 │                         │
                 └────────────┬────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │            BRONZE           │
               │                             │
               │ Auto Loader +               │
               │ Materialized Views          │
               │                             │
               │ Source preservation         │
               │ Ingestion lineage           │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │            SILVER           │
               │                             │
               │ Type standardisation        │
               │ Key normalisation           │
               │ Reference resolution        │
               │ Referential integrity       │
               │ Clinical DQ rules           │
               │ AUTO CDC / SCD Type 2       │
               └─────────┬───────────┬───────┘
                         │           │
                       VALID       INVALID
                         │           │
                         ▼           ▼
                      SILVER     QUARANTINE
                         │
                         ▼
               ┌─────────────────────────────┐
               │             GOLD            │
               │                             │
               │ Business-ready analytical   │
               │ datasets                    │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │       Databricks AI/BI      │
               │                             │
               │ Clinical Trial Intelligence │
               │ & Risk Monitoring           │
               └─────────────────────────────┘

Cross-layer execution:

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

Transformation logic and workflow orchestration are deliberately separated.

Individual Lakeflow Declarative Pipelines define transformations and dataset dependencies within each Medallion layer, while a Lakeflow Job coordinates execution across Bronze, Silver, and Gold.

⸻

3. Technology Stack

Area	Technology
Cloud	AWS
Object Storage	Amazon S3
Cloud Access	AWS IAM Role
Governed Storage Access	Unity Catalog Storage Credential
Storage Abstraction	Unity Catalog External Location
Data Platform	Databricks
Processing	Apache Spark / PySpark / SQL
Ingestion	Databricks Auto Loader (cloudFiles)
Pipeline Framework	Lakeflow Declarative Pipelines
Table Format	Delta Lake
Governance	Unity Catalog
Historical Processing	AUTO CDC / SCD Type 2
Orchestration	Lakeflow Jobs
Analytics	Databricks AI/BI
Version Control	Git / GitHub
Deployment as Code	Declarative Automation Bundles — in progress
CI/CD	GitHub Actions — in progress

⸻

4. AWS → Databricks Security and Storage Configuration

A major design requirement was to allow Databricks to access Amazon S3 without embedding AWS access keys or secret access keys in notebooks or pipeline code.

The integration uses:

AWS S3
   ▲
   │
AWS IAM Role
   ▲
   │ AssumeRole
   │ + External ID
   │
Unity Catalog Storage Credential
   ▲
   │
Unity Catalog External Location
   ▲
   │
Databricks Workloads

4.1 S3 Storage Layout

The project separates externally landed source data from Unity Catalog-managed analytical storage.

s3://<project-bucket>/
│
├── Landing/
│   ├── CTMS/
│   ├── EDC/
│   ├── Lab/
│   ├── Safety/
│   ├── master/
│   ├── protocol/
│   └── reference/
│
└── UnityManaged/
    ├── bronze/
    ├── silver/
    ├── quarantine/
    └── gold/

Landing/ contains externally delivered source files.

UnityManaged/ provides storage for governed analytical data managed through Unity Catalog.

⸻

4.2 AWS IAM Role

An AWS IAM role provides Databricks with controlled access to the required S3 resources.

Conceptually:

AWS IAM Role
     │
     ├── Permissions Policy
     │       │
     │       └── Required S3 permissions
     │
     └── Trust Policy
             │
             ├── Databricks-authorised principal
             ├── Self-assumption configuration
             └── External ID condition

The IAM permissions define which S3 resources the role can access.

The trust relationship controls who is allowed to assume the role.

No long-lived AWS access key or secret access key is stored in transformation code.

⸻

4.3 Trust Relationship and External ID

The IAM role contains a trust relationship that allows the appropriate Databricks identity to assume the role.

The Unity Catalog storage credential generates an External ID, which is included in the IAM role trust relationship.

Conceptually:

Databricks / Unity Catalog
          │
          │ sts:AssumeRole
          │
          │ External ID
          ▼
      AWS IAM Role
          │
          ▼
    Authorised S3 Paths

The external ID strengthens the cross-account trust relationship by associating role assumption with the intended storage credential configuration.

Actual AWS account IDs, external IDs, credentials, and other security-sensitive identifiers are not published in this repository.

⸻

4.4 Unity Catalog Storage Credential

The IAM role is represented inside Unity Catalog through a Storage Credential.

AWS IAM Role
      │
      ▼
Unity Catalog
Storage Credential

The Storage Credential defines the cloud identity Unity Catalog uses when accessing authorised S3 locations.

This keeps AWS identity configuration separate from application and pipeline code.

⸻

4.5 Unity Catalog External Location

The S3 storage path is registered with Unity Catalog through an External Location.

Conceptually:

External Location
       │
       ├── S3 Path
       │
       └── Storage Credential
                │
                ▼
             IAM Role

This separates:

* the physical cloud-storage path;
* the cloud identity authorised to access it;
* and the Unity Catalog privileges controlling which workloads/users can use that location.

⸻

4.6 End-to-End Access Flow

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

This creates a governed access boundary between Databricks workloads and AWS storage.

⸻

5. Unity Catalog Governance

The platform uses the following catalog:

clinical_trial_intelligence

with four logical schemas:

clinical_trial_intelligence
│
├── bronze
├── silver
├── quarantine
└── gold

Consumers interact with governed objects such as:

clinical_trial_intelligence.bronze.edc_subjects

rather than depending on physical storage paths.

Unity Catalog therefore provides the governed interface between cloud storage, transformation pipelines, and downstream consumers.

⸻

6. Source Landscape

The S3 landing-zone exploration identified 7 source families:

Landing/
│
├── CTMS/
├── EDC/
├── Lab/
├── Safety/
├── master/
├── protocol/
└── reference/

At the latest completed Bronze landscape exploration:

* 7 source families were accessible;
* 18 dataset-level source objects were identified;
* 55 physical source files were present.

The number of physical landing objects should not be confused with the number of production Bronze datasets because supporting landing artifacts can exist without becoming independent Bronze tables.

Source Systems

Source	Data
EDC	Subjects, visits
CTMS	Studies, sites
Laboratory	Laboratory results
Safety	Adverse events
Master	Institutions, products, sponsors
Protocol	Study arms
Reference	Geography and clinical/reference mappings

Recurring clinical feeds use date-versioned filenames.

The exploration also showed that S3 object modification timestamps should not automatically be treated as logical source-delivery order. Dates embedded in recurring source filenames provide an important logical delivery indicator.

⸻

7. Bronze Layer

Bronze answers:

What did the source system send?

Bronze preserves source representation and ingestion lineage rather than performing business harmonisation during ingestion.

Incremental Auto Loader Feeds

Four recurring feeds are processed incrementally:

edc_subjects
edc_visits
lab_results
safety_adverse_events

Materialized Supporting Sources

CTMS, master, protocol, and reference datasets are processed as relatively small supporting datasets.

These include:

* studies;
* sites;
* institutions;
* products;
* sponsors;
* study arms;
* geography mappings;
* diagnosis mappings;
* laboratory-test references;
* severity mappings;
* sex mappings;
* unit mappings;
* country/region mappings.

Bronze Lineage

Every Bronze record retains ingestion metadata including:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date

This enables downstream records to be traced back to the physical source delivery that produced them.

Bronze deliberately avoids business correction so that source anomalies remain distinguishable from transformation behaviour.

⸻

8. Silver Layer

Silver answers:

What is the standardised and trustworthy representation of the source data?

Silver performs:

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

⸻

9. Business-Key Normalisation

Primary and foreign business keys use a shared transformation contract implemented in the Silver utility layer.

TRIM
  ↓
blank / "-" → NULL
  ↓
UPPER

Examples:

" sub-001 "  →  "SUB-001"
""           →  NULL
"-"          →  NULL

Centralising this behaviour ensures that the same business key receives identical normalisation semantics across subjects, visits, laboratory results, adverse events, dimensions, and reference joins.

⸻

10. Reference Standardisation

Reference datasets can contain semantically equivalent values with different physical representations.

For example:

M
m
Male
MALE

Joining clinical data against an unresolved reference dataset after normalisation can create one-to-many matches.

The Silver design therefore follows:

Raw Reference
      ↓
Key Normalisation
      ↓
Deterministic Resolution
      ↓
Unique Reference Representation
      ↓
Clinical Fact Join

Unresolved values remain visible through controlled representations such as UNMAPPED, where appropriate, rather than silently disappearing from downstream processing.

⸻

11. Subject Historical Processing — AUTO CDC / SCD Type 2

The subject feed behaves differently from the other clinical feeds.

Observed source deliveries consist of an initial subject population followed by smaller incremental change files rather than independent full snapshots.

Subject processing therefore uses AUTO CDC with SCD Type 2.

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

Business Key

subject_id

Deterministic Source Ordering

Source changes are sequenced using logical source-delivery information rather than relying solely on ingestion time.

The sequence uses:

struct(
    source_snapshot_date,
    _source_file_name
)

where source_snapshot_date is derived from the source filename.

This is important because multiple files can be processed during the same pipeline execution, making ingestion timestamp alone unsuitable for deterministic source ordering.

History Semantics

Technical ingestion metadata should not create false clinical history.

A subject should not accumulate:

ENROLLED
   ↓
ENROLLED
   ↓
ENROLLED

simply because the same business state appeared in several source deliveries.

History therefore represents meaningful source/business-state change rather than ingestion-file noise.

⸻

12. Visits, Laboratory Results and Adverse Events

Each clinical entity was evaluated according to its observed source behaviour instead of automatically copying the subject CDC strategy.

Visits

Grain:
1 row = 1 subject visit

Observed visit_id behaviour supports append-oriented processing.

Silver validation includes subject relationships, study/site consistency, status-aware date validation, and relevant clinical rules.

Laboratory Results

Grain:
1 row = 1 laboratory measurement

Measurements can arrive in different units and are standardised through laboratory-test and unit reference data.

Conceptually:

standardized_result_value
        =
result_value × conversion_factor

Both source and standardised representations are retained.

This allows derived abnormality to be compared with the source-provided abnormality indicator rather than silently replacing source information.

Adverse Events

Grain:
1 row = 1 adverse event

Severity and seriousness are modelled separately.

Severity represents event intensity, while seriousness is a separate clinical/regulatory characteristic.

The platform therefore does not infer regulatory seriousness solely from severity.

⸻

13. Referential Integrity

Clinical fact records are validated against trusted Silver entities rather than raw Bronze data.

Bronze Subject
      │
      ▼
Silver Validation
      │
      ├── Valid ───────► Silver Subject
      │
      └── Invalid ─────► Quarantine
Visit / Lab / AE
      │
      ▼
Validate relationships against
trusted Silver entities

This prevents an invalid raw subject record from legitimising downstream clinical facts.

For historical subjects, downstream relationships use the appropriate trusted/current Silver representation where required.

⸻

14. Data Quality and Quarantine

The platform distinguishes between records that cannot safely participate in trusted analytics and records that are unusual but still clinically plausible.

DQ Failure
    │
    ▼
QUARANTINE
Clinical Warning
    │
    ▼
SILVER + FLAG

Blocking rules can include:

* missing critical business keys;
* unknown study/site/subject relationships;
* referential-integrity failures;
* cross-entity inconsistencies;
* invalid ranges;
* unmappable required reference values;
* impossible date sequences.

Warnings represent unusual but potentially legitimate conditions that should remain available for review.

Quarantine Design

Quarantined records preserve:

Original business attributes
          +
Source lineage
          +
All triggered DQ rules
          +
Human-readable failure information
          +
Quarantine timestamp

The _dq_failures representation preserves all applicable failed rules rather than only the first failure encountered.

This makes quarantine an auditable data product rather than a discarded-record bucket.

⸻

15. Gold Analytical Layer

Gold answers:

What does the business need from trusted clinical data?

The Gold layer contains business-ready analytical datasets supporting:

* subject analytical spine;
* subject-level summary;
* subject disposition;
* discontinuation analysis;
* enrolment trends;
* site performance;
* visit compliance;
* safety monitoring;
* laboratory monitoring;
* data-quality monitoring;
* Bronze → Silver → quarantine reconciliation.

Gold datasets are built from trusted Silver representations rather than directly from raw Bronze data.

⸻

16. Clinical Modelling Boundaries

The platform deliberately avoids deriving clinical variables that cannot be supported by the available source data.

TEAE

A defensible treatment-emergent adverse-event flag requires an appropriate treatment/exposure start timestamp.

Randomisation alone should not automatically be interpreted as first treatment exposure.

Safety Population

A safety-population flag should not automatically be interpreted as “received treatment” without appropriate exposure information.

Exposure-Adjusted Event Rates

Exposure-adjusted safety rates require a defensible exposure or follow-up denominator.

Where CDISC concepts influence the analytical model, they are treated as design inspiration rather than being presented as evidence of regulatory compliance.

The modelling principle is:

Do not manufacture precision that the source data cannot support.

⸻

17. Dashboard

The Gold layer powers the Clinical Trial Intelligence & Risk Monitoring Databricks AI/BI dashboard.

The dashboard contains five analytical areas:

1. Study Overview
2. Enrolment & Site Performance
3. Patient & Visit Monitoring
4. Safety & Lab Monitoring
5. Data Quality & Operational Risk

The dashboard demonstrates how governed engineering outputs support operational clinical-trial monitoring rather than functioning as an isolated visualisation layer.

⸻

18. Exploration and Design Proof

Important pipeline decisions are supported by source exploration rather than assumptions about source behaviour.

The exploration architecture deliberately separates three questions:

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

The target exploration structure is:

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

These notebooks make engineering decisions reviewable instead of leaving their justification implicit inside production code.

⸻

19. Validation Strategy

Validation is separated from transformation logic.

It covers:

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

Final numerical results are published only after being reproduced by the corresponding exploration or validation workflow.

This prevents stale pipeline-run metrics from being presented as the current platform state.

⸻

20. Orchestration

The Medallion layers execute as independent Lakeflow Declarative Pipelines.

clinical-trial-bronze
        │
        │ all-succeeded
        ▼
clinical-trial-silver
        │
        │ all-succeeded
        ▼
clinical-trial-gold

A Lakeflow Job manages the cross-pipeline dependency.

This separation is deliberate:

Pipeline
   │
   └── Dataset transformation/dependency graph
Job
   │
   └── Operational task/workflow dependency graph

Transformation logic therefore remains independent from workflow orchestration.

⸻

21. Repository Structure

The repository is being consolidated toward the following production-oriented structure:

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

The repository separates three engineering concerns:

Area	Question
notebooks/exploration/	What does the source data demonstrate?
pipelines/	What should the production platform do to it?
notebooks/validation/	Did the implementation produce the expected result?

⸻

22. Current Project Status

Core Platform — Implemented

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

Implemented components include:

* synthetic multi-source clinical-trial data;
* AWS S3 landing architecture;
* AWS IAM role-based S3 access;
* IAM trust relationship and external-ID configuration;
* Unity Catalog Storage Credential;
* Unity Catalog External Location;
* governed Bronze/Silver/Quarantine/Gold schemas;
* incremental Auto Loader ingestion;
* materialized supporting datasets;
* ingestion lineage;
* shared business-key normalisation;
* deterministic reference resolution;
* referential-integrity validation;
* clinical data-quality rules;
* quarantine processing;
* subject AUTO CDC / SCD Type 2;
* entity-specific visit/lab/AE processing;
* Gold analytical datasets;
* Bronze → Silver → Gold orchestration;
* Databricks AI/BI dashboard;
* validation and reconciliation logic.

Repository Hardening — In Progress

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

⸻

23. Remaining Engineering Work

Exploration

Complete the structured Bronze, Silver, and Gold exploration notebooks and ensure important implementation decisions are backed by reproducible evidence.

Final Validation

Reproduce final:

* Bronze volumes;
* Silver valid/quarantine counts;
* SCD current/history checks;
* reference conflict checks;
* Gold reconciliation;
* source-to-target balance.

Repository Hygiene

* remove temporary development artifacts;
* standardise directory naming;
* remove system-generated files;
* add .gitignore;
* freeze package paths;
* ensure documentation matches the actual filesystem.

Automated Testing

Add automated tests around reusable transformation contracts, particularly:

" subj-001 " → "SUBJ-001"
"-"          → NULL
""           → NULL

Deployment as Code

Represent Databricks resources using Declarative Automation Bundles:

databricks.yml
       │
       └── resources/
              ├── bronze.pipeline.yml
              ├── silver.pipeline.yml
              ├── gold.pipeline.yml
              ├── orchestration.job.yml
              └── dashboard.yml

CI/CD

Add GitHub Actions:

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

Final Reproducibility

Validate the project from a clean repository checkout so that documented setup and deployment instructions match the actual implementation.

⸻

24. Scope and Limitations

* All source data is synthetic.
* No real patient or clinical-trial participant data is used.
* No AWS access keys or secret keys are stored in transformation code.
* Security-sensitive AWS account identifiers, external IDs, credentials, and trust-policy values are not published.
* The subject source feed does not currently provide explicit delete semantics.
* Visits, laboratory results, and adverse events are processed according to their observed source behaviour; this assumption should be revalidated if the source contract changes.
* Clinical variables requiring genuine treatment/exposure information are not fabricated from weaker proxy fields.
* Final numerical results will be refreshed after completion of the current exploration and validation pass.
* Automated CI/CD and deployment-as-code are part of the current repository-hardening phase.

⸻

25. Reproducing the Platform

A reproduction requires:

1. Create an AWS S3 bucket for landing and governed storage.
2. Upload the synthetic landing datasets.
3. Create an AWS IAM role with the required S3 permissions.
4. Configure the IAM trust relationship for Databricks role assumption.
5. Create the Unity Catalog Storage Credential using the IAM Role ARN.
6. Obtain the Storage Credential External ID.
7. Update the IAM trust policy with the generated External ID and required self-assumption configuration.
8. Validate the Storage Credential.
9. Create the Unity Catalog External Location using the Storage Credential and S3 path.
10. Configure the catalog and Bronze, Silver, Quarantine, and Gold schemas.
11. Configure the Bronze Lakeflow Declarative Pipeline.
12. Configure the Silver Lakeflow Declarative Pipeline.
13. Configure the Gold Lakeflow Declarative Pipeline.
14. Create the Lakeflow Job that sequences Bronze → Silver → Gold.
15. Import/configure the Databricks AI/BI dashboard.
16. Run the validation and reconciliation workflows.

Conceptually:

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

No long-lived AWS credentials should be committed to the repository.

⸻

26. Security Principles Demonstrated

The project demonstrates:

* IAM role-based cloud access instead of embedded AWS credentials;
* least-privilege-oriented S3 access;
* separation of storage permissions from transformation logic;
* trust-policy-based role assumption;
* External ID-based cross-account trust configuration;
* Unity Catalog-governed Storage Credentials;
* Unity Catalog External Locations;
* separation of landing and managed analytical storage;
* governed catalog/schema/table interfaces;
* ingestion lineage and auditability;
* quarantine instead of silent data loss;
* and secret-free source code.

⸻

27. Demo

End-to-End Architecture & Platform Walkthrough

YouTube: [ADD YOUTUBE DEMO LINK]

The narrated walkthrough demonstrates:

* AWS S3 source architecture;
* AWS IAM and Unity Catalog integration;
* Bronze ingestion;
* Silver standardisation and data quality;
* quarantine handling;
* AUTO CDC / SCD Type 2;
* Gold analytics;
* Lakeflow orchestration;
* validation and reconciliation;
* and the Clinical Trial Intelligence & Risk Monitoring AI/BI dashboard.

⸻

28. Roadmap

After repository hardening, possible extensions include:

* automated pipeline-failure alerts;
* data-quality threshold notifications;
* development/staging/production environment separation;
* protocol-document processing;
* PDF extraction and document chunking;
* embeddings and retrieval-augmented generation for protocol-aware analysis;
* Databricks Genie natural-language querying over governed Gold datasets.

⸻

29. License

This project is intended to use the MIT License.

The final LICENSE file will be included during repository hardening.
