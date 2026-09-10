Yes. At this stage your Bronze + Silver architecture and Silver pipeline implementation are essentially complete, with Silver validation being the next step.

Below is a README you can paste into your repository’s README.md.

# Clinical Trial Intelligence Platform
An end-to-end clinical trial data engineering and analytics platform built using AWS S3, Databricks, Unity Catalog, and Lakeflow Declarative Pipelines.
The project implements a medallion architecture for ingesting, standardizing, validating, governing, and preparing clinical operational data for downstream analytics.
> Current project status: Bronze and Silver implemented. Silver validation and Gold analytical modeling are the next stages.
---
# 1. Project Objective
Clinical trial operational data is typically distributed across multiple systems such as:
- Clinical Trial Management Systems (CTMS)
- Electronic Data Capture (EDC)
- Laboratory systems
- Safety systems
- Master/reference datasets
These systems produce data with different schemas, naming conventions, update patterns, and data-quality characteristics.
The objective of this project is to build a governed data platform that:
1. ingests heterogeneous clinical trial source data,
2. preserves raw source records,
3. standardizes clinical entities,
4. validates business and referential-integrity rules,
5. quarantines invalid records,
6. maintains subject history,
7. standardizes laboratory measurements,
8. prepares trusted datasets for clinical-trial analytics.
---
# 2. Architecture
```text
Source Systems
     │
     ├── CTMS
     ├── EDC
     ├── Laboratory
     ├── Safety
     └── Master / Reference Data
     │
     ▼
AWS S3
Landing/
     │
     ▼
Databricks
Lakeflow Declarative Pipelines
     │
     ▼
┌─────────────────────────────────────────┐
│                BRONZE                   │
│                                         │
│ Raw source representation               │
│ Source lineage                          │
│ Ingestion metadata                      │
│ Schema rescue                           │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│                SILVER                   │
│                                         │
│ Type conversion                         │
│ Key normalization                       │
│ Reference standardization               │
│ Referential integrity                   │
│ Clinical DQ validation                  │
│ CDC / SCD Type 2                        │
└───────────────┬─────────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
   Valid Records     Invalid Records
        │                │
        ▼                ▼
     SILVER          QUARANTINE
        │
        ▼
       GOLD
        │
        ▼
Clinical Trial Analytics / KPIs

Gold modeling is the next development stage.

⸻

3. Technology Stack

Layer	Technology
Cloud	AWS
Object Storage	Amazon S3
Data Platform	Databricks
Governance	Unity Catalog
Pipeline Framework	Lakeflow Declarative Pipelines
Processing	Apache Spark / PySpark
Storage Format	Delta Lake
Transformation	Python + SQL
CDC	AUTO CDC
Data Quality	Rule-based validation + quarantine
Version Control	Git
Source Storage	S3 Landing Zone
Managed Storage	Unity Catalog managed storage

⸻

4. Repository Structure

clinical-trial-intelligence-platform/
│
├── data/
│
├── src/
│   │
│   ├── notebook/
│   │   └── exploration/
│   │
│   ├── pipelines/
│   │   ├── bronze/
│   │   └── silver/
│   │       ├── dimensions.sql
│   │       ├── subjects.py
│   │       ├── visits.py
│   │       ├── lab_results.py
│   │       └── adverse_events.py
│   │
│   ├── setup/
│   │   └── create_schemas.sql
│   │
│   ├── Utility/
│   │   ├── bronze_common.py
│   │   ├── bronze_materialized_common.py
│   │   └── silver_common.py
│   │
│   └── Validation/
│       ├── bronze/
│       │   └── bronze_validation.sql
│       │
│       └── silver/
│           ├── silver_validation.sql
│           ├── subjects_validation
│           └── visits_validation
│
└── README.md

⸻

5. AWS S3 Storage Architecture

The platform uses the S3 bucket:

clinical-trial-intelligence-platform-sk

Logical storage organization:

clinical-trial-intelligence-platform-sk/
│
├── Landing/
│   │
│   ├── source datasets
│   └── reference datasets
│
└── UnityManaged/
    │
    ├── bronze/
    ├── silver/
    ├── quarantine/
    └── gold/

Landing/ represents externally supplied source data.

UnityManaged/ is governed through Unity Catalog and stores managed analytical datasets.

Unity Catalog controls the actual internal storage structure for managed tables.

⸻

6. Unity Catalog Organization

The project uses:

Catalog:
clinical_trial_intelligence

with the following schemas:

clinical_trial_intelligence
│
├── bronze
├── silver
├── quarantine
└── gold

Responsibilities:

Bronze

Raw source-system representation.

Silver

Cleaned, standardized, validated, and conformed clinical data.

Quarantine

Records rejected by blocking Silver data-quality rules.

Gold

Business-ready analytical datasets and clinical-trial KPIs.

Gold implementation is pending.

⸻

7. Bronze Layer

The Bronze layer preserves source-system data while adding ingestion and lineage metadata.

Current Bronze datasets include:

ctms_sites
ctms_studies
edc_subjects
edc_visits
lab_results
safety_adverse_events
master_institutions
master_products
master_sponsors
protocol_study_arms
ref_country_region
ref_diagnosis_mapping
ref_geography
ref_lab_test
ref_severity_mapping
ref_sex_mapping
ref_unit_mapping

Bronze records also preserve operational metadata such as:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date
_rescued_data

where applicable.

This provides traceability from a Silver record back to its original source file.

⸻

8. Silver Design Principles

Silver performs more than basic cleaning.

It implements:

Raw Bronze record
       │
       ▼
Type conversion
       │
       ▼
Business-key normalization
       │
       ▼
Reference standardization
       │
       ▼
Entity resolution
       │
       ▼
Referential-integrity validation
       │
       ▼
Clinical business-rule validation
       │
       ├──────── Valid ────────► Silver
       │
       └──────── Invalid ──────► Quarantine

⸻

9. Canonical Business-Key Normalization

A shared Silver normalization contract is used for business identifiers.

The transformation is:

TRIM
  ↓
blank / "-" → NULL
  ↓
UPPER

Example:

" sub-001 "  →  "SUB-001"
""           →  NULL
"-"          →  NULL
NULL         →  NULL

The same normalization is applied to primary and foreign business keys so joins use consistent semantics.

Reusable transformations are maintained in:

src/Utility/silver_common.py

including:

blank_to_null()
normalize_key()

⸻

10. Deterministic Reference Resolution

Reference datasets can contain duplicate normalized lookup keys.

Using an unordered aggregation such as FIRST() after a distributed GROUP BY can produce non-deterministic results.

The Silver implementation therefore uses deterministic aggregation where the lookup key is expected to functionally determine its attributes.

Reference-conflict validation was performed for:

sex mapping
diagnosis mapping
site → study mapping
lab test mapping
unit conversion mapping
severity mapping

All tested conflict queries returned:

0 conflicting mappings

This validates the current use of deterministic aggregation for these reference datasets.

⸻

11. Silver Dimensions and References

dimensions.sql produces standardized reference and dimensional entities used by downstream Silver transformations.

These include entities such as:

ref_sex
ref_diagnosis
ref_lab_test
ref_unit
ref_severity
dim_study
dim_site
dim_study_arm
dim_institution
dim_product
dim_sponsor

These datasets provide trusted lookup structures for the clinical event pipelines.

⸻

12. Subject Pipeline

Source:

bronze.edc_subjects

Target:

silver.subjects

Rejected records:

quarantine.subjects

Source Behavior

Exploration established that the EDC subject feed consists of:

Initial population file
        +
Incremental subject change files

The source is therefore not treated as a sequence of complete snapshots.

⸻

13. Subject CDC Strategy

Subjects are maintained using:

AUTO CDC
+
SCD Type 2

Business key:

subject_id

Ordering expression:

STRUCT(
    source_snapshot_date,
    _source_file_name
)

source_snapshot_date is derived from:

subjects_YYYYMMDD.csv

The filename is used as a deterministic tie-breaker.

No explicit delete indicator currently exists in the source feed, so hard-delete semantics are not implemented.

⸻

14. Subject SCD Type 2

The subject dimension maintains historical versions of changing subject attributes.

Conceptually:

SUB-001
│
├── Version 1
│
├── Version 2
│
└── Current Version

AUTO CDC maintains SCD Type 2 metadata including:

__START_AT
__END_AT

Because the sequencing expression is a compound structure, these boundaries represent source-change ordering rather than clinical-event effective dates.

For that reason, clinical dates such as:

visit_date
collection_date
onset_date

are not compared directly with these SCD boundaries.

Downstream Silver referential-integrity checks use the current trusted subject version:

__END_AT IS NULL

⸻

15. Subject Data Quality

Subject validation includes rules covering:

missing subject identifier
missing / unknown study
missing / unknown site
site-study inconsistency
age outside accepted range
unmappable sex
unknown baseline diagnosis
invalid source sequence date
missing screening date
enrollment before screening
consent after enrollment
randomization before enrollment
discontinuation before enrollment
discontinuation before randomization
invalid subject status
missing required arm
unknown study arm

Any blocking failure routes the record to:

quarantine.subjects

The quarantine record retains all DQ failure reasons rather than only the first failure.

⸻

16. Visit Pipeline

Source:

bronze.edc_visits

Target:

silver.visits

Rejected records:

quarantine.visits

Source exploration showed:

18,262 rows
18,262 distinct visit IDs

No duplicate/reissued visit IDs were observed in the current source files, so the current implementation treats visits as append-oriented.

This assumption should continue to be monitored as additional source files arrive.

⸻

17. Visit Data Quality

Visit validation includes:

business-key validation
subject existence
study existence
site existence
subject-study consistency
subject-site consistency
visit-status validation
visit-date validation

A missing visit date is not universally invalid.

For example, a missing date can be acceptable for statuses such as:

MISSED
RESCHEDULED

but a completed visit requires an appropriate visit date.

Referential integrity is evaluated against trusted Silver entities rather than directly against raw Bronze data.

⸻

18. Laboratory Results Pipeline

Source:

bronze.lab_results

Target:

silver.lab_results

Rejected records:

quarantine.lab_results

Exploration identified approximately:

65K laboratory-result records

with no repeated lab_result_id values in the observed source.

⸻

19. Laboratory Unit Standardization

Laboratory measurements can arrive in different units.

Silver standardizes measurements using reference mappings.

The basic transformation is:

standardized_result_value
        =
result_value × conversion_factor

The pipeline retains both:

original result
original unit

and:

standardized result
standard unit

to preserve source traceability.

⸻

20. Laboratory Reference Ranges

The source-provided reference range is retained for auditability:

source_reference_low
source_reference_high

A standardized reference range from the laboratory-test reference dataset is used for analytical abnormality classification:

standard_reference_low
standard_reference_high

This separates:

source-reported clinical context

from:

platform-standardized analytical logic

⸻

21. Derived Laboratory Abnormality

Silver independently derives an abnormality indicator from the standardized measurement:

standardized_result_value < standard_reference_low
                    OR
standardized_result_value > standard_reference_high

The original source abnormal flag is retained.

The pipeline therefore supports comparison between:

source_abnormal_flag

and:

derived_abnormal_flag

using an abnormal-flag discrepancy indicator.

This improves auditability rather than silently overwriting the source classification.

⸻

22. Laboratory Data Quality

Blocking laboratory rules include checks for:

missing lab_result_id
missing subject_id
missing study_id
missing visit_id
missing lab_test_code
missing result value
missing result unit
unknown Silver subject
subject-study mismatch
unknown Silver visit
visit-subject mismatch
visit-study mismatch
unknown lab test
unmapped test/unit combination
invalid standard reference range
invalid collection date

Invalid records are routed to:

quarantine.lab_results

⸻

23. Adverse Event Pipeline

Source:

bronze.safety_adverse_events

Target:

silver.adverse_events

Rejected records:

quarantine.adverse_events

Exploration identified approximately:

1,952 adverse-event records

with no duplicate AE identifiers in the observed source.

The current implementation is therefore append-oriented.

⸻

24. Adverse Event Validation

Blocking AE validation includes:

missing AE identifier
missing subject
unknown Silver subject
subject-study mismatch
subject-site mismatch
missing study
missing site
unknown site
site-study mismatch
invalid onset date

Invalid events are routed to:

quarantine.adverse_events

⸻

25. Clinical Warning Flags

Not every unusual clinical relationship should cause data rejection.

Some relationships are therefore retained as warning indicators rather than blocking DQ rules.

Examples include:

severity mapping warning
resolution before onset warning
high-severity non-serious warning
mild serious-event warning
fatal non-serious warning

This distinction is intentional:

DQ failure
    → record cannot be trusted structurally
    → quarantine
Clinical warning
    → record may be unusual
    → retain + flag for review

This avoids incorrectly rejecting potentially legitimate clinical events.

⸻

26. Severity vs Seriousness

Adverse-event severity and regulatory seriousness are treated as separate concepts.

The severity reference currently includes:

Grade 1
Grade 2
Grade 3
Grade 4 – LIFE_THREATENING
Grade 5 – DEATH

Severity does not automatically determine whether an event is regulatory-serious.

Potential inconsistencies are therefore exposed as warning flags rather than automatically rewriting the source value.

⸻

27. Referential Integrity Strategy

Silver facts do not use raw Bronze entities as their trusted referential-integrity authority.

The relationship is:

Bronze
   │
   ▼
Validated Silver Subject
   │
   ├── Visits
   ├── Laboratory Results
   └── Adverse Events

This prevents an invalid Bronze subject from legitimizing downstream clinical facts.

⸻

28. Quarantine Architecture

Every major Silver pipeline separates records into:

VALID
  ↓
Silver
INVALID
  ↓
Quarantine

Quarantine tables preserve:

original business attributes
source lineage
DQ failure array
human-readable failure reasons
quarantine timestamp

This provides observability and makes rejected records explainable.

⸻

29. Data Lineage

Operational metadata is preserved through the pipeline where appropriate.

Examples include:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date

This supports:

Silver record
      ↓
Bronze record
      ↓
Original source file

and improves troubleshooting and auditability.

⸻

30. Unity Catalog Managed Storage

Managed storage has been configured for the medallion schemas.

Logical roots:

Bronze:
s3://clinical-trial-intelligence-platform-sk/UnityManaged/bronze
Silver:
s3://clinical-trial-intelligence-platform-sk/UnityManaged/silver
Quarantine:
s3://clinical-trial-intelligence-platform-sk/UnityManaged/quarantine
Gold:
s3://clinical-trial-intelligence-platform-sk/UnityManaged/gold

Unity Catalog creates internal managed paths beneath these storage roots using its own __unitystorage hierarchy.

These internal files should not be directly manipulated from S3.

⸻

31. Current Pipeline Result

The Silver Lakeflow pipeline has successfully executed.

Current approximate pipeline outputs are:

subjects             ~3.6K valid source subject changes
visits               ~18K
lab_results           ~63K
adverse_events        ~2K

Subject storage is SCD Type 2, so the physical Silver subject row count must be interpreted as historical versions rather than simply as one row per source subject.

Invalid records are independently available in the corresponding quarantine datasets.

Exact counts are verified through the validation layer rather than relying on UI-rounded pipeline counts.

⸻

32. Validation Strategy

Validation is performed independently from transformation logic.

The validation layer checks areas such as:

Bronze → Silver reconciliation
business-key uniqueness
SCD current-record uniqueness
referential integrity
reference mapping conflicts
quarantine counts
DQ reason distribution
NULL behavior
clinical relationship consistency
source-to-standardized measurement behavior

Reference-conflict checks have already completed successfully.

The next stage is complete Silver post-run validation.

⸻

33. Current Development Status

AWS S3 Landing Zone                  COMPLETE
        │
        ▼
Unity Catalog                       COMPLETE
        │
        ▼
Bronze ingestion                    COMPLETE
        │
        ▼
Bronze validation                   COMPLETE
        │
        ▼
Source exploration                  COMPLETE
        │
        ▼
Silver dimensions/references        COMPLETE
        │
        ▼
Subject SCD2                        COMPLETE
        │
        ▼
Visit pipeline                      COMPLETE
        │
        ▼
Laboratory pipeline                 COMPLETE
        │
        ▼
Adverse-event pipeline              COMPLETE
        │
        ▼
Quarantine framework                COMPLETE
        │
        ▼
Reference conflict validation       COMPLETE
        │
        ▼
Silver pipeline execution           COMPLETE
        │
        ▼
Silver post-run validation          IN PROGRESS
        │
        ▼
Gold analytical layer              NEXT
        │
        ▼
Clinical KPIs / Dashboard           PLANNED

⸻

34. Planned Gold Layer

The Gold layer will expose analytics-ready clinical trial datasets.

Planned areas include:

Subject analytical spine
Study / site operational metrics
Enrollment and retention metrics
Visit compliance
Adverse-event analytics
Laboratory abnormality analytics
Data-quality monitoring

Gold datasets will be derived only from trusted Silver data.

Where CDISC concepts are used, the project will describe datasets as:

SDTM-inspired
ADaM-inspired

rather than claiming formal regulatory compliance.

⸻

35. Important Clinical Modeling Constraints

The platform intentionally avoids deriving variables when the required source information does not exist.

For example:

TEAE

requires a defensible treatment-start / first-dose timestamp.

A randomization date is not assumed to be equivalent to first dose.

Similarly:

SAFFL

should not be interpreted as “received treatment” unless exposure data supports that conclusion.

Exposure-adjusted adverse-event rates also require an appropriate exposure or follow-up denominator.

These variables will therefore only be introduced when their source requirements are available.

⸻

36. Engineering Principles Demonstrated

This project demonstrates:

* Medallion architecture
* AWS S3 integration
* Unity Catalog governance
* Lakeflow Declarative Pipelines
* Delta Lake managed tables
* streaming ingestion
* CDC processing
* SCD Type 2 modeling
* deterministic transformations
* reusable PySpark utilities
* schema standardization
* business-key normalization
* reference-data management
* referential integrity
* clinical data-quality rules
* quarantine architecture
* lineage preservation
* laboratory unit standardization
* clinical warning flags
* source-vs-derived reconciliation
* validation-driven pipeline development

⸻

37. Next Steps

The immediate development sequence is:

1. Complete Silver post-run validation
              ↓
2. Reconcile Silver and quarantine outputs
              ↓
3. Validate SCD2 subject history
              ↓
4. Validate cross-entity referential integrity
              ↓
5. Build Gold analytical model
              ↓
6. Build clinical operational KPIs
              ↓
7. Build analytical dashboard
              ↓
8. Add orchestration / monitoring
              ↓
9. Add CI/CD and deployment controls

⸻

38. Project Status

Current milestone: Silver Layer Implemented

The platform currently provides a governed and validated transformation path from raw clinical operational data in AWS S3 through Bronze and Silver, including SCD Type 2 subject history, standardized laboratory results, adverse-event validation, referential-integrity controls, and record-level quarantine.

The next milestone is the Gold analytical layer.

One correction I deliberately made in the README: I did **not** state that the `186` shown beside `subjects` in the pipeline UI is the Silver subject row count. In your screenshot, that number can represent update/output metrics in the pipeline UI and should not be used as the definitive physical table count. We'll put exact Silver/quarantine counts into the README **after running the validation SQL**.
Also, the storage description is accurate: Unity Catalog managed tables remain physically in your AWS account, and schema-level managed locations determine where new managed objects are stored; Unity Catalog creates hashed `__unitystorage` paths underneath the configured storage root.  [oai_citation:0‡Databricks Docs](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage?utm_source=chatgpt.com) AUTO CDC is also the current Databricks API for SCD Type 1/2 processing.  [oai_citation:1‡Databricks Docs](https://docs.databricks.com/gcp/en/ldp/cdc?utm_source=chatgpt.com)
So after Silver validation, we should update **Section 31 with exact counts**, then start Gold.