Yes. At this point I would freeze the Bronze → Silver architecture and document it before starting visits.py.

Below is a README section you can put into the project. It documents what you have actually implemented so far, including S3 landing, Auto Loader, Bronze, Silver, subject SCD2, reference mapping, and the new Lakeflow Job orchestration. Databricks’ current documentation supports this separation: pipelines declaratively define datasets and transformations, while Lakeflow Jobs procedurally orchestrates multiple pipeline tasks and their dependencies.  

⸻

Clinical Trial Intelligence Platform

Bronze → Silver Data Engineering Architecture

Current Implementation Status

The project currently implements the first two layers of a medallion-style clinical trial data platform:

                     AWS S3
                       │
                       ▼
                 LANDING ZONE
                       │
                       ▼
               ┌───────────────┐
               │  Auto Loader  │
               └───────┬───────┘
                       │
                       ▼
              BRONZE ETL PIPELINE
              clinical-trial-bronze
                       │
                       ▼
              Bronze Managed Tables
                       │
                       │
          Lakeflow Job dependency
                       │
                       ▼
              SILVER ETL PIPELINE
              clinical-trial-silver
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
     Reference     Dimensions    Clinical
       Data                       Entities
                                   │
                                   ▼
                               SUBJECTS
                                SCD2

The complete execution is coordinated by:

clinical-trial-intelligence-workflow

The current workflow contains:

bronze_ingestion
       │
       │ Depends on:
       │ SUCCESS
       ▼
silver_transformation

⸻

1. End-to-End Data Flow

The current data flow is:

SOURCE SYSTEMS
      │
      ▼
AWS S3 Landing Zone
      │
      │ Raw CSV files
      ▼
Auto Loader
      │
      ▼
Bronze ETL Pipeline
      │
      ▼
clinical_trial_intelligence.bronze.*
      │
      │ Bronze pipeline SUCCESS
      ▼
Lakeflow Job
      │
      ▼
Silver ETL Pipeline
      │
      ├── Standardization
      ├── Type conversion
      ├── Reference mapping
      ├── Data-quality processing
      ├── Business transformations
      └── Historical processing
              │
              ▼
clinical_trial_intelligence.silver.*

⸻

2. Layer 0 — AWS S3 Landing Zone

The landing zone is the external entry point for source data.

Example:

AWS S3
│
└── Landing/
    │
    ├── EDC/
    │   ├── subjects/
    │   ├── visits/
    │   ├── lab_results/
    │   └── adverse_events/
    │
    └── other source domains/

For subjects, multiple files represent snapshots of the source system:

subjects_20260825.csv
subjects_20260827.csv
subjects_20260828.csv
subjects_20260831.csv
subjects_20260901.csv
subjects_20260902.csv
subjects_20260903.csv
subjects_20260904.csv
subjects_20260907.csv

These are not treated as unrelated files.

Each file represents the state of the EDC subject dataset at a particular point in time.

For example:

subjects_20260825.csv
        │
        └── Snapshot Date = 2026-08-25

The filename therefore contains meaningful CDC ordering information.

⸻

3. Incremental File Ingestion — Auto Loader

The Bronze pipeline uses Databricks Auto Loader for ingestion from the cloud landing area.

Conceptually:

S3 Landing
     │
     │ New file arrives
     ▼
Auto Loader
     │
     ▼
Bronze

Auto Loader uses the cloudFiles source to incrementally discover and process files arriving in cloud object storage, including Amazon S3.  

Therefore the ingestion model is not:

Every execution
      ↓
Read every S3 file again

It is conceptually:

Previously processed files
          +
Newly discovered files
          │
          ▼
     Auto Loader
          │
          ▼
Process incremental input

This provides the ingestion foundation for the Bronze layer.

⸻

4. Bronze ETL Pipeline

Pipeline:

clinical-trial-bronze

The Bronze pipeline is responsible primarily for source ingestion and raw persistence.

Its responsibility is different from the Silver layer.

                    BRONZE
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
     Ingestion     Metadata       Raw source
                   capture        preservation

The Bronze layer should preserve source information with minimal business transformation.

⸻

5. Bronze Metadata

Along with business columns, ingestion metadata is retained.

For example:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date

For a subject record:

_source_file_name
=
subjects_20260825.csv

and:

_source_file
=
s3://.../Landing/EDC/subjects/subjects_20260825.csv

This provides lineage from:

Silver record
     ↓
Bronze record
     ↓
Source file
     ↓
S3 object

⸻

6. Why Bronze Does Not Perform the Subject Business Logic

Bronze answers:

What did the source system provide?

Silver answers:

What does that data mean in our standardized analytical model?

Therefore transformations such as:

M
m
Male
MALE

becoming:

M

belong in Silver rather than raw Bronze ingestion.

Likewise:

ENROLLED
DISCONTINUED

history is handled in Silver.

This keeps the architectural boundary clear:

BRONZE
Raw / traceable / source-oriented
             ↓
SILVER
Clean / standardized / validated /
business-oriented / historical

⸻

7. Silver ETL Pipeline

Pipeline:

clinical-trial-silver

The Silver pipeline reads the Bronze datasets and creates standardized clinical datasets.

Current Silver source structure includes code such as:

pipelines/
└── silver/
    │
    ├── dimensions.sql
    ├── subjects.py
    ├── visits.py
    ├── lab_results.py
    └── adverse_events.py

The pipeline also contains reference datasets used for standardization.

Conceptually:

BRONZE
   │
   ▼
SILVER PIPELINE
   │
   ├── Reference data
   ├── Dimensions
   ├── Subjects
   ├── Visits
   ├── Labs
   └── Adverse Events

Lakeflow pipelines can contain streaming tables, materialized views and views, and automatically resolve dependencies between datasets within the pipeline.  

⸻

8. Subject Pipeline — First Completed Clinical Entity

The first major clinical entity completed in Silver is:

clinical_trial_intelligence.silver.subjects

Source:

clinical_trial_intelligence.bronze.edc_subjects

Flow:

S3
 │
 ▼
subjects_YYYYMMDD.csv
 │
 ▼
Bronze
edc_subjects
 │
 ▼
subjects.py
 │
 ├── Snapshot discovery
 ├── Snapshot ordering
 ├── Data cleaning
 ├── Data-type conversion
 ├── Sex reference mapping
 ├── Business-key validation
 └── SCD Type 2
 │
 ▼
Silver
subjects

⸻

9. Problem Identified in Subject Data

The Bronze validation showed an important characteristic of the data.

The first snapshot contained:

subjects_20260825.csv
3359 records
3359 distinct subjects

Subsequent snapshots contained:

subjects_20260827.csv   55
subjects_20260828.csv   55
subjects_20260831.csv   55
subjects_20260901.csv   55
subjects_20260902.csv   55
subjects_20260903.csv   55
subjects_20260904.csv   55
subjects_20260907.csv   55

Within each snapshot:

COUNT(*) = COUNT(DISTINCT subject_id)

Therefore:

(subject_id, source_snapshot_date)

is unique.

This established that snapshot date could be used as the ordering/version mechanism for historical processing.

⸻

10. Snapshot Date Extraction

The source filename:

subjects_20260825.csv

is converted into:

20260825

and:

source_snapshot_date = 2026-08-25

The snapshots are then processed chronologically:

20260825
   ↓
20260827
   ↓
20260828
   ↓
20260831
   ↓
20260901
   ↓
...

This is essential because SCD2 history must be reconstructed in temporal order.

⸻

11. Subject Business Key

The business key is:

subject_id

Before processing:

TRIM(subject_id)

is performed and invalid keys are removed.

Validation confirmed:

invalid_subject_keys = 0

Therefore every retained Silver subject history record has a usable business key.

⸻

12. Subject Standardization

Subject attributes are standardized before historical comparison.

Examples include:

subject_id
study_id
site_id
arm_code
subject_status
baseline_condition_code
age
screening_date
enrollment_date
randomization_date
informed_consent_date
discontinuation_date
discontinuation_reason
sex

Typical processing includes:

TRIM
UPPER
CAST
NULL normalization
DATE conversion

For example:

" enrolled "
       ↓
TRIM
       ↓
"enrolled"
       ↓
UPPER
       ↓
"ENROLLED"

Placeholder values such as:

""
"-"

are converted where appropriate to:

NULL

⸻

13. Reference Data Mapping

Sex is not standardized using a hard-coded chain such as:

if sex == "Male":
    ...

Instead, a Silver reference dataset is used:

clinical_trial_intelligence.silver.ref_sex

The reference data contains:

raw_sex          standard_sex
--------------------------------
M                M
m                M
MALE             M
Male             M
F                F
f                F
FEMALE           F
Female           F

The mapping process is:

Bronze raw sex
      │
      ▼
TRIM
      │
      ▼
UPPER
      │
      ▼
Normalized lookup key
      │
      ▼
ref_sex
      │
      ▼
standard_sex

Example:

Male
 │
 ▼
MALE
 │
 ▼
ref_sex
 │
 ▼
M

Validation confirmed the final Silver subject values currently contain:

M
F

with observed counts:

M = 1904
F = 1895

The reference validation also established that case variants mapping to the same normalized raw value do not produce conflicting standardized values.

⸻

14. Why the Reference Table Is Broadcast

The reference dataset is small relative to the subject dataset.

Therefore the subject pipeline uses a broadcast join conceptually:

Large Subject Dataset
          │
          │
          ├──────────┐
          │          │
          ▼          ▼
       Subjects    ref_sex
                     small
          │          │
          └────┬─────┘
               ▼
             JOIN

This avoids treating a small lookup table like a large distributed join input.

⸻

15. Unmapped Reference Handling

If a raw sex value does not exist in the reference table:

Raw value
   │
   ▼
Reference lookup
   │
   └── no match
          │
          ▼
      UNMAPPED

This is intentional.

It avoids silently converting unknown source values into incorrect standardized values.

⸻

16. Why SCD Type 2 Was Required

Subject attributes can change over time.

For example:

Subject 102-003-0078
2026-08-25
ENROLLED
   │
   ▼
2026-08-27
DISCONTINUED

If only the latest record were retained:

102-003-0078 → DISCONTINUED

we would lose the fact that the subject was previously:

ENROLLED

Therefore Silver subjects uses:

SCD TYPE 2

rather than simply overwriting the previous row.

Lakeflow pipelines provide AUTO CDC support for SCD Type 1 and Type 2 processing, including snapshot-oriented CDC functionality.  

⸻

17. AUTO CDC FROM SNAPSHOT

The implementation uses:

dp.create_auto_cdc_from_snapshot_flow(...)

with:

keys = ["subject_id"]
stored_as_scd_type = 2

Conceptually:

Snapshot 1
    │
    ▼
Snapshot 2
    │
 Compare subject attributes
    │
    ├── No business change
    │       ↓
    │   No new SCD version
    │
    └── Business change
            ↓
       Close old version
            ↓
       Create new version

⸻

18. Preventing False History

One important design problem was distinguishing:

BUSINESS CHANGE

from:

INGESTION / LINEAGE CHANGE

Fields such as:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date
source_snapshot_date

naturally change between snapshots.

Those changes should not by themselves create a new business version of the subject.

Therefore they are excluded from history tracking.

Conceptually:

Subject A
Snapshot 1:
status = ENROLLED
file   = subjects_20260825.csv
Snapshot 2:
status = ENROLLED
file   = subjects_20260827.csv

The filename changed.

But the business state did not.

Therefore:

❌ Do not create new SCD version

Whereas:

Snapshot 1:
status = ENROLLED
Snapshot 2:
status = DISCONTINUED

is:

✅ Genuine business change

and creates a new historical version.

⸻

19. SCD2 Output

Databricks maintains:

__START_AT
__END_AT

for the SCD history.

Example from the resulting dataset:

subject_id       status         __START_AT   __END_AT
------------------------------------------------------
102-003-0078     ENROLLED       20260825     20260827
102-003-0078     DISCONTINUED   20260827     20260828
...

The current record has:

__END_AT = NULL

Therefore:

WHERE __END_AT IS NULL

returns the current subject state.

⸻

20. SCD2 Validation Results

The completed subject table produced:

Total SCD rows       = 3799
Distinct subjects    = 3359
Current rows         = 55

This is meaningful.

Because:

3799 > 3359

the table contains historical versions rather than simply one row per subject.

At the same time, validation showed no subject having more than one current record.

Expected:

subject_id | current_record_count
---------------------------------
NO ROWS

Observed:

NO ROWS

Therefore the current-state invariant holds:

For each subject:
COUNT(current version) <= 1

⸻

21. Example Historical Evolution

One resulting history illustrates the design.

For:

102-003-0078

history includes:

2026-08-25
ENROLLED
age = 77
sex = M
arm = B
      │
      ▼
2026-08-27
DISCONTINUED
age = 77
sex = M
arm = B
discontinuation_date = 2026-08-23

Another subject demonstrates that changes other than status can also create historical versions:

104-003-0067
ENROLLED
age = 37
      │
      ▼
ENROLLED
age = 38
      │
      ▼
DISCONTINUED

Therefore SCD2 is tracking changes in relevant business attributes, not merely subject status.

⸻

22. Pipeline vs Job Boundary

The project deliberately separates data transformation from workflow orchestration.

ETL Pipelines

ETL pipelines answer:

HOW should this dataset be processed?

Current pipelines:

clinical-trial-bronze
clinical-trial-silver

They contain the actual ingestion and transformation definitions.

A Lakeflow pipeline automatically resolves dependencies among the datasets defined within that pipeline.  

⸻

23. Lakeflow Job

The workflow is:

clinical-trial-intelligence-workflow

It contains:

Task 1
bronze_ingestion
Type:
Pipeline
Pipeline:
clinical-trial-bronze

followed by:

Task 2
silver_transformation
Type:
Pipeline
Pipeline:
clinical-trial-silver
Depends on:
bronze_ingestion
Run if dependencies:
All succeeded

Lakeflow Jobs is specifically designed to coordinate multiple tasks and supports task dependencies, scheduling, retries, conditional execution and other workflow control.  

⸻

24. Final Orchestration Logic

The current operational flow is:

                   JOB START
                       │
                       ▼
             ┌─────────────────┐
             │ bronze_ingestion│
             └────────┬────────┘
                      │
              Run Bronze Pipeline
                      │
             ┌────────┴────────┐
             │                 │
           FAIL              SUCCESS
             │                 │
             ▼                 ▼
           STOP      ┌─────────────────────┐
                     │silver_transformation│
                     └──────────┬──────────┘
                                │
                        Run Silver Pipeline
                                │
                       ┌────────┴────────┐
                       │                 │
                     FAIL              SUCCESS
                       │                 │
                       ▼                 ▼
                  JOB FAILED        JOB SUCCESS

This prevents Silver from processing against a failed upstream Bronze execution.

⸻

25. Why a Job Was Used Instead of a Third ETL Pipeline

A third ETL pipeline was not created merely to execute Bronze and then Silver.

That would mix two different concerns.

Pipeline
    =
Dataset processing /
transformation relationships
Job
    =
Workflow/task relationships

Databricks describes this distinction directly: Lakeflow Jobs provides a procedural approach to task relationships, while Lakeflow pipelines provide a declarative approach to dataset and transformation relationships.  

Databricks also recommends a workflow orchestrator when coordinating work outside the dependency graph of a single pipeline.  

⸻

26. Current Architecture

The platform now operates as:

                    SOURCE SYSTEMS
                          │
                          ▼
                       AWS S3
                          │
                          ▼
                     LANDING ZONE
                          │
                          ▼
                     Auto Loader
                          │
                          ▼
              ┌─────────────────────┐
              │ BRONZE ETL PIPELINE │
              │ clinical-trial-     │
              │ bronze              │
              └──────────┬──────────┘
                         │
                         ▼
                Bronze Managed Data
                         │
                         │
              JOB dependency:
               "All succeeded"
                         │
                         ▼
              ┌─────────────────────┐
              │ SILVER ETL PIPELINE │
              │ clinical-trial-     │
              │ silver              │
              └──────────┬──────────┘
                         │
            ┌────────────┼──────────────┐
            │            │              │
            ▼            ▼              ▼
        Reference     Dimensions     Clinical
          Data                        Entities
                                       │
                                       ▼
                                    Subjects
                                     SCD2

⸻

27. Responsibility Matrix

Component	Responsibility
AWS S3 Landing	Raw source-file arrival
Auto Loader	Incremental cloud-file discovery and ingestion
Bronze Pipeline	Raw ingestion and source-oriented persistence
Bronze Tables	Traceable source data
Silver Pipeline	Cleaning, standardization and business transformation
Reference Tables	Controlled source-to-standard mappings
subjects.py	Subject standardization and historical processing
AUTO CDC From Snapshot	Snapshot comparison and SCD processing
SCD Type 2	Preserve historical subject states
Lakeflow Job	Cross-pipeline orchestration
bronze_ingestion task	Execute Bronze pipeline
silver_transformation task	Execute Silver after successful Bronze

⸻

28. Current Completion Point

At this stage:

S3 Landing                    ✅
       ↓
Auto Loader                   ✅
       ↓
Bronze ETL                    ✅
       ↓
Bronze datasets               ✅
       ↓
Reference datasets            ✅
       ↓
Silver dimensions             ✅
       ↓
Subject standardization       ✅
       ↓
Sex reference mapping         ✅
       ↓
Snapshot discovery            ✅
       ↓
SCD Type 2                    ✅
       ↓
Subject validation            ✅
       ↓
Bronze → Silver Job           ✅
       ↓
Workflow execution            ✅

The workflow has also been executed successfully end-to-end:

bronze_ingestion       SUCCESS
        ↓
silver_transformation  SUCCESS

So the platform-level Bronze → Silver path is established.

⸻

29. Next Development Stage

Now we should not redesign the orchestration again.

The architecture stays:

                 S3
                  ↓
                Bronze
                  ↓
                  │
               JOB DAG
                  │
                  ↓
                Silver
                  │
       ┌──────────┼──────────┐
       ▼          ▼          ▼
   Subjects     Visits      Labs
      ✅          NEXT

The next task is therefore:

visits.py

But visits.py should not simply be a copy of subjects.py.

Before implementing it, we should first determine the grain, business key, snapshot/CDC behaviour, duplicates, relationship to subject_id, visit sequencing, date behaviour, and required reference mappings of the Bronze visits dataset. Only after those validations should we decide whether Visits requires SCD2, SCD1, snapshot CDC, or another incremental strategy.

That is the right stopping point for the README before moving to Visits.


Area

Question

notebook/exploration

What is happening in my data?

pipelines

What should I do to the data?

Validation

Did what I built produce the correct data?

i want to follow this thing so that anyone in github also can see the expolation notebook regarding the data so that they can understand what is going on and see the difference between all 3 validatation ,notebook,transformatoin