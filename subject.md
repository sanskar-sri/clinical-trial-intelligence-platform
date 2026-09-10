Yes. For your repository README, I would document subjects.py at a much more granular engineering level than just saying “Bronze → Silver.” The important part is to show what the source problem was, why the first implementation failed, how you diagnosed it, what mappings are applied, how snapshot CDC works, and how you validated the final SCD2 output.

Below is a README section you can directly add to your project.

⸻

Silver Subject Pipeline — Snapshot CDC & SCD Type 2

1. Purpose

The Subject Silver pipeline transforms raw EDC subject snapshots from the Bronze layer into a standardized, history-preserving Silver dataset.

Source

clinical_trial_intelligence.bronze.edc_subjects

Target

clinical_trial_intelligence.silver.subjects

The pipeline performs four major functions:

Raw EDC snapshots
        ↓
Data cleaning and standardization
        ↓
Reference-data mapping
        ↓
Snapshot-to-snapshot change detection
        ↓
SCD Type 2 history
        ↓
Silver subjects

Unlike a conventional overwrite pipeline, the Subject pipeline must preserve how a subject changes over time.

For example:

Subject 102-003-0078
2026-08-25
ENROLLED
     ↓
2026-08-27
DISCONTINUED
     ↓
2026-09-01
DISCONTINUED
(discontinuation date changed)

For this reason, the Silver target uses Slowly Changing Dimension Type 2 (SCD2).

Databricks AUTO CDC FROM SNAPSHOT is appropriate when periodic snapshots are available instead of a native CDC event feed. It compares consecutive snapshots and derives inserts, updates, and deletions before applying SCD logic.  

⸻

2. Source Data Pattern

Subject data arrives as date-versioned EDC snapshot files.

Example:

Landing/
└── EDC/
    └── subjects/
        ├── subjects_20260825.csv
        ├── subjects_20260827.csv
        ├── subjects_20260828.csv
        ├── subjects_20260831.csv
        ├── subjects_20260901.csv
        ├── subjects_20260902.csv
        ├── subjects_20260903.csv
        ├── subjects_20260904.csv
        └── subjects_20260907.csv

These files are ingested into:

clinical_trial_intelligence.bronze.edc_subjects

Bronze preserves both the business data and ingestion lineage.

Important lineage columns include:

_source_file
_source_file_name
_source_file_modification_ts
_ingestion_ts
_ingestion_date

Therefore a Bronze record contains two different categories of information:

                     BRONZE SUBJECT
                           │
             ┌─────────────┴─────────────┐
             │                           │
       Business Data               Technical Metadata
             │                           │
       subject_id                   _source_file
       study_id                     _source_file_name
       site_id                      _ingestion_ts
       status                       _ingestion_date
       age                          ...
       sex
       arm
       dates

This distinction becomes important for SCD2 history tracking.

⸻

3. Snapshot Version Identification

The Bronze table itself contains records from multiple snapshot files.

The logical snapshot date is encoded in the filename:

subjects_20260825.csv
         │
         └──────── 20260825

The pipeline extracts this using:

F.regexp_extract(
    F.col("_source_file_name"),
    r"subjects_(\d{8})\.csv",
    1
)

and converts the extracted value into a date:

F.to_date(..., "yyyyMMdd")

Therefore:

subjects_20260825.csv
        ↓
"20260825"
        ↓
2026-08-25
        ↓
source_snapshot_date

Before implementing CDC, the filename parsing was validated.

Result:

invalid_snapshot_dates = 0

This confirms that every relevant subject filename could be converted into a valid snapshot date.

⸻

4. Snapshot Inventory Validation

The Bronze layer was then profiled by snapshot.

Observed results:

Snapshot	Rows	Distinct Subjects
2026-08-25	3,359	3,359
2026-08-27	55	55
2026-08-28	55	55
2026-08-31	55	55
2026-09-01	55	55
2026-09-02	55	55
2026-09-03	55	55
2026-09-04	55	55
2026-09-07	55	55

An important validation was:

row_count = distinct_subjects

for every individual snapshot.

This verifies that a subject occurs at most once inside a particular snapshot.

Conceptually:

(subject_id, source_snapshot_date)
                ↓
             UNIQUE

This is essential because the CDC engine must receive one source state for a business key within a given snapshot version.

⸻

5. Business Key

The Subject entity uses:

subject_id

as its business key.

Example:

subject_id = 102-003-0078

The CDC flow therefore declares:

keys=["subject_id"]

This tells the CDC engine that records across different snapshots having the same subject_id represent different temporal states of the same business entity, rather than different subjects. Databricks defines keys as the columns that uniquely identify source records for CDC processing.  

⸻

6. Chronological Snapshot Processing

Snapshots must not be processed arbitrarily.

The pipeline discovers all available snapshot dates:

20260825
20260827
20260828
20260831
20260901
20260902
20260903
20260904
20260907

and sorts them chronologically.

The custom provider:

next_subject_snapshot(latest_snapshot_version)

determines which snapshot should be processed next.

The behavior is:

latest_snapshot_version = None
              ↓
       return 20260825
latest_snapshot_version = 20260825
              ↓
       return 20260827
latest_snapshot_version = 20260827
              ↓
       return 20260828
              ...
latest_snapshot_version = 20260907
              ↓
            None

Databricks requires historical snapshots to be supplied with an associated snapshot version and processed in ascending version order.  

⸻

7. Snapshot Isolation

After determining the next version, the pipeline reads only that logical snapshot.

For example:

next_version
     =
20260827

causes:

Bronze edc_subjects
        │
        ├── 20260825
        ├── 20260827  ← SELECTED
        ├── 20260828
        ├── 20260831
        └── ...

The result is one DataFrame representing one point-in-time source state.

The provider then returns:

return source, next_version

Conceptually:

(
    DataFrame for subjects_20260827.csv,
    20260827
)

⸻

8. Subject Data Standardization

Before CDC comparison, the snapshot is standardized.

This prevents meaningless formatting differences from being interpreted as business changes.

8.1 Business identifiers

The following identifiers are trimmed:

subject_id
study_id
site_id

Example:

" 104-003-0067 "
        ↓
"104-003-0067"

⸻

8.2 Arm Code

arm_code is normalized.

" a "
  ↓
"A"

while placeholders are converted to null:

""
"-"
 ↓
NULL

⸻

8.3 Subject Status

Subject status is trimmed and standardized to uppercase.

" enrolled "
      ↓
"ENROLLED"
"discontinued"
      ↓
"DISCONTINUED"

Placeholder values such as:

""
"-"

become:

NULL

⸻

9. Diagnosis Standardization

The source diagnosis field:

baseline_condition_code

is standardized by:

TRIM
  +
UPPER
  +
placeholder → NULL

Example:

" dx001 "
    ↓
"DX001"

This field can subsequently be associated with the Silver diagnosis reference:

clinical_trial_intelligence.silver.ref_diagnosis

whose validated structure is:

diagnosis_code
diagnosis_description

The Subject pipeline currently standardizes the diagnosis code itself; the supplied subjects.py does not perform a ref_diagnosis join. This distinction is intentional in the documentation so that the README reflects the actual implementation.

⸻

10. Age Standardization

Age is explicitly converted to an integer:

F.col("age").cast("int")

Example:

"73"
 ↓
73

This ensures Silver exposes age as a typed analytical attribute rather than an uncontrolled source string.

⸻

11. Date Standardization

Clinical date fields are converted to Spark DATE.

The pipeline processes:

screening_date
enrollment_date
randomization_date
informed_consent_date
discontinuation_date

Conceptually:

Raw
"2026-08-29"
      ↓
CAST DATE
      ↓
2026-08-29

This allows downstream date comparisons, interval calculations, cohort analysis, and trial timeline analysis without repeatedly parsing strings.

⸻

12. Discontinuation Reason Standardization

discontinuation_reason is:

TRIM
 +
UPPER
 +
"-" / empty → NULL

Therefore:

" adverse event "
        ↓
"ADVERSE EVENT"

while:

"-"
 ↓
NULL

⸻

13. Sex Reference Mapping

Sex is not hardcoded using a large CASE WHEN.

Instead, the pipeline uses reference-driven standardization.

Reference table:

clinical_trial_intelligence.silver.ref_sex

Schema:

raw_sex
standard_sex

The source value is first normalized:

F.upper(F.trim(F.col("sex")))

Then the reference key is normalized using exactly the same transformation.

This is important because the reference data contains case variants.

For example:

M
m
MALE
Male
F
f
FEMALE
Female

⸻

14. Reference Mapping Logic

The mapping works conceptually as:

SOURCE                         REFERENCE
sex                            raw_sex     standard_sex
---                            -------     ------------
m                              M           M
Male                           MALE        M
female                         FEMALE      F
F                              F           F

Both sides are normalized:

SOURCE                           REFERENCE
"m"                              "M"
 ↓                                ↓
UPPER(TRIM())                    UPPER(TRIM())
 ↓                                ↓
"M"             =               "M"
                 │
                 ▼
          standard_sex = M

The join is:

_raw_sex_clean
       =
_ref_raw_sex

and the reference is broadcast:

F.broadcast(sex_reference)

This is appropriate because the sex mapping is a small lookup/reference dataset compared with the subject fact-like dataset.

⸻

15. Duplicate Reference Mapping Problem

During implementation, an important issue was discovered in ref_sex.

An initial validation grouped the normalized raw_sex values and showed:

normalized_raw_sex     record_count
-----------------------------------
M                           2
FEMALE                      2
F                           2
MALE                        2

At first this looked like an invalid duplicate-reference problem.

However, further validation checked whether these duplicates mapped to different standard values.

The important test became:

COUNT(
    DISTINCT UPPER(TRIM(standard_sex))
)

per normalized raw value.

Result:

ZERO ROWS

for conflicting mappings.

Therefore:

M
m
      ──────→ M
MALE
Male
      ──────→ M
F
f
      ──────→ F
FEMALE
Female
      ──────→ F

The problem was not contradictory reference semantics.

It was case-variant duplication after normalization.

⸻

16. Why the First Pipeline Failed

The first implementation joined the source directly to the normalized reference table.

Consider:

Source
sex = "M"

After normalization:

_raw_sex_clean = "M"

But the reference effectively contained multiple rows normalizing to "M":

raw_sex
-------
M
m

After normalization:

M → M
m → M

The join therefore became:

ONE SOURCE ROW
      │
      ├──── match → M
      │
      └──── match → m
           ↓
TWO OUTPUT ROWS

Thus one subject could become two rows before entering CDC.

⸻

17. CDC Failure Produced by the Duplicate Join

The pipeline subsequently failed with an APPLY CHANGES FROM SNAPSHOT/snapshot CDC error.

The key error was effectively:

Found 2 rows for key:
{"subject_id":"104-012-0014"}
Expected at most 1 row per key.

The investigation then traced the problem backward:

CDC ERROR
"2 rows for subject_id"
          ↑
          │
Duplicate subject after transformation?
          ↑
          │
Bronze duplicate?
          │
          ├── Checked (subject_id, snapshot_date)
          │
          └── No duplicate
          ↑
Reference join?
          │
          └── YES
          ↑
Case variants in ref_sex

This was an important debugging point: the Bronze source was not duplicating the subject; the many-to-one reference normalization was turning a valid source row into multiple transformed rows.

⸻

18. Fix Applied

The reference table was normalized and then deduplicated before joining.

Conceptually:

RAW REFERENCE
M       M
m       M
MALE    M
Male    M
        ↓ normalize
M       M
M       M
MALE    M
MALE    M
        ↓ deduplicate
M       M
MALE    M

Therefore the Silver pipeline receives a deterministic mapping:

normalized raw value
          │
          │ exactly one row
          ▼
standard value

This restores the required relationship:

Source Subject
      1
      │
      │ reference lookup
      ▼
      1
Transformed Subject

rather than:

1 source subject
      ↓
N reference matches
      ↓
N transformed rows     ❌

⸻

19. Unmapped Sex Handling

The standardized value is generated using:

F.coalesce(
    F.col("_standard_sex"),
    F.lit("UNMAPPED")
)

Therefore:

Reference match
      ↓
M / F

while:

No reference match
      ↓
UNMAPPED

This design prevents unknown source values from silently becoming null.

An unexpected value such as:

sex = "UNKNOWN_CODE"

would therefore surface as:

sex = "UNMAPPED"

and could be detected by data-quality validation.

⸻

20. Final Sex Validation

After fixing the reference join, the Silver output contained:

Standard Sex	SCD Records
M	1,904
F	1,895

Total:

1904 + 1895 = 3799

which matches:

total_scd_rows = 3799

No UNMAPPED category appeared in the final profile.

Therefore the reference mapping successfully covered the subject data processed by the pipeline.

⸻

21. Why SCD Type 2 Was Selected

Subject attributes are mutable.

A subject can transition through clinical states such as:

SCREENED
   ↓
ENROLLED
   ↓
RANDOMIZED
   ↓
DISCONTINUED / COMPLETED

Other attributes may also change between snapshots:

age
subject_status
arm_code
discontinuation_date
discontinuation_reason
etc.

If SCD Type 1 were used:

OLD
ENROLLED
   ↓ overwrite
DISCONTINUED
History lost ❌

With SCD Type 2:

ENROLLED
20260825 → 20260827
DISCONTINUED
20260827 → NULL

both states can be queried.

Databricks describes SCD2 as retaining historical versions rather than overwriting previous values.  

⸻

22. AUTO CDC FROM SNAPSHOT

The target is first declared as a streaming table:

dp.create_streaming_table(
    name="subjects"
)

Then the snapshot flow is applied:

dp.create_auto_cdc_from_snapshot_flow(
    target="subjects",
    source=next_subject_snapshot,
    keys=["subject_id"],
    stored_as_scd_type=2,
    ...
)

This architecture is:

             next_subject_snapshot()
                       │
                       ▼
              Snapshot DataFrame
                       +
                Snapshot Version
                       │
                       ▼
         AUTO CDC FROM SNAPSHOT
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       INSERT        UPDATE       DELETE/
                                  disappearance
          │            │            │
          └────────────┼────────────┘
                       ▼
                    SCD2
                       │
                       ▼
             silver.subjects

Databricks compares snapshots to infer changes rather than requiring the source files themselves to contain INSERT, UPDATE, and DELETE operation records.  

⸻

23. SCD2 History Columns

Lakeflow maintains:

__START_AT
__END_AT

These define the validity interval of each subject version.  

Example from the final output:

subject_id       status          __START_AT   __END_AT
------------------------------------------------------
102-003-0078     ENROLLED        20260825     20260827
102-003-0078     DISCONTINUED    20260827     20260828
102-003-0078     DISCONTINUED    20260901     20260902

Interpretation:

             Subject 102-003-0078
20260825                 20260827
   │────────────────────────│
           ENROLLED
                         20260828
                            │
20260827 ───────────────────│
       DISCONTINUED
20260901                 20260902
   │────────────────────────│
       DISCONTINUED
   with another tracked change

⸻

24. Current Record

The currently active SCD2 version is identified by:

__END_AT IS NULL

Example:

104-010-0065
DISCONTINUED
__START_AT = 20260907
__END_AT   = NULL

means this is the current known state for that subject.

⸻

25. Why Metadata Is Excluded From History Tracking

Not every column change represents a business change.

For example:

_source_file_name
_ingestion_ts
_ingestion_date
source_snapshot_date

naturally change as new files are processed.

Suppose the business state remains:

subject_status = ENROLLED
age            = 37
sex            = F
arm_code       = C

but:

_source_file_name
subjects_20260827.csv
        ↓
subjects_20260828.csv

If filename changes were tracked as SCD events, the system could create meaningless history:

ENROLLED → ENROLLED → ENROLLED → ENROLLED

simply because a new snapshot file arrived.

Therefore these operational columns are excluded:

track_history_except_column_list=[
    "_source_file",
    "_source_file_name",
    "_source_file_modification_ts",
    "_ingestion_ts",
    "_ingestion_date",
    "source_snapshot_date"
]

Databricks supports excluding selected columns from SCD2 history tracking so changes in those columns do not themselves generate historical versions.  

The principle is:

BUSINESS CHANGE
      ↓
Create history
METADATA CHANGE ONLY
      ↓
Do not create history

⸻

26. End-to-End Data Flow

The complete Subject data flow is:

┌───────────────────────────────────────┐
│ EDC SOURCE SNAPSHOTS                  │
│                                       │
│ subjects_20260825.csv                 │
│ subjects_20260827.csv                 │
│ subjects_20260828.csv                 │
│ ...                                   │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ LANDING / S3                          │
│ Landing/EDC/subjects/                 │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ BRONZE                                │
│ bronze.edc_subjects                   │
│                                       │
│ Raw business attributes               │
│ + ingestion lineage                   │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ SNAPSHOT DISCOVERY                    │
│                                       │
│ Parse YYYYMMDD from filename           │
│ → source_snapshot_date                │
│ → order snapshots                     │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ SNAPSHOT PROVIDER                     │
│ next_subject_snapshot()               │
│                                       │
│ Return next DataFrame + version       │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ CLEANING                              │
│                                       │
│ TRIM identifiers                      │
│ UPPER categorical values              │
│ "-" → NULL                            │
│ CAST age                              │
│ CAST clinical dates                   │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ REFERENCE STANDARDIZATION             │
│                                       │
│ source.sex                            │
│      ↓                                │
│ normalize                             │
│      ↓                                │
│ silver.ref_sex                        │
│      ↓                                │
│ M / F                                 │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ SNAPSHOT CDC                          │
│                                       │
│ Business Key: subject_id              │
│ Compare consecutive snapshots         │
└──────────────────┬────────────────────┘
                   │
             ┌─────┼─────┐
             ▼     ▼     ▼
          INSERT UPDATE DELETE/
                        ABSENT
             └─────┬─────┘
                   ▼
┌───────────────────────────────────────┐
│ SCD TYPE 2                            │
│                                       │
│ Preserve business history             │
│ __START_AT                            │
│ __END_AT                              │
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│ SILVER                                │
│ clinical_trial_intelligence           │
│        .silver.subjects               │
└───────────────────────────────────────┘

⸻

27. Debugging Journey

The most important engineering issue encountered during implementation was:

Pipeline Failure
      │
      ▼
AUTO CDC FROM SNAPSHOT:
multiple rows found for subject_id
      │
      ▼
Hypothesis 1:
Duplicate source subjects?
      │
      ▼
Validate Bronze
(subject_id, snapshot_date)
      │
      ▼
No duplicates
      │
      ▼
Inspect transformations
      │
      ▼
Reference mapping identified
      │
      ▼
ref_sex contains case variants
M/m, F/f, MALE/Male, FEMALE/Female
      │
      ▼
UPPER(TRIM()) collapses variants
      │
      ▼
Multiple reference rows match
one source row
      │
      ▼
Join duplicates subjects
      │
      ▼
CDC business-key uniqueness violated
      │
      ▼
Deduplicate normalized reference mapping
      │
      ▼
One normalized key → one standard value
      │
      ▼
Rerun Lakeflow pipeline
      │
      ▼
SUCCESS

This is an important part of the project because the solution was not to arbitrarily dropDuplicates() from the final Subject dataset. The actual duplication source was identified and corrected at the reference-mapping boundary.

⸻

28. Final Validation

After the fix, the Lakeflow pipeline completed successfully.

Basic profile

total_scd_rows     = 3799
distinct_subjects  = 3359
current_rows       = 55

Current-record uniqueness

Validation:

SELECT
    subject_id,
    COUNT(*) AS current_record_count
FROM clinical_trial_intelligence.silver.subjects
WHERE __END_AT IS NULL
GROUP BY subject_id
HAVING COUNT(*) <> 1;

Result:

0 rows

Therefore every currently active subject has exactly one active SCD2 record.

Business-key completeness

invalid_subject_keys = 0

Therefore no null or blank subject_id entered the target.

Sex standardization

M = 1904
F = 1895

No unexpected standardized category was observed.

⸻

29. Example Historical Subject

A real output pattern demonstrates that history is being preserved:

104-003-0067
ENROLLED
age = 37
20260825 → 20260827
        ↓ age/state changes across snapshots
ENROLLED
age = 38
20260828 → 20260831
        ↓
DISCONTINUED
age = 37
discontinuation_date = 2026-09-01
20260904 → 20260907

This demonstrates that the Silver table is not merely storing the latest subject state.

It is storing:

WHO changed?
    +
WHAT changed?
    +
WHEN did that version become effective?
    +
WHEN did that version stop being effective?

⸻

30. Important Snapshot Semantic

There is one critical architectural assumption in this implementation:

subjects_YYYYMMDD.csv is treated as a complete source snapshot for that snapshot version.

This matters because AUTO CDC FROM SNAPSHOT compares complete source states.

If a key exists in the previous snapshot but disappears from the next snapshot, Databricks treats that absence as a deletion/end of the previous source state.  

For this dataset:

20260825
3359 subjects
       ↓
20260827
55 subjects

Consequently, only the subjects represented by the subsequent complete snapshot remain current according to snapshot semantics.

This explains the final validation:

Distinct historical subjects = 3359
Current subject records       = 55

This behavior would not be correct if the later 55-row files were incremental/delta files rather than complete snapshots. Therefore snapshot completeness is a required source contract for this implementation.

⸻

31. Mapping Summary

Source Attribute	Silver Transformation	Reference Mapping
subject_id	Trim	—
study_id	Trim	—
site_id	Trim	—
subject_status	Trim + uppercase + placeholder→NULL	—
age	Cast to integer	—
sex	Trim + uppercase	silver.ref_sex
baseline_condition_code	Trim + uppercase + placeholder→NULL	Code retained; no diagnosis join in current subjects.py
arm_code	Trim + uppercase + placeholder→NULL	—
screening_date	Cast to date	—
enrollment_date	Cast to date	—
randomization_date	Cast to date	—
informed_consent_date	Cast to date	—
discontinuation_date	Cast to date	—
discontinuation_reason	Trim + uppercase + placeholder→NULL	—
Filename date	Parse YYYYMMDD	source_snapshot_date
subject_id	CDC business key	SCD2
Business attributes	History tracked	SCD2
Operational metadata	Retained but excluded from history triggers	track_history_except_column_list

⸻

32. Engineering Outcome

The final implementation converts a collection of raw periodic EDC files into a temporally consistent Subject history table:

Periodic snapshots
        +
Filename-based versioning
        +
Data standardization
        +
Reference-data normalization
        +
Deterministic lookup mapping
        +
Snapshot CDC
        +
SCD Type 2
        +
Data-quality validation
        =
Auditable Silver Subject History

The implementation also addresses a non-trivial CDC failure caused by reference-table cardinality after normalization. Instead of masking the failure downstream, the pipeline validates the source grain, identifies the many-to-one normalization issue in ref_sex, makes the reference mapping deterministic, and then applies CDC.

That is the part I would emphasize in your project README and in a Data Engineer interview: you didn’t merely implement SCD2—you diagnosed a grain/cardinality violation introduced by a dimension lookup and fixed it at the correct transformation boundary.

For reference, the implementation follows Databricks’ current create_auto_cdc_from_snapshot_flow pattern: a streaming target, snapshot provider returning (DataFrame, version), business keys, SCD mode, and optional history-column exclusions.  

⁠Databricks — AUTO CDC FROM SNAPSHOT reference