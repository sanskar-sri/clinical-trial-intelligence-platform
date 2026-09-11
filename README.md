# Clinical Trial Intelligence Platform

An end-to-end clinical trial data platform built on AWS S3, Databricks, Unity Catalog,
Delta Lake and Lakeflow Declarative Pipelines.

The platform ingests multi-source clinical trial operational data (EDC, CTMS, laboratory,
safety, master/reference), standardises and validates it against clinical business rules,
quarantines records that cannot be trusted, and publishes analytics-ready Gold datasets
and an AI/BI dashboard for study, site, safety and data-quality monitoring.

> **All data in this repository is synthetically generated.** No real patient, subject,
> site or sponsor data is present, and none of the identifiers correspond to real people
> or organisations.

**Status:** Bronze → Silver → Gold implemented, orchestrated as a single Lakeflow Job,
and executing end-to-end. Dashboard built on the Gold layer.

---

## 1. Why this project exists

Clinical trial operations generate data across independent systems — EDC captures subject
and visit activity, CTMS holds study and site operations, central labs return measurements,
safety systems record adverse events, and master data defines sponsors, products and
institutions. These feeds arrive at different frequencies, with different schemas,
different terminology for the same concept, and different data quality.

The engineering problem is not file ingestion. It is producing a governed layer where
study, site, subject and visit data can be reconciled, where invalid records are visible
rather than silently dropped, and where every analytical number can be traced back to the
source file that produced it.

---

## 2. Architecture

```text
  EDC        CTMS       Laboratory     Safety      Master / Reference
   │           │            │             │               │
   └───────────┴────────────┴─────────────┴───────────────┘
                             │
                             ▼
                  AWS S3 — Landing zone
                   (CSV, date-versioned)
                             │
                    Unity Catalog external location
                             │
                             ▼
        ┌────────────────────────────────────────────┐
        │ BRONZE — clinical-trial-bronze             │
        │ Auto Loader streaming + materialised views │
        │ Source preservation, ingestion lineage     │
        └────────────────────┬───────────────────────┘
                             ▼
        ┌────────────────────────────────────────────┐
        │ SILVER — clinical-trial-silver             │
        │ Typing, key normalisation, reference       │
        │ standardisation, referential integrity,    │
        │ clinical DQ rules, CDC / SCD Type 2        │
        └──────────┬──────────────────────┬──────────┘
                   ▼                      ▼
             valid records          failed DQ rules
                   │                      │
                   ▼                      ▼
                SILVER                QUARANTINE
                   │
                   ▼
        ┌────────────────────────────────────────────┐
        │ GOLD — clinical-trial-gold                 │
        │ Subject spine, enrolment, site performance,│
        │ visit compliance, safety, lab monitoring,  │
        │ DQ metrics, reconciliation                 │
        └────────────────────┬───────────────────────┘
                             ▼
              AI/BI dashboard — 5 pages
     Study · Enrolment & Sites · Patient & Visits ·
        Safety & Labs · Data Quality & Risk
```

---

## 3. Technology stack

| Layer | Technology |
|---|---|
| Cloud | AWS |
| Object storage | Amazon S3 |
| Data platform | Databricks (serverless compute) |
| Governance | Unity Catalog |
| Pipelines | Lakeflow Declarative Pipelines |
| Orchestration | Lakeflow Jobs |
| Ingestion | Databricks Auto Loader (`cloudFiles`) |
| Table format | Delta Lake (Unity Catalog managed tables) |
| Processing | Apache Spark / PySpark, SQL |
| CDC | `create_auto_cdc_flow()` — SCD Type 2 |
| BI | Databricks AI/BI dashboards |
| Version control | Git |

---

## 4. Orchestration

Transformation logic and workflow control are deliberately separated. Each medallion layer
is its own Lakeflow pipeline, which resolves dataset dependencies declaratively. A Lakeflow
Job sequences the three pipelines and enforces the cross-layer dependency.

**Job:** `clinical-trial-intelligence-workflow`

```text
          bronze_ingestion            silver_transformation            gold_transformation
       clinical-trial-bronze  ──▶   clinical-trial-silver     ──▶   clinical-trial-gold
              1m 14s                        2m 02s                         1m 12s
                              all-succeeded dependency between tasks
```

Latest full refresh: **4m 30s end-to-end**, all three tasks succeeded, serverless compute
with performance-optimised mode enabled.

A third pipeline was not created just to run Bronze then Silver then Gold. A pipeline
expresses relationships between *datasets*; a job expresses relationships between *tasks*.
Collapsing the two would put orchestration logic inside a transformation graph.

---

## 5. Repository structure

```text
clinical-trial-intelligence-platform/
│
├── data/landing/              synthetic source files, organised by source system
│   ├── ctms/                  sites, studies
│   ├── edc/                   subjects, visits (date-versioned deltas)
│   ├── lab/                   lab_results
│   ├── safety/                adverse_events
│   ├── master/                institutions, products, sponsors
│   └── protocol/              study_arms
│
├── src/
│   ├── setup/                 create_schemas.sql — catalog & schema DDL
│   │
│   ├── pipelines/
│   │   ├── bronze/
│   │   │   ├── streaming/     4 Auto Loader ingestion modules
│   │   │   └── materialized/  13 master / reference modules
│   │   ├── silver/
│   │   │   ├── dimensions.sql dimensions + reference standardisation
│   │   │   ├── subjects.py    AUTO CDC, SCD Type 2
│   │   │   ├── visits.py
│   │   │   ├── lab_results.py
│   │   │   └── adverse_events.py
│   │   └── gold/              9 analytical modules
│   │
│   ├── utils/                 shared transformation contracts
│   │   ├── bronze_common.py
│   │   ├── bronze_materialized_common.py
│   │   └── silver_common.py
│   │
│   ├── notebooks/exploration/ design proof-of-work (see §11)
│   └── validation/            post-run validation, by layer
│
├── dashboard/                 AI/BI dashboard definition (.lvdash.json)
└── README.md
```

Three directories, three different questions:

| Directory | Question it answers |
|---|---|
| `notebooks/exploration/` | What is actually in this data? |
| `pipelines/` | What should be done to it? |
| `validation/` | Did what was built produce the correct result? |

---

## 6. Governance and storage

**Catalog:** `clinical_trial_intelligence`
**Schemas:** `bronze`, `silver`, `quarantine`, `gold`

Landing data and managed analytical storage are separated at the bucket level:

```text
s3://clinical-trial-intelligence-platform-sk/
├── Landing/          externally delivered source files (external location)
└── UnityManaged/     Unity Catalog managed storage root
    ├── bronze/  ├── silver/  ├── quarantine/  └── gold/
```

Databricks reaches S3 through Unity Catalog external locations rather than credentials
embedded in transformation code. Consumers query `clinical_trial_intelligence.bronze.edc_subjects`,
never the underlying `__unitystorage` UUID paths — the table name is the governed interface
and Unity Catalog owns the physical layout.

---

## 7. Bronze layer

Bronze answers one question: *what did the source system send?* No business harmonisation
happens here, because correcting data during ingestion makes it impossible to tell later
whether an anomaly came from the source, the ingestion, or the transformation.

**17 datasets**, ingested through two patterns:

*Streaming (Auto Loader, incremental file discovery):*
`edc_subjects`, `edc_visits`, `lab_results`, `safety_adverse_events`

*Materialised views (small master / reference feeds):*
`ctms_sites`, `ctms_studies`, `master_institutions`, `master_products`, `master_sponsors`,
`protocol_study_arms`, `ref_country_region`, `ref_diagnosis_mapping`, `ref_geography`,
`ref_lab_test`, `ref_severity_mapping`, `ref_sex_mapping`, `ref_unit_mapping`

Every Bronze record carries ingestion lineage — `_source_file`, `_source_file_name`,
`_source_file_modification_ts`, `_ingestion_ts`, `_ingestion_date` — so any Silver or Gold
number can be traced back to the file that produced it.

**Validated Bronze volumes**

| Table | Rows | Source files |
|---|---:|---:|
| `edc_subjects` | 3,829 | 9 |
| `edc_visits` | 18,262 | 10 |
| `lab_results` | 65,006 | 10 |
| `safety_adverse_events` | 1,952 | 10 |
| `ctms_sites` | 45 | 1 |
| `ctms_studies` | 6 | 1 |

---

## 8. Silver layer

Silver answers: *what is the standardised, trustworthy representation?* It produces 13
dimension/reference datasets, 4 validated clinical entities, and 4 matching quarantine
datasets.

### 8.1 Business-key normalisation contract

All primary and foreign business keys pass through one shared contract in
`src/utils/silver_common.py`, so joins have consistent semantics everywhere:

```text
TRIM  →  blank / "-" → NULL  →  UPPER

" sub-001 " → "SUB-001"      "" → NULL      "-" → NULL
```

### 8.2 Deterministic reference resolution

Reference datasets contain case variants that collapse to the same normalised key
(`M`, `m`, `MALE`, `Male`). Joining a source row against an un-deduplicated reference turns
one subject into several rows, which then violates CDC business-key uniqueness — this was a
real pipeline failure during development, and the fix was applied at the reference boundary
rather than by dropping duplicates downstream.

Reference resolution therefore normalises and deduplicates before joining, and uses
deterministic aggregation instead of `FIRST()` over an unordered distributed group.
Conflict checks across all six reference mappings — sex, diagnosis, site→study, lab test,
unit conversion, severity — return **0 conflicting mappings**.

Values with no reference match become `UNMAPPED` rather than `NULL`, so unknown source
values stay visible to data-quality checks.

### 8.3 Dimensions and references

`dim_institution`, `dim_sponsor`, `dim_product`, `dim_site`, `dim_study`, `dim_study_arm`,
`ref_country_region`, `ref_geography`, `ref_diagnosis`, `ref_lab_test`, `ref_severity`,
`ref_sex`, `ref_unit`

`dim_site` resolves region through `city + country` → `ref_geography`, falling back to
country-level `ref_country_region`, and finally to `UNMAPPED` — so unresolved geography
cannot silently vanish from regional rollups.

### 8.4 Subjects — CDC and SCD Type 2

The EDC subject feed is **not** a series of complete snapshots. An initial population file
is followed by incremental change files, which is why the pipeline uses
`create_auto_cdc_flow()` with SCD Type 2 rather than snapshot-comparison CDC.

| Property | Value |
|---|---|
| Source | `bronze.edc_subjects` |
| Business key | `subject_id` |
| Sequence | `struct(source_snapshot_date, _source_file_name)` |
| History | SCD Type 2 (`__START_AT` / `__END_AT`) |
| Target | `silver.subjects` / `quarantine.subjects` |

`source_snapshot_date` is derived from the filename (`subjects_YYYYMMDD.csv`) because
`_ingestion_ts` is identical across files processed in one run, and file modification order
does not match logical snapshot order. The filename is the deterministic tie-breaker.

Ingestion metadata is excluded from history tracking via `track_history_except_column_list`.
A new source file changes `_source_file_name` on every snapshot; if that counted as a change,
the table would accumulate `ENROLLED → ENROLLED → ENROLLED` history for subjects that never
changed. History tracks business change only.

Because the sequencing expression is a compound source-ordering structure, `__START_AT` /
`__END_AT` represent *source change ordering*, not clinical effective dates — so clinical
dates such as `visit_date` or `onset_date` are never compared against SCD boundaries.

### 8.5 Visits, lab results and adverse events

All three were profiled before implementation rather than copied from `subjects.py`.

| Entity | Grain | Key behaviour | Strategy |
|---|---|---|---|
| `visits` | 1 row = 1 subject visit | `visit_id` unique, never reissued across files | append-only streaming, no CDC |
| `lab_results` | 1 row = 1 measurement | `lab_result_id` unique | append-only, no CDC |
| `adverse_events` | 1 row = 1 AE | `ae_id` complete and unique | append-only, no CDC |

**Laboratory standardisation.** Measurements arrive in mixed units and are standardised
through `ref_lab_test` and `ref_unit`:

```text
standardized_result_value = result_value × conversion_factor
```

Both the original and standardised value/unit are retained. Abnormality is *derived* from
the standardised measurement against the standard reference range rather than trusting the
source `abnormal_flag` — the two disagree on 10,373 rows. The source flag is kept for
lineage and exposed through a discrepancy indicator, so the disagreement is auditable
instead of silently overwritten.

**Severity vs seriousness.** `ref_severity` maps 9 source spellings to MILD / MODERATE /
SEVERE. Clinical severity and regulatory seriousness are modelled as separate concepts;
`serious_flag` is normalised to YES/NO independently. A Grade 4 event is not automatically
regulatory-serious.

### 8.6 Referential integrity against Silver, not Bronze

Clinical facts validate against trusted Silver entities — for subjects, the current SCD
version (`__END_AT IS NULL`) — never against raw Bronze. Otherwise an invalid Bronze subject
would legitimise every visit, lab result and adverse event hanging off it.

### 8.7 Blocking rules vs warning flags

Not every clinical oddity should reject a record.

```text
DQ failure        → record is structurally untrustworthy  → quarantine
Clinical warning  → record is unusual but plausible       → retain + flag for review
```

Blocking rules cover missing or unknown business keys, referential integrity failures,
cross-entity mismatches (subject–study, subject–site, visit–subject), out-of-range values,
unmappable reference values, and impossible date sequences (enrolment before screening,
randomisation before enrolment, discontinuation before randomisation).

Warning flags cover severity mapping anomalies, resolution before onset, high-severity
non-serious events, mild serious events, and fatal non-serious events.

Visit-date validation is status-aware: a missing `visit_date` is invalid for a completed
visit but legitimate for `MISSED` or `RESCHEDULED`.

### 8.8 Quarantine

Every clinical pipeline follows the same shape: validated temp view → valid stream +
quarantine table. Quarantine records preserve the original business attributes, source
lineage, a `_dq_failures` array holding **every** failed rule rather than only the first,
human-readable reasons, and a quarantine timestamp.

**Silver outcome**

| Entity | Valid | Quarantined | Notes |
|---|---:|---:|---|
| `subjects` | 3,640 | 186 | current SCD version |
| `visits` | 17,942 | 320 | 218 missing `subject_id`, 102 missing `visit_date`, no overlap |
| `lab_results` | 62,401 | 2,605 | 389 rows failed more than one rule |
| `adverse_events` | **TBD** | **TBD** | fill from `gold_reconciliation` |

---

## 9. Gold layer

Gold answers: *what does the business need?* Eleven datasets built only from trusted Silver
data, modelled around how clinical operations teams actually monitor a trial.

| Dataset | Question it answers |
|---|---|
| `gold_subject_spine` | One analytical row per subject, the join backbone for everything else |
| `gold_subject_summary` | Subject-level roll-up of visits, labs and safety activity |
| `gold_subject_disposition` | Where subjects stand: screened, enrolled, randomised, discontinued, completed |
| `gold_discontinuation_reasons` | Why subjects leave, by study and site |
| `gold_enrollment_timeseries` | Enrolment curve over time — actual vs expected pace |
| `gold_site_performance` | Site-level enrolment, retention, data quality and activity |
| `gold_visit_compliance` | Protocol adherence: completed, missed, out-of-window visits |
| `gold_safety_metrics` | AE counts and rates by study, site, severity and seriousness |
| `gold_lab_monitoring` | Abnormal laboratory findings and source-vs-derived flag discrepancies |
| `gold_data_quality_metrics` | DQ rule trigger rates by entity and rule |
| `gold_reconciliation` | Bronze → Silver → quarantine record balance per entity |

### Deliberate modelling exclusions

The platform does not derive variables the source data cannot support:

- **TEAE** requires a defensible first-dose timestamp. A randomisation date is not first dose.
- **SAFFL** should not be read as "received treatment" without exposure data.
- **Exposure-adjusted AE rates** need a genuine exposure or follow-up denominator.

Where CDISC concepts inform the model, datasets are described as *SDTM-inspired* or
*ADaM-inspired* — not as regulatory-compliant. In place of TEAE, the model uses
`post_randomization_ae_flag`, which the available data does support.

Stating these gaps is the point. Deriving a plausible-looking TEAE flag from a randomisation
date would be the actual error.

---

## 10. Dashboard

`dashboard/Clinical Trial Intelligence & Risk Monitoring.lvdash.json` — a five-page Databricks
AI/BI dashboard over the Gold layer with global study/site/date filters:

1. **Study Overview** — portfolio-level trial status
2. **Enrolment & Site Performance** — recruitment pace and site comparison
3. **Patient & Visit Monitoring** — subject disposition and protocol compliance
4. **Safety & Lab Monitoring** — AE signals and abnormal laboratory findings
5. **Data Quality & Operational Risk** — DQ rule trigger rates and reconciliation

---

## 11. Design proof-of-work

Each clinical entity was investigated before a line of pipeline code was written. The
exploration notebooks are written as **Question → Purpose → Query → Result → Design
conclusion**, so the reasoning behind each design decision is reviewable rather than implied.

| Notebook | Sections | What it established |
|---|---:|---|
| `bronze_ingestion_exploration` | — | Source file inventory, delivery pattern, schema behaviour |
| `subject_exploration` | 15 | Feed is delta files not snapshots; sequencing field selection; SCD2 justification |
| `visits_exploration` | 7 | `visit_id` never reissued → append-only, no CDC needed |
| `lab_results_exploration` | 13 | Unit conversion model; derived vs source abnormality (10,373 disagreements) |
| `adverse_events_exploration` | 8 | `ae_id` uniqueness; severity spelling variants; severity vs seriousness |

These notebooks are the most useful thing in the repository for understanding *why* the
pipelines look the way they do.

---

## 12. Validation

Validation is written independently of transformation logic and lives in `src/validation/`,
organised by layer. It covers Bronze→Silver reconciliation, business-key uniqueness, SCD
current-record uniqueness (exactly one `__END_AT IS NULL` row per subject), cross-entity
referential integrity, reference mapping conflicts, quarantine counts, DQ reason
distribution, NULL behaviour, and source-vs-standardised measurement behaviour.

Counts published in this README come from validation SQL, not from UI-rounded pipeline
output metrics.

---

## 13. Scope and limitations

- **All data is synthetic.** Volumes and distributions are realistic; the records are not real.
- **Reconciliation gap.** `gold_reconciliation` balances Bronze against Silver + quarantine
  for every entity except subjects: 3,640 current + 186 quarantined vs 3,829 Bronze rows
  leaves 3 records unaccounted for. Under investigation; the reconciliation dataset exposes
  it rather than hiding it.
- **No delete semantics.** The EDC subject feed carries no delete indicator, so hard-delete
  handling is not implemented.
- **Append-only assumption.** Visits, labs and adverse events are treated as append-only
  based on the observed files. This contract should be re-validated as new files arrive.
- **Single environment.** No dev/staging/prod separation or CI/CD gating yet.

---

## 14. Roadmap

- CI/CD with Databricks Asset Bundles and environment separation
- Job-level alerting and DQ threshold breach notifications
- Protocol document intelligence: PDF → embeddings → RAG for protocol-aware analysis
- Genie natural-language querying over the Gold layer

---

## 15. Running it

Reproducing this requires a Databricks workspace with Unity Catalog and an AWS S3 bucket.

1. Create the S3 bucket and upload `data/landing/` to the `Landing/` prefix.
2. Create Unity Catalog external locations for `Landing/` and `UnityManaged/`; grant
   `CREATE MANAGED STORAGE` on the managed location.
3. Run `src/setup/create_schemas.sql` to create the catalog and the four schemas.
4. Create three Lakeflow pipelines — `clinical-trial-bronze`, `clinical-trial-silver`,
   `clinical-trial-gold` — pointing at `src/pipelines/bronze`, `silver` and `gold`.
5. Create a Lakeflow Job with three pipeline tasks chained on all-succeeded dependencies.
6. Import `dashboard/*.lvdash.json`.

No credentials are stored in this repository. All S3 access is brokered through Unity Catalog.

---

## 16. Licence

MIT — see [LICENSE](LICENSE).
