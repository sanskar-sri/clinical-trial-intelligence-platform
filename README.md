# Clinical Trial Intelligence Platform

A cloud-based data engineering platform for ingesting, processing, governing,
and analyzing clinical-trial data from multiple operational source systems.

The platform uses AWS S3 as the cloud data lake and Databricks as the
data-processing, governance, and analytics platform.

---

# Source Systems

The platform simulates data arriving from multiple clinical-trial operational
systems.

| Source | Description | Example Data |
|---|---|---|
| EDC | Electronic Data Capture | Patient/subject and visit data |
| CTMS | Clinical Trial Management System | Study and site-management data |
| LAB | Laboratory System | Laboratory test results |
| SAFETY | Safety / Pharmacovigilance System | Adverse-event data |

The S3 Landing layer is organized by source system and dataset so that
individual datasets can be ingested independently.

Example:

```text
Landing/
├── EDC/
│   ├── subjects/
│   └── visits/
├── CTMS/
├── LAB/
├── SAFETY/
├── master/
└── protocol/
```

---

# Target Platform Architecture

```text
Clinical Trial Source Systems
       │
       ├── EDC
       ├── CTMS
       ├── LAB
       └── SAFETY
       │
       ▼
Amazon S3
       │
       ▼
Landing Layer
       │
       ▼
Databricks Auto Loader
       │
       ▼
Bronze Delta
       │
       ▼
Silver Delta
       │
       ▼
Gold Delta
       │
       ▼
Analytics / Dashboard / Serving
```

The platform follows a medallion-style architecture.

```text
Landing → Bronze → Silver → Gold
```

The Landing layer preserves source files.

Bronze will provide the first persistent Delta representation of the source
data with minimal transformation.

Silver will contain cleaned, validated, standardized, and integrated data.

Gold will contain analytics-ready business datasets and metrics.

---

# AWS S3 Data Lake

An Amazon S3 bucket was created as the cloud object-storage layer for the
platform.

```text
Bucket:
clinical-trial-intelligence-platform-sk
```

The raw ingestion area is:

```text
s3://clinical-trial-intelligence-platform-sk/Landing/
```

Source data is organized into domain-specific prefixes beneath the Landing
layer.

Example validated dataset:

```text
s3://clinical-trial-intelligence-platform-sk/Landing/EDC/subjects/
```

Multiple date/source files can exist under a dataset path, allowing the
ingestion pipeline to process new files incrementally.

---

# AWS S3 – Databricks Unity Catalog Integration

Secure access between Databricks and Amazon S3 is implemented using AWS IAM
roles and Databricks Unity Catalog Storage Credentials.

Permanent AWS access keys and secret keys are not embedded in notebooks or
application code.

## Resources Created

```text
AWS S3 Bucket
└── clinical-trial-intelligence-platform-sk

AWS IAM Role
└── databricks-clinical-trial-s3-role

AWS IAM Policy
└── databricks-clinical-trial-s3-policy

Databricks Storage Credential
└── clinical_trial_s3_credential

Databricks External Location
└── clinical_trial_s3

Unity Catalog
└── clinical_trial_intelligence
```

---

# Authentication Architecture

```text
Databricks
     │
     │ Unity Catalog Storage Credential
     ▼
AWS STS AssumeRole
     │
     │ External ID validation
     ▼
AWS IAM Role
     │
     │ IAM Permission Policy
     ▼
Amazon S3
```

The IAM trust relationship authorizes the Databricks Unity Catalog AWS
principal to assume the project IAM role using AWS STS.

A Databricks-generated External ID is included in the trust relationship to
restrict role assumption to the intended Unity Catalog credential.

The IAM permissions policy determines which AWS resources and operations are
available after the role has been assumed.

---

# IAM Security Model

An important distinction in the integration is:

```text
IAM Trust Policy
       │
       └── Who can assume the role?

IAM Permission Policy
       │
       └── What can the role access?
```

For this architecture:

```text
Databricks
     │
     │ STS AssumeRole
     ▼
IAM Trust Policy
     │
     │ validates principal + External ID
     ▼
IAM Role
     │
     │ IAM Permission Policy
     ▼
S3 / SNS / SQS
```

Authentication and authorization therefore represent separate layers of the
integration.

Successful `AssumeRole` does not automatically prove that the assumed role
has permission to access the required S3 objects.

---

# Databricks Storage Credential

The following Unity Catalog Storage Credential was created:

```text
Storage Credential:
clinical_trial_s3_credential

Credential Type:
AWS IAM Role
```

The credential references the AWS IAM role used by Databricks.

Conceptually:

```text
AWS IAM Role
       +
Databricks External ID
       │
       ▼
Unity Catalog Storage Credential
```

The Storage Credential separates cloud authentication configuration from
individual notebooks and workloads.

---

# Unity Catalog External Location

The Storage Credential was associated with the S3 Landing layer through a
Unity Catalog External Location.

```text
External Location:
clinical_trial_s3

S3 Path:
s3://clinical-trial-intelligence-platform-sk/Landing/

Storage Credential:
clinical_trial_s3_credential
```

Architecture:

```text
Storage Credential
       │
       ▼
External Location
       │
       ▼
s3://clinical-trial-intelligence-platform-sk/Landing/
```

This allows access to the S3 path to be governed through Unity Catalog.

---

# Storage Credential Validation

During initial configuration, Storage Credential validation returned:

```text
Assume Role             PASS
External ID Condition   PASS
Self Assume Role        FAIL
```

The IAM role could be assumed by Databricks, but the role-assumption
configuration did not satisfy the self-assume requirement.

The IAM trust/permission configuration was corrected while retaining the
External ID security condition.

After correction:

```text
Assume Role             PASS
Self Assume Role        PASS
External ID Condition   PASS
```

---

# Troubleshooting — External Location File Events

## Problem

After basic S3 connectivity was established, Databricks failed while validating
the resources required for automatic file events.

Normal storage operations were working, but file-event resource provisioning
and teardown failed.

The AWS error exposed missing authorization associated with the S3 bucket
notification configuration, including:

```text
s3:GetBucketNotification
```

## Investigation

The validation results were analyzed individually rather than treating the
integration as a single pass/fail operation.

The working and failing components were separated as follows:

```text
Databricks → IAM Role             WORKING
IAM Role → S3                     WORKING
External ID Validation            WORKING
Read / Write / List / Delete      WORKING
File Event Configuration          FAILING
```

This demonstrated that the problem was isolated to the event-infrastructure
permissions rather than basic S3 connectivity.

## Resolution

The IAM permissions associated with the Databricks storage role were extended
with the required permissions for Databricks-managed file-event infrastructure.

The required configuration involved permissions associated with:

```text
Amazon S3
Amazon SNS
Amazon SQS
```

After correcting the permissions, Databricks successfully validated the
file-event operations.

```text
File Events Resource Provision    PASS
Read File Event Queue             PASS
Purge File Event Queue            PASS
File Events Resource Teardown     PASS
```

---

# Why "Force Create" Was Not Used

Databricks provided an option to force creation of the external location even
when the file-event validation failed.

The issue was investigated instead of bypassing the failed validation because
the target ingestion architecture is intended to support scalable incremental
file discovery.

This also avoided hiding an underlying IAM configuration problem.

---

# Troubleshooting — S3 Read Permission

After the file-event configuration was corrected, another validation exposed
an S3 read-access issue.

The important observation was that role assumption itself was already working.

Conceptually:

```text
Databricks
     │
     │ AssumeRole
     ▼
IAM Role                    PASS
     │
     │ GetObject / ListBucket
     ▼
S3 Data                     FAIL
```

This demonstrated the difference between authentication and authorization.

The S3 data-access permissions and target path were reviewed and corrected for
the intended Landing prefix.

Final path:

```text
s3://clinical-trial-intelligence-platform-sk/Landing/
```

---

# Final External Location Validation

After correcting the IAM trust relationship, data permissions, and file-event
permissions, the external-location validation completed successfully.

```text
Read                         PASS
List                         PASS
Write                        PASS
Delete                       PASS
Path Exists                  PASS
File Events Read             PASS
Assume Role                  PASS
Self Assume Role             PASS
External ID Condition        PASS
```

Result:

```text
ALL REQUIRED PERMISSIONS CONFIRMED
```

---

# Unity Catalog

A dedicated Unity Catalog catalog was created for the project.

```text
clinical_trial_intelligence
```

Its purpose is to govern the platform's clinical-trial datasets across the
processing layers.

Current catalog structure:

```text
clinical_trial_intelligence
│
├── default
└── information_schema
```

The planned processing schemas are:

```text
clinical_trial_intelligence
│
├── bronze
├── silver
└── gold
```

The Bronze schema is the next implementation milestone.

---

# Raw S3 Data Access Validation

Before implementing production ingestion, direct access from Databricks to the
S3 Landing layer was tested.

Validated source:

```text
s3://clinical-trial-intelligence-platform-sk/Landing/EDC/subjects/
```

A PySpark DataFrame was created using:

```python
source_path = "s3://clinical-trial-intelligence-platform-sk/Landing/EDC/subjects/"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

display(df.limit(10))
```

The DataFrame was successfully created and sample records were displayed.

This confirmed that:

- Databricks can access the S3 Landing layer.
- The Unity Catalog External Location is functioning.
- The AWS IAM role provides the required S3 read access.
- Files under `Landing/EDC/subjects/` can be discovered.
- Spark can parse the source CSV data.
- The source is accessible for the Bronze ingestion implementation.

This test is only a connectivity and source-read validation.

It is **not** the production ingestion mechanism.

The target production flow is:

```text
S3 Landing
     │
     ▼
Auto Loader
     │
     ▼
Bronze Delta
     │
     ▼
Silver Delta
     │
     ▼
Gold Delta
```

---

# GitHub – Databricks Development Integration

Before starting the Bronze ingestion implementation, the GitHub repository was
connected to the Databricks workspace using a Databricks Git folder.

GitHub repository:

```text
clinical-trial-intelligence-platform
```

Development architecture:

```text
VS Code / Local Development
        │
        ▼
GitHub Repository
        │
        ├── main
        └── test
             │
             ▼
     Databricks Git Folder
             │
             ▼
     Databricks Development
```

The Databricks Git folder was created under the user workspace and connected
to the existing GitHub repository.

The `test` branch is currently used for development and validation.

This allows local development and Databricks development to remain associated
with the same Git repository.

The repository acts as the source-control layer for pipeline code.

Current development workflow:

```text
Local / Databricks Development
             │
             ▼
          test
             │
             ▼
         Git Commit
             │
             ▼
          GitHub
             │
             ▼
       Pull Request
             │
             ▼
           main
```

---

# Bronze Layer — Planned Design

The next implementation milestone is the Bronze layer.

Bronze will provide the first persistent Delta representation of source-system
data.

The initial implementation will use one dataset before generalizing the
pattern.

```text
Source System:
EDC

Dataset:
subjects

Source:
s3://clinical-trial-intelligence-platform-sk/Landing/EDC/subjects/

Target:
clinical_trial_intelligence.bronze.edc_subjects
```

Planned flow:

```text
Landing/EDC/subjects/
        │
        │ CSV source files
        ▼
Databricks Auto Loader
        │
        │ Incremental ingestion
        ▼
clinical_trial_intelligence
        │
        └── bronze
              │
              └── edc_subjects
                     │
                     ▼
                 Delta Table
```

Bronze will preserve source information with minimal transformation.

Additional ingestion metadata will be introduced for lineage and
observability, including fields such as:

```text
source file
ingestion timestamp
ingestion date
```

The first implementation will be validated using `EDC/subjects` before the
pattern is generalized to additional clinical-trial datasets.

---

# Planned Bronze Implementation Sequence

The Bronze layer will be implemented incrementally.

1. Create the `bronze` schema in Unity Catalog.
2. Implement Auto Loader for `EDC/subjects`.
3. Add ingestion metadata.
4. Persist the ingested data as a Delta table.
5. Validate the initial ingestion.
6. Add a new source file to the S3 Landing path.
7. Validate incremental ingestion.
8. Confirm that already processed files are not unnecessarily reprocessed.
9. Test schema evolution.
10. Generalize the ingestion pattern for additional source datasets.
11. Introduce automated data-quality and pipeline tests.
12. Integrate the stable pipeline with the project's deployment architecture.

---

# Repository Structure

Current repository:

```text
clinical-trial-intelligence-platform/
│
├── README.md
│
└── data/
```

The repository will evolve incrementally rather than creating unused
directories in advance.

The next expected structure is:

```text
clinical-trial-intelligence-platform/
│
├── README.md
│
├── data/
│
└── src/
    └── bronze/
        └── edc_subjects.py
```

As additional layers are implemented, the target structure will evolve toward:

```text
clinical-trial-intelligence-platform/
│
├── README.md
│
├── data/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── tests/
│
├── resources/
│
└── .github/
    └── workflows/
```

CI/CD configuration will be introduced after the core pipeline has been
implemented and validated.

---

# Planned CI/CD Architecture

GitHub will act as the source of truth for pipeline and deployment code.

Development workflow:

```text
Developer
    │
    ▼
test / feature branch
    │
    ▼
GitHub
    │
    ▼
Pull Request
    │
    ▼
main
```

Target deployment workflow:

```text
main
    │
    ▼
GitHub Actions
    │
    ▼
Databricks Declarative Automation Bundles
    │
    ▼
Databricks Jobs / Lakeflow Pipelines
```

GitHub Actions and Databricks Declarative Automation Bundles are planned for a
later phase to automate validation and deployment of Databricks resources.

They have **not yet been implemented**.

---

# Current Implementation Status

## Phase 1 — AWS + Databricks Foundation ✅

```text
S3 Data Lake                               ✅
Landing Zone                               ✅
Source-system folder structure             ✅
AWS IAM Role                               ✅
IAM Permission Policy                      ✅
IAM Trust Relationship                     ✅
STS AssumeRole                             ✅
External ID                                ✅
Storage Credential                         ✅
Storage Credential Validation              ✅
External Location                          ✅
External Location Validation               ✅
File Events                                ✅
Unity Catalog                              ✅
S3 → Databricks read validation            ✅
GitHub Repository                          ✅
Databricks Git Folder                      ✅
GitHub ↔ Databricks integration            ✅
```

## Phase 2 — Bronze Layer 🚧

```text
Bronze architecture                        PLANNED
Bronze Unity Catalog schema                NEXT
EDC Subjects Auto Loader                   PENDING
Bronze Delta table                         PENDING
Ingestion metadata                         PENDING
Initial ingestion validation               PENDING
Incremental ingestion validation           PENDING
Schema evolution testing                   PENDING
Generalized Bronze ingestion               PENDING
```

## Future Phases

```text
Silver Layer                               PENDING
Data Quality                               PENDING
Gold Layer                                 PENDING
Lakeflow Pipeline                          PENDING
Automated Testing                          PENDING
GitHub Actions CI/CD                       PENDING
Declarative Automation Bundles             PENDING
Dashboard / Serving                        PENDING
```

---

# Engineering Lessons

Several important engineering concepts were demonstrated during the platform
foundation work.

### Authentication != Authorization

```text
AssumeRole succeeds
        ≠
S3 access automatically succeeds
```

Authentication determines whether Databricks can assume the AWS role.

Authorization determines what that assumed role is allowed to do.

### Storage Access != Event Infrastructure

```text
S3 Read/Write
      ≠
S3/SNS/SQS file-event permissions
```

Successful object access does not prove that the role can provision or use
event-driven ingestion infrastructure.

### Validate Components Independently

Instead of treating an integration as simply:

```text
WORKING / NOT WORKING
```

the architecture was decomposed into:

```text
Identity
   ↓
STS Role Assumption
   ↓
External ID Validation
   ↓
IAM Authorization
   ↓
S3 Storage Access
   ↓
File Event Infrastructure
   ↓
Databricks Processing
```

This made it possible to isolate failures without unnecessarily modifying
components that were already functioning.

---

# Interview Story — Databricks S3 Integration Failure

## Situation

While building the Clinical Trial Intelligence Platform using AWS S3 and
Databricks, the Unity Catalog Storage Credential and External Location
encountered multiple validation failures involving role assumption, S3
permissions, and automatic file events.

## Task

The objective was to establish secure and governed connectivity between
Databricks and the S3 Landing layer without embedding long-lived AWS
credentials or bypassing failed validation.

## Action

The integration was debugged layer by layer.

The IAM trust relationship and External ID were first validated.

When the Storage Credential reported a Self Assume Role failure, the
role-assumption configuration was corrected.

A subsequent external-location validation exposed missing authorization for
managed file events. The failure was isolated to the event infrastructure and
the required S3/SNS/SQS permissions were configured.

Finally, an S3 read-access failure was investigated separately from role
assumption. The S3 data permissions and target Landing prefix were reviewed
and corrected.

Rather than granting unrestricted permissions or bypassing validation using
Force Create, each failing component was isolated and corrected independently.

## Result

The final Unity Catalog External Location passed the required storage,
role-assumption, External ID, and file-event validation checks.

The S3 Landing layer is now accessible from Databricks through Unity Catalog
and is ready for the Bronze ingestion implementation.

## Leadership Principles Demonstrated

Primary:

- Dive Deep
- Ownership

Supporting:

- Learn and Be Curious
- Insist on the Highest Standards

---

# Troubleshooting Evidence

Screenshots and troubleshooting evidence can be maintained separately from the
main README.

Planned structure:

```text
docs/
└── troubleshooting/
    ├── databricks-storage-credential-success.png
    ├── external-location-file-events-failure.png
    └── external-location-file-events-success.png
```

This keeps the README focused while retaining implementation evidence and
debugging history.

---

# Current Checkpoint

The cloud and governance foundation is complete.

```text
AWS S3
   │
   ▼
IAM / STS / External ID
   │
   ▼
Unity Catalog Storage Credential
   │
   ▼
External Location
   │
   ▼
S3 Landing Access
   │
   ▼
GitHub ↔ Databricks Development
   │
   ▼
──────────────────────────────
   │
   ▼
Bronze Schema             ← NEXT
   │
   ▼
Auto Loader
   │
   ▼
Bronze Delta
   │
   ▼
Incremental Ingestion Test
   │
   ▼
Schema Evolution Test
   │
   ▼
Silver
   │
   ▼
Gold
```

The next implementation milestone is the creation of the Bronze schema and the
first incremental ingestion pipeline for `EDC/subjects`.