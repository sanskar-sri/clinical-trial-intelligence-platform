# clinical-trial-intelligence-platform

EDC     → Electronic Data Capture
          Patient/subject + visit data

CTMS    → Clinical Trial Management System
          Study + site management data

LAB     → Laboratory System
          Lab test results

SAFETY  → Safety / Pharmacovigilance System
          Adverse-event data


README Notes

You can put this section directly into your project README.

AWS S3 – Databricks Unity Catalog Integration

The project uses Amazon S3 as the cloud object-storage layer and Databricks as the data-processing and analytics platform. Access between Databricks and S3 is implemented through AWS IAM roles and Unity Catalog Storage Credentials instead of static AWS access keys.

Resources created

S3 Bucket
└── clinical-trial-intelligence-platform-sk
IAM Role
└── databricks-clinical-trial-s3-role
IAM Policy
└── databricks-clinical-trial-s3-policy
Databricks Storage Credential
└── clinical_trial_s3_credential

Authentication flow

Databricks
     │
     │ Unity Catalog Storage Credential
     ↓
AWS STS AssumeRole
     │
     ↓
AWS IAM Role
     │
     │ S3 IAM Permissions
     ↓
Amazon S3

The IAM trust relationship authorizes the Databricks Unity Catalog principal to assume the project IAM role using AWS STS. A Databricks-generated External ID is included in the trust relationship to restrict role assumption to the intended Unity Catalog credential.

The IAM permission policy grants the role access only to the required S3 resources. No permanent AWS access key or secret access key is stored in Databricks notebooks or source code.

Validation status

Assume Role            PASS
External ID Condition  PASS
Self Assume Role       FAIL

The initial validation confirmed that cross-account role assumption and External ID verification were configured successfully. Validation identified one remaining configuration issue: the IAM role must explicitly support self-assumption. The trust policy is being updated to include the IAM role itself as a trusted principal before final credential validation.

⸻

Important interview learning from this step

The strongest thing to remember isn’t the console clicks. It’s this:

IAM Permission Policy = what the role can access.
IAM Trust Policy = who can assume the role.

And in your particular architecture:

S3 Policy
   ↓
"What can Databricks do?"
List/Get/Put S3 objects
Trust Policy
   ↓
"Who can become this role?"
Databricks Unity Catalog + role itself
External ID
   ↓
"Is this the intended Databricks credential?"

That is a much stronger explanation in a Data Engineer interview than saying, “I connected Databricks with S3.”

Your current error is also worth retaining in the README as a Troubleshooting / Engineering Challenges entry rather than hiding it. It demonstrates that you validated the security configuration, isolated the failing IAM mechanism, and corrected the trust relationship rather than simply granting broader permissions.  

Next: fix only the Self Assume Role check. Once that becomes green, we’ll document the final successful state and move to External Location → S3 path → Bronze ingestion.


## Troubleshooting: Databricks External Location File Events Failure

### Problem

While configuring an AWS S3 External Location in Databricks Unity Catalog, 
the storage credential and basic S3 connectivity were successfully validated.

The following checks passed:

- Read: Success
- List: Success
- Write: Success
- Delete: Success
- Path Exists: Success
- Assume Role: Success
- Self Assume Role: Success
- External ID Condition: Success

However, two validations failed:

- File Events Resource Provision
- File Events Resource Teardown

Databricks returned an AccessDenied / 403 error indicating that the IAM role
was not authorized to perform:

s3:GetBucketNotification

### Root Cause

The IAM role had sufficient permissions for normal S3 data operations such as
reading, writing, listing, and deleting objects.

However, Databricks Automatic File Events requires additional AWS permissions
to inspect/configure S3 event notifications and provision the supporting
event infrastructure.

Therefore:

S3 access was working correctly.

The failure was specifically related to the event-driven ingestion capability,
not the basic Databricks-to-S3 connection.

### Investigation

Instead of assuming that the entire S3 integration had failed, I examined each
validation result independently.

This showed that:

Databricks -> IAM Role        = Working
IAM Role -> S3               = Working
External ID validation       = Working
Read/Write/Delete/List       = Working
File Event configuration     = Failing

The error message identified the missing permission:

s3:GetBucketNotification

This isolated the problem to IAM permissions required for file-event
configuration.

### Resolution

The IAM policy attached to the Databricks storage role needed to be extended
with the permissions required for Databricks file events.

The existing S3 permissions were retained because they were already working.

The additional permissions enable Databricks to work with the S3 notification
configuration and the AWS resources used for file-event processing.

### Why I Did Not Immediately Use "Force Create"

Databricks provided a "Force create" option because file events are not
mandatory for basic external-location access.

However, forcing creation would cause Databricks to fall back to directory
listing for file discovery.

For this project, I chose to investigate the IAM failure because the target
architecture is intended to support scalable and event-driven ingestion.

### Architecture Lesson

Basic storage access and event-driven ingestion require different permission
sets.

Basic access:

Databricks
    |
    v
IAM Role
    |
    v
S3
    |
    +-- List
    +-- Read
    +-- Write
    +-- Delete

Event-driven ingestion:

S3
 |
 +-- File arrival
 |
 v
AWS event infrastructure
 |
 v
Databricks / Auto Loader

A successful S3 read/write test therefore does not automatically mean that
file-event functionality is correctly configured.


## Amazon Leadership Principle Story — Databricks S3 Integration Failure

### Leadership Principles

Primary:
- Dive Deep
- Ownership

Supporting:
- Learn and Be Curious
- Insist on the Highest Standards

### Situation

While building a clinical-trial data platform using AWS S3 and Databricks,
I configured a Unity Catalog storage credential and external location.

The IAM role assumption, External ID validation, and S3 read/write/list/delete
operations were successful, but Databricks failed while provisioning automatic
file-event resources.

### Task

I needed to determine whether the problem was with the overall S3 integration
or with a specific component of the architecture, while avoiding unnecessary
changes to permissions that were already working.

### Action

I analyzed the Databricks validation results individually instead of treating
the validation as a single pass/fail test.

I confirmed that:

1. Databricks could assume the AWS IAM role.
2. Self-assume configuration was working.
3. The External ID condition was correct.
4. Databricks could read, list, write, and delete S3 objects.
5. Only File Events Resource Provision and Teardown were failing.

I then examined the AWS error and identified the missing
`s3:GetBucketNotification` capability.

This demonstrated that the problem was isolated to permissions required for
file-event configuration rather than the S3 connection itself.

Instead of bypassing the issue using "Force create", I decided to correct the
IAM configuration so that the architecture could support event-driven file
discovery.

### Result

The troubleshooting process isolated the failure without disrupting the
working S3 integration and identified the exact IAM capability that needed to
be corrected.

More importantly, I learned to separate:

Authentication
        ↓
Authorization
        ↓
Storage access
        ↓
Event infrastructure

when debugging cross-cloud-service integrations.

Most of the validation checks were green, so I did not treat it as a generic S3 connectivity failure. I decomposed the integration layer by layer, identified that only the event-provisioning path was failing, traced the AWS 403 to a specific IAM permission, and chose to fix the underlying configuration rather than bypassing it with Force Create.

This makes the architecture easier to troubleshoot and supports a more
production-oriented ingestion design.

docs/
└── troubleshooting/
    ├── databricks-storage-credential-success.png
    ├── external-location-file-events-failure.png
    └── external-location-file-events-success.png



    Absolutely. Since you want this for your GitHub README and interview preparation, I would document both the successful implementation and the failures you debugged. You can paste the following directly into your README.

AWS S3 Integration with Databricks Unity Catalog

Objective

Configured secure access between AWS S3 and Databricks Unity Catalog using an AWS IAM role and a Databricks Storage Credential. The objective was to allow Databricks to securely read and write clinical-trial data stored in the S3 landing layer without storing long-lived AWS access keys inside Databricks.

Architecture

                    AWS
                     │
                     ▼
        ┌─────────────────────────┐
        │       S3 Bucket         │
        │ clinical-trial-         │
        │ intelligence-platform-sk│
        └────────────┬────────────┘
                     │
                  Landing/
                     │
                     ▼
        ┌─────────────────────────┐
        │      AWS IAM Role       │
        │ databricks-clinical-    │
        │ trial-s3-role           │
        └────────────┬────────────┘
                     │
          STS AssumeRole
          + External ID
                     │
                     ▼
        ┌─────────────────────────┐
        │ Databricks Storage      │
        │ Credential              │
        │ clinical_trial_s3_      │
        │ credential              │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │ Unity Catalog External  │
        │ Location                │
        │ clinical_trial_s3       │
        └────────────┬────────────┘
                     │
                     ▼
s3://clinical-trial-intelligence-platform-sk/Landing/

Implementation

An AWS IAM role named databricks-clinical-trial-s3-role was created specifically for Databricks access.

The IAM trust relationship was configured so that the Databricks Unity Catalog AWS principal could assume the role using AWS STS. An External ID condition was included in the trust policy to restrict role assumption to the intended Databricks storage credential.

The IAM permissions were scoped to the project S3 bucket and included the S3 operations required for data access:

s3:GetObject
s3:PutObject
s3:DeleteObject
s3:ListBucket
s3:GetBucketLocation
s3:ListBucketMultipartUploads
s3:ListMultipartUploadParts
s3:AbortMultipartUpload

Additional S3/SNS/SQS permissions were configured to support Databricks-managed file events.

In Databricks, the following Storage Credential was created:

Storage Credential:
clinical_trial_s3_credential
Authentication:
AWS IAM Role

The Storage Credential was then mapped to the S3 landing path through a Unity Catalog External Location:

External Location:
clinical_trial_s3
S3 Path:
s3://clinical-trial-intelligence-platform-sk/Landing/
Storage Credential:
clinical_trial_s3_credential

This follows the Databricks Unity Catalog model in which a storage credential encapsulates cloud authentication while an external location associates that credential with a cloud-storage path.  ⁠Databricks — Connect to S3 with Unity Catalog

Troubleshooting and Engineering Decisions

The integration did not work successfully on the first attempt. Several IAM and storage-access issues were identified and resolved during implementation.

Issue 1 — Self Assume Role Failure

Initial Storage Credential validation returned:

Success - Assume Role
Failed  - Self Assume Role
Success - External ID Condition

The IAM role could be assumed by Databricks, but the configuration did not satisfy the self-assume requirement.

The IAM trust/permission configuration was updated to allow the required role-assumption behavior while retaining the Databricks-generated External ID condition.

After the change:

Success - Assume Role
Success - Self Assume Role
Success - External ID Condition

Issue 2 — File Events Permission Failure

During External Location creation, Databricks initially reported failures while provisioning managed file-event resources.

The error exposed missing authorization around S3 bucket notification configuration.

Instead of bypassing the validation using Force Create, the IAM permissions were investigated and extended with the required S3, SNS, and SQS permissions for Databricks-managed file events.

After updating the IAM policy, validation confirmed successful:

File Events Resource Provision  ✅
Read File Event Queue           ✅
Purge File Event Queue          ✅
File Events Resource Teardown   ✅

Issue 3 — S3 Read Permission Denied

After resolving the file-event permissions, the External Location validation still returned:

Failed - Read

This demonstrated that successful role assumption does not automatically imply successful access to the underlying S3 data.

The S3 data-access permissions and target path were reviewed. The External Location was ultimately mapped to the intended landing prefix:

s3://clinical-trial-intelligence-platform-sk/Landing/

and the IAM role was configured with the corresponding bucket and object-level permissions.

Final Validation

The final Databricks Test Connection completed successfully.

Read                   ✅
List                   ✅
Write                  ✅
Delete                 ✅
Path Exists            ✅
File Events Read       ✅
Assume Role            ✅
Self Assume Role       ✅
External ID Condition  ✅

Result: All Permissions Confirmed

The Databricks Storage Credential therefore has the permissions required to access the configured S3 External Location.

⸻

STAR Interview Story

Situation: While integrating AWS S3 with Databricks Unity Catalog for a clinical-trial data platform, the Storage Credential and External Location repeatedly failed validation because of IAM trust, self-assumption, file-event, and S3 access issues.

Task: Establish secure, governed read/write connectivity between Databricks and the project’s S3 landing layer without using long-lived AWS access keys or bypassing failed validation.

Action: I created a dedicated IAM role, configured STS role assumption using the Databricks principal and External ID, and granted bucket/object-level S3 permissions. When validation reported a Self Assume Role failure, I corrected the role-assumption configuration. A subsequent failure showed missing permissions for managed file events, so I added the required S3 notification, SNS, and SQS permissions. Finally, I diagnosed the remaining S3 Read failure by validating both the IAM data permissions and the exact S3 prefix used by the External Location rather than using Force Create to bypass the checks.

Result: The final Unity Catalog External Location passed all validation checks, including Read, List, Write, Delete, Path Exists, File Events Read, Assume Role, Self Assume Role, and External ID Condition. The S3 landing layer is now securely accessible through Databricks Unity Catalog and ready for the downstream Bronze ingestion pipeline.

Key Learning

Authentication and authorization are separate concerns. Successful AssumeRole proves that Databricks can obtain the AWS role, but it does not prove that the role can access the required S3 objects. IAM trust policy controls who can assume the role, while the IAM permissions policy controls what the assumed role can actually do.

That distinction is probably the most interview-worthy learning from this entire setup, because you encountered it through an actual failure rather than simply configuring a happy-path tutorial.

Yes. This is a very good checkpoint to update your README, because you have completed the entire AWS S3 → IAM → Databricks Unity Catalog connectivity layer. Databricks defines the storage credential as the authentication abstraction and the external location as the combination of that credential with the cloud-storage path.  

I would put the following section in your README before moving to Bronze/Silver/Gold.

AWS S3 Integration with Databricks Unity Catalog

The Clinical Trial Intelligence Platform uses Amazon S3 as the cloud data lake and Databricks Unity Catalog for centralized data governance and controlled access to cloud storage.

Architecture implemented so far

                    AWS
                     │
                     ▼
        S3 Bucket
clinical-trial-intelligence-platform-sk
                     │
                     ▼
                  Landing/
                     │
                     │ IAM permissions
                     ▼
      databricks-clinical-trial-s3-role
                     │
                     │ STS AssumeRole
                     ▼
              Databricks
                     │
                     ▼
          Storage Credential
      clinical_trial_s3_credential
                     │
                     ▼
           External Location
            clinical_trial_s3
                     │
                     ▼
       s3://clinical-trial-intelligence-
           platform-sk/Landing/
                     │
                     ▼
              Unity Catalog
       clinical_trial_intelligence

1. Amazon S3 Data Lake

Created an S3 bucket to serve as the cloud storage layer for the platform.

Bucket:
clinical-trial-intelligence-platform-sk
Initial ingestion path:
s3://clinical-trial-intelligence-platform-sk/Landing/

The Landing/ path is intended to receive source data before downstream processing.

⸻

2. IAM Role for Databricks

Created a dedicated AWS IAM role:

databricks-clinical-trial-s3-role

The role provides Databricks with controlled access to the required S3 resources rather than embedding AWS access keys in notebooks or application code.

The IAM policy provides the required S3 operations and permissions needed for Databricks file-event integration.

⸻

3. Cross-Account Trust Configuration

Configured the IAM role’s trust relationship so that Databricks can assume the AWS IAM role using AWS STS.

The trust relationship includes:

Action:
sts:AssumeRole
Security condition:
sts:ExternalId

The Databricks-generated External ID was added to the IAM trust policy.

This protects the AssumeRole relationship by ensuring that the role can only be assumed when the expected External ID condition is satisfied.

⸻

4. Databricks Storage Credential

Created the Unity Catalog storage credential:

clinical_trial_s3_credential

Credential type:

AWS IAM Role

The credential references:

AWS IAM Role
        +
Databricks External ID

The storage credential abstracts the cloud authentication mechanism from users and workloads.  

⸻

5. Storage Credential Validation

The initial validation identified a configuration problem:

Success - Assume Role
Failed  - Self Assume Role
Success - External ID Condition

The IAM trust policy was updated to support the required role-assumption configuration.

After correction:

Success - Assume Role
Success - Self Assume Role
Success - External ID Condition
All Permissions Confirmed

This validated the AWS IAM ↔ Databricks authentication configuration.

⸻

6. File Events Permission Issue

During creation of the external location, Databricks initially failed to verify file-event permissions.

The validation reported failure while attempting operations associated with S3 bucket notifications.

Additional permissions for the Databricks managed file-event infrastructure were therefore added for:

Amazon S3
Amazon SNS
Amazon SQS

This enabled Databricks to provision and manage the resources required for file-event processing.

This troubleshooting step demonstrated that successful S3 data access does not automatically imply sufficient permissions for event-driven ingestion infrastructure.

⸻

7. S3 Data Access Permission Issue

A subsequent validation showed:

Failed  - Read
Success - Assume Role
Success - Self Assume Role
Success - External ID Condition
Success - File Events Resource Provision
Success - Read File Event Queue
Success - Purge File Event Queue
Success - File Events Resource Teardown

The IAM policy was updated with the required S3 data-plane permissions, including object and bucket operations.

After correcting the S3 permissions, the external location validation returned:

Success - Read
Success - List
Success - Write
Success - Delete
Success - Path Exists
Success - File Events Read
Success - Assume Role
Success - Self Assume Role
Success - External ID Condition
All Permissions Confirmed

⸻

8. Unity Catalog External Location

Created the external location:

Name:
clinical_trial_s3
URL:
s3://clinical-trial-intelligence-platform-sk/Landing/
Storage Credential:
clinical_trial_s3_credential

An external location associates an S3 URI with a Unity Catalog storage credential, allowing access to the cloud path to be governed through Unity Catalog.  

⸻

9. Unity Catalog

Created the project catalog:

clinical_trial_intelligence

Description:

Unity Catalog for the Clinical Trial Intelligence Platform.
Governs clinical trial data across the Landing, Bronze,
Silver, and Gold layers, with AWS S3 used as external
cloud storage.

The catalog currently contains the automatically available:

clinical_trial_intelligence
│
├── default
│
└── information_schema

The next stage will introduce the platform’s processing schemas.

clinical_trial_intelligence
│
├── bronze
├── silver
└── gold

Current project status

S3 Bucket                         ✅
Landing Zone                      ✅
IAM Role                          ✅
IAM S3 Permissions                ✅
IAM SNS/SQS File Event Permissions✅
Cross-account Trust               ✅
External ID                       ✅
Storage Credential                ✅
Credential Validation             ✅
External Location                 ✅
External Location Validation      ✅
Unity Catalog                     ✅
Bronze Schema                     ⏳ NEXT
Silver Schema                     ⏳
Gold Schema                       ⏳
Data Ingestion                    ⏳
Transformation Pipeline           ⏳
Data Quality                      ⏳
Serving / Dashboard               ⏳

This is worth documenting because the architecture is not simply “Databricks connected to S3.” You have implemented the security/governance chain:

S3 → IAM policy → IAM trust relationship → STS/External ID → Storage Credential → External Location → Unity Catalog.

That is also a strong interview discussion point because you encountered and diagnosed three different classes of access problems: role assumption, file-event permissions, and S3 data-plane permissions, rather than solving them by granting broad unrestricted access.

Next, we can create bronze properly and decide its S3 storage path before clicking anything.

## Implementation Status

### Phase 1 — AWS and Databricks Foundation ✅

- Created the AWS S3 data lake for the Clinical Trial Intelligence Platform.
- Created the Landing layer for raw source-system data.
- Organized source data into EDC, CTMS, Lab, Safety, master, and protocol domains.
- Created an AWS IAM role for Databricks.
- Configured S3, SNS, and SQS permissions required for data access and file events.
- Created a Unity Catalog storage credential using the AWS IAM role.
- Created and validated the `clinical_trial_s3` external location.
- Enabled file events for incremental file discovery.
- Created the `clinical_trial_intelligence` Unity Catalog catalog.
- Configured the managed storage location.
- Successfully validated access to raw S3 files from Databricks.

Example validated source path:

`Landing/EDC/subjects/`

Databricks successfully detected the date-partitioned/source files stored in this location.

### Phase 2 — Data Engineering Pipeline ⏳

Next implementation phase:

S3 Landing → Auto Loader → Bronze Delta → Silver Delta → Gold Delta

Pipeline code will be version-controlled in GitHub and deployed to Databricks using CI/CD.