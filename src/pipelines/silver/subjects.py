# ============================================================
# SILVER: SUBJECTS
# Clinical Trial Intelligence Platform
# ============================================================
#
# Source:
#   clinical_trial_intelligence.bronze.edc_subjects
#
# Targets:
#
#   subjects
#       - validated Silver subject records
#       - SCD Type 2 history
#
#   clinical_trial_intelligence.quarantine.subjects
#       - rejected subject records
#       - retains all DQ failure reasons
#
# ============================================================
# CDC DESIGN
# ============================================================
#
# Source exploration established that the EDC subject feed is
# composed of:
#
#   1. an initial population file
#   2. subsequent incremental change files
#
# It is therefore NOT a sequence of complete snapshots.
#
# AUTO CDC is used instead of AUTO CDC FROM SNAPSHOT.
#
# Business key:
#
#   subject_id
#
# Logical ordering:
#
#   STRUCT(
#       source_snapshot_date,
#       _source_file_name
#   )
#
# source_snapshot_date is derived from:
#
#   subjects_YYYYMMDD.csv
#
# No explicit source delete indicator currently exists.
# Therefore hard-delete semantics are not applied.
#
# ============================================================
# KEY NORMALIZATION
# ============================================================
#
# All Silver business identifiers use the common normalization
# contract defined in:
#
#   Utility.silver_common
#
# Canonical key normalization:
#
#   TRIM
#      ->
#   blank / "-" to NULL
#      ->
#   UPPER
#
# This ensures that subject keys stored in the SCD2 dimension
# use the same representation as foreign keys in downstream
# Silver facts.
#
# ============================================================
# REFERENCE DEDUPLICATION
# ============================================================
#
# Reference/dimension lookups expose one deterministic row per
# normalized business key.
#
# F.first() is deliberately avoided after groupBy because the
# surviving row is not deterministically ordered.
#
# MAX() is used only for reference attributes expected to be
# functionally dependent on the normalized lookup key.
#
# Reference-conflict validation must verify that duplicate
# normalized keys do not contain conflicting business values.
#
# ============================================================
# DQ POLICY
# ============================================================
#
# ANY identified blocking DQ failure routes the source record
# to quarantine.
#
# Exploration baseline:
#
#   Bronze rows:       3,829
#   DQ-valid rows:     3,643
#   DQ-invalid rows:     186
#
#   9 invalid records fail two rules.
#
# These remain exploration baselines and must be revalidated
# after the final Silver implementation.
#
# ============================================================


from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.Utility.silver_common import (
    blank_to_null,
    normalize_key
)


# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "clinical_trial_intelligence"

BRONZE_SUBJECTS = (
    f"{CATALOG}.bronze.edc_subjects"
)


# ============================================================
# 1. VALIDATED SUBJECT STREAM
# ============================================================

@dp.temporary_view(
    name="v_subjects_validated"
)
def v_subjects_validated():

    # ========================================================
    # BRONZE SOURCE
    # ========================================================

    src = spark.readStream.table(
        BRONZE_SUBJECTS
    )


    # ========================================================
    # REFERENCE: SEX
    # ========================================================

    sex_ref = (

        spark.table("ref_sex")

        .select(
            F.upper(
                F.trim(
                    F.col("raw_sex")
                )
            ).alias("_ref_raw_sex"),

            F.upper(
                F.trim(
                    F.col("standard_sex")
                )
            ).alias("_standard_sex")
        )

        .where(
            F.col("_ref_raw_sex").isNotNull()
            & (F.col("_ref_raw_sex") != "")
        )

        .groupBy(
            "_ref_raw_sex"
        )

        .agg(
            F.max(
                "_standard_sex"
            ).alias("_standard_sex")
        )
    )


    # ========================================================
    # REFERENCE: DIAGNOSIS
    # ========================================================

    diagnosis_ref = (

        spark.table("ref_diagnosis")

        .select(
            F.upper(
                F.trim(
                    F.col("diagnosis_code")
                )
            ).alias("_dx_code"),

            F.trim(
                F.col("diagnosis_description")
            ).alias("baseline_condition")
        )

        .where(
            F.col("_dx_code").isNotNull()
            & (F.col("_dx_code") != "")
        )

        .groupBy(
            "_dx_code"
        )

        .agg(
            F.max(
                "baseline_condition"
            ).alias("baseline_condition")
        )
    )


    # ========================================================
    # DIMENSION: STUDY
    # ========================================================

    studies = (

        spark.table("dim_study")

        .select(
            normalize_key(
                "study_id"
            ).alias("_st_study_id")
        )

        .where(
            F.col("_st_study_id").isNotNull()
        )

        .distinct()
    )


    # ========================================================
    # DIMENSION: SITE
    # ========================================================

    sites = (

        spark.table("dim_site")

        .select(
            normalize_key(
                "site_id"
            ).alias("_si_site_id"),

            normalize_key(
                "study_id"
            ).alias("_si_study_id")
        )

        .where(
            F.col("_si_site_id").isNotNull()
        )

        .groupBy(
            "_si_site_id"
        )

        .agg(
            F.max(
                "_si_study_id"
            ).alias("_si_study_id")
        )
    )


    # ========================================================
    # DIMENSION: STUDY ARM
    # ========================================================

    arms = (

        spark.table("dim_study_arm")

        .select(
            normalize_key(
                "study_id"
            ).alias("_arm_study_id"),

            normalize_key(
                "arm_code"
            ).alias("_arm_code")
        )

        .where(
            F.col("_arm_study_id").isNotNull()
            & F.col("_arm_code").isNotNull()
        )

        .distinct()
    )


    # ========================================================
    # NORMALIZATION + TYPING
    # ========================================================

    typed = (

        src


        # ----------------------------------------------------
        # Logical source date
        # ----------------------------------------------------

        .withColumn(
            "source_snapshot_date",

            F.to_date(
                F.regexp_extract(
                    F.col("_source_file_name"),
                    r"^subjects_(\d{8})\.csv$",
                    1
                ),
                "yyyyMMdd"
            )
        )


        # ----------------------------------------------------
        # Canonical business identifiers
        # ----------------------------------------------------

        .withColumn(
            "subject_id",
            normalize_key(
                "subject_id"
            )
        )

        .withColumn(
            "study_id",
            normalize_key(
                "study_id"
            )
        )

        .withColumn(
            "site_id",
            normalize_key(
                "site_id"
            )
        )

        .withColumn(
            "arm_code",
            normalize_key(
                "arm_code"
            )
        )


        # ----------------------------------------------------
        # Subject status
        # ----------------------------------------------------

        .withColumn(
            "subject_status",

            F.upper(
                blank_to_null(
                    "subject_status"
                )
            )
        )


        # ----------------------------------------------------
        # Clinical attributes
        # ----------------------------------------------------

        .withColumn(
            "baseline_condition_code",

            F.upper(
                blank_to_null(
                    "baseline_condition_code"
                )
            )
        )

        .withColumn(
            "discontinuation_reason",

            F.upper(
                blank_to_null(
                    "discontinuation_reason"
                )
            )
        )


        # ----------------------------------------------------
        # Numeric conversion
        # ----------------------------------------------------

        .withColumn(
            "age",

            F.expr(
                "try_cast(age AS INT)"
            )
        )


        # ----------------------------------------------------
        # Date conversion
        # ----------------------------------------------------

        .withColumn(
            "screening_date",

            F.expr(
                "try_cast(screening_date AS DATE)"
            )
        )

        .withColumn(
            "enrollment_date",

            F.expr(
                "try_cast(enrollment_date AS DATE)"
            )
        )

        .withColumn(
            "randomization_date",

            F.expr(
                "try_cast(randomization_date AS DATE)"
            )
        )

        .withColumn(
            "informed_consent_date",

            F.expr(
                "try_cast(informed_consent_date AS DATE)"
            )
        )

        .withColumn(
            "discontinuation_date",

            F.expr(
                "try_cast(discontinuation_date AS DATE)"
            )
        )


        # ----------------------------------------------------
        # Normalized sex lookup key
        # ----------------------------------------------------

        .withColumn(
            "_raw_sex",

            F.when(
                blank_to_null(
                    "sex"
                ).isNull(),

                F.lit(None)
            )

            .otherwise(
                F.upper(
                    blank_to_null(
                        "sex"
                    )
                )
            )
        )
    )


    # ========================================================
    # REFERENCE / DIMENSION RESOLUTION
    # ========================================================

    enriched = (

        typed


        # ----------------------------------------------------
        # Sex standardization
        # ----------------------------------------------------

        .join(
            F.broadcast(
                sex_ref
            ),

            F.col("_raw_sex")
            == F.col("_ref_raw_sex"),

            "left"
        )


        # ----------------------------------------------------
        # Diagnosis lookup
        # ----------------------------------------------------

        .join(
            F.broadcast(
                diagnosis_ref
            ),

            F.col("baseline_condition_code")
            == F.col("_dx_code"),

            "left"
        )


        # ----------------------------------------------------
        # Study validation
        # ----------------------------------------------------

        .join(
            F.broadcast(
                studies
            ),

            F.col("study_id")
            == F.col("_st_study_id"),

            "left"
        )


        # ----------------------------------------------------
        # Site validation
        # ----------------------------------------------------

        .join(
            F.broadcast(
                sites
            ),

            F.col("site_id")
            == F.col("_si_site_id"),

            "left"
        )


        # ----------------------------------------------------
        # Study-arm validation
        # ----------------------------------------------------

        .join(
            F.broadcast(
                arms
            ),

            (
                F.col("study_id")
                == F.col("_arm_study_id")
            )
            &
            (
                F.col("arm_code")
                == F.col("_arm_code")
            ),

            "left"
        )


        # ----------------------------------------------------
        # Standardized sex
        # ----------------------------------------------------

        .withColumn(
            "sex",

            F.when(
                F.col("_raw_sex").isNull(),
                F.lit(None)
            )

            .otherwise(
                F.coalesce(
                    F.col("_standard_sex"),
                    F.lit("UNMAPPED")
                )
            )
        )
    )


    # ========================================================
    # CLINICAL VALIDATION RULES
    # ========================================================

    arm_required_status = (

        F.col(
            "subject_status"
        )

        .isin(
            "ENROLLED",
            "DISCONTINUED",
            "COMPLETED"
        )
    )


    dq_failures = F.array_compact(

        F.array(


            # ------------------------------------------------
            # Business key
            # ------------------------------------------------

            F.when(
                F.col(
                    "subject_id"
                ).isNull(),

                F.lit(
                    "missing_subject_id"
                )
            ),


            # ------------------------------------------------
            # Study
            # ------------------------------------------------

            F.when(
                F.col(
                    "study_id"
                ).isNull(),

                F.lit(
                    "missing_study_id"
                )
            ),

            F.when(
                F.col(
                    "study_id"
                ).isNotNull()
                & F.col(
                    "_st_study_id"
                ).isNull(),

                F.lit(
                    "unknown_study_id"
                )
            ),


            # ------------------------------------------------
            # Site
            # ------------------------------------------------

            F.when(
                F.col(
                    "site_id"
                ).isNull(),

                F.lit(
                    "missing_site_id"
                )
            ),

            F.when(
                F.col(
                    "site_id"
                ).isNotNull()
                & F.col(
                    "_si_site_id"
                ).isNull(),

                F.lit(
                    "unknown_site_id"
                )
            ),

            F.when(
                F.col(
                    "_si_site_id"
                ).isNotNull()
                & F.col(
                    "study_id"
                ).isNotNull()
                & (
                    F.col(
                        "_si_study_id"
                    )
                    !=
                    F.col(
                        "study_id"
                    )
                ),

                F.lit(
                    "site_not_in_study"
                )
            ),


            # ------------------------------------------------
            # Age
            # ------------------------------------------------

            F.when(
                F.col(
                    "age"
                ).isNull()
                |
                ~F.col(
                    "age"
                ).between(
                    18,
                    100
                ),

                F.lit(
                    "age_out_of_range"
                )
            ),


            # ------------------------------------------------
            # Sex
            # ------------------------------------------------

            F.when(
                F.col(
                    "_raw_sex"
                ).isNotNull()
                & F.col(
                    "_standard_sex"
                ).isNull(),

                F.lit(
                    "unmappable_sex"
                )
            ),


            # ------------------------------------------------
            # Diagnosis
            # ------------------------------------------------

            F.when(
                F.col(
                    "baseline_condition_code"
                ).isNotNull()
                & F.col(
                    "_dx_code"
                ).isNull(),

                F.lit(
                    "unknown_baseline_condition"
                )
            ),


            # ------------------------------------------------
            # CDC sequence
            # ------------------------------------------------

            F.when(
                F.col(
                    "source_snapshot_date"
                ).isNull(),

                F.lit(
                    "invalid_source_snapshot_date"
                )
            ),


            # ------------------------------------------------
            # Screening
            # ------------------------------------------------

            F.when(
                F.col(
                    "screening_date"
                ).isNull(),

                F.lit(
                    "missing_screening_date"
                )
            ),


            # ------------------------------------------------
            # Temporal consistency
            # ------------------------------------------------

            F.when(
                F.col(
                    "enrollment_date"
                ).isNotNull()
                & F.col(
                    "screening_date"
                ).isNotNull()
                & (
                    F.col(
                        "enrollment_date"
                    )
                    <
                    F.col(
                        "screening_date"
                    )
                ),

                F.lit(
                    "enrollment_before_screening"
                )
            ),

            F.when(
                F.col(
                    "informed_consent_date"
                ).isNotNull()
                & F.col(
                    "enrollment_date"
                ).isNotNull()
                & (
                    F.col(
                        "informed_consent_date"
                    )
                    >
                    F.col(
                        "enrollment_date"
                    )
                ),

                F.lit(
                    "consent_after_enrollment"
                )
            ),

            F.when(
                F.col(
                    "randomization_date"
                ).isNotNull()
                & F.col(
                    "enrollment_date"
                ).isNotNull()
                & (
                    F.col(
                        "randomization_date"
                    )
                    <
                    F.col(
                        "enrollment_date"
                    )
                ),

                F.lit(
                    "randomization_before_enrollment"
                )
            ),

            F.when(
                F.col(
                    "discontinuation_date"
                ).isNotNull()
                & F.col(
                    "enrollment_date"
                ).isNotNull()
                & (
                    F.col(
                        "discontinuation_date"
                    )
                    <
                    F.col(
                        "enrollment_date"
                    )
                ),

                F.lit(
                    "discontinuation_before_enrollment"
                )
            ),

            F.when(
                F.col(
                    "discontinuation_date"
                ).isNotNull()
                & F.col(
                    "randomization_date"
                ).isNotNull()
                & (
                    F.col(
                        "discontinuation_date"
                    )
                    <
                    F.col(
                        "randomization_date"
                    )
                ),

                F.lit(
                    "discontinuation_before_randomization"
                )
            ),


            # ------------------------------------------------
            # Subject status
            # ------------------------------------------------

            F.when(
                F.col(
                    "subject_status"
                ).isNull()
                |
                ~F.col(
                    "subject_status"
                ).isin(
                    "SCREENING",
                    "ENROLLED",
                    "SCREEN_FAILED",
                    "DISCONTINUED",
                    "COMPLETED"
                ),

                F.lit(
                    "invalid_subject_status"
                )
            ),


            # ------------------------------------------------
            # Arm assignment
            # ------------------------------------------------

            F.when(
                arm_required_status
                & F.col(
                    "arm_code"
                ).isNull(),

                F.lit(
                    "enrolled_without_arm"
                )
            ),

            F.when(
                F.col(
                    "arm_code"
                ).isNotNull()
                & F.col(
                    "_arm_code"
                ).isNull(),

                F.lit(
                    "unknown_arm_code"
                )
            )
        )
    )


    # ========================================================
    # FINAL VALIDATED DATASET
    # ========================================================

    return (

        enriched

        .withColumn(
            "_dq_failures",
            dq_failures
        )

        .withColumn(
            "_is_valid",

            F.size(
                F.col(
                    "_dq_failures"
                )
            ) == 0
        )

        .select(

            "subject_id",
            "study_id",
            "site_id",

            "screening_date",
            "enrollment_date",
            "subject_status",

            "age",
            "sex",

            "baseline_condition_code",
            "baseline_condition",
            "arm_code",

            "randomization_date",
            "informed_consent_date",
            "discontinuation_date",
            "discontinuation_reason",

            "source_snapshot_date",

            "_source_file",
            "_source_file_name",
            "_source_file_modification_ts",
            "_ingestion_ts",
            "_ingestion_date",

            "_dq_failures",
            "_is_valid"
        )
    )


# ============================================================
# 2. VALID SUBJECT STREAM
# ============================================================

@dp.temporary_view(
    name="v_subjects_valid"
)
def v_subjects_valid():

    return (

        spark.readStream
        .table(
            "v_subjects_validated"
        )

        .where(
            F.col(
                "_is_valid"
            ) == True
        )
    )


# ============================================================
# 3. SILVER SUBJECT TARGET
# ============================================================

dp.create_streaming_table(

    name="subjects",

    comment=(
        "Validated Silver subject history from the EDC "
        "incremental change feed, maintained as SCD Type 2."
    ),

    table_properties={
        "quality": "silver"
    }
)


# ============================================================
# 4. AUTO CDC — SCD TYPE 2
# ============================================================

dp.create_auto_cdc_flow(

    target="subjects",

    source="v_subjects_valid",

    keys=[
        "subject_id"
    ],

    sequence_by=F.struct(
        F.col(
            "source_snapshot_date"
        ),
        F.col(
            "_source_file_name"
        )
    ),

    stored_as_scd_type=2,

    except_column_list=[
        "_dq_failures",
        "_is_valid",
        "source_snapshot_date",
        "_source_file_name"
    ],

    track_history_except_column_list=[
        "_source_file",
        "_source_file_modification_ts",
        "_ingestion_ts",
        "_ingestion_date"
    ]
)


# ============================================================
# 5. SUBJECT QUARANTINE
# ============================================================

@dp.table(

    name=f"{CATALOG}.quarantine.subjects",

    comment=(
        "Subject records rejected by Silver clinical "
        "data-quality validation."
    ),

    table_properties={
        "quality": "quarantine"
    }
)
def quarantine_subjects():

    return (

        spark.readStream
        .table(
            "v_subjects_validated"
        )

        .where(
            F.col(
                "_is_valid"
            ) == False
        )

        .withColumn(
            "dq_failure_reasons",

            F.array_join(
                F.col(
                    "_dq_failures"
                ),
                ";"
            )
        )

        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )

        .drop(
            "_is_valid"
        )
    )