# ============================================================
# SILVER: VISITS
# Clinical Trial Intelligence Platform
# ============================================================
#
# Source:
#   clinical_trial_intelligence.bronze.edc_visits
#
# Targets:
#
#   visits
#       - validated Silver clinical visit records
#       - append-only at visit_id grain
#
#   clinical_trial_intelligence.quarantine.visits
#
# Design:
#
#   - canonical shared key normalization
#   - RI against current trusted Silver subject
#   - study/site validation
#   - status-aware visit-date validation
#   - append-only based on currently observed source behavior
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

BRONZE_VISITS = (
    f"{CATALOG}.bronze.edc_visits"
)


# ============================================================
# 1. VALIDATED VISIT STREAM
# ============================================================

@dp.temporary_view(
    name="v_visits_validated"
)
def v_visits_validated():

    # ========================================================
    # BRONZE SOURCE
    # ========================================================

    src = spark.readStream.table(
        BRONZE_VISITS
    )


    # ========================================================
    # CURRENT TRUSTED SILVER SUBJECTS
    # ========================================================
    #
    # Subject SCD2 boundaries represent source-change ordering,
    # not clinical effective dates.
    #
    # Therefore visit RI is validated against the current
    # trusted Silver subject version.
    #
    # ========================================================

    subjects = (

        spark.table("subjects")

        .where(
            F.col("__END_AT").isNull()
        )

        .select(
            normalize_key(
                "subject_id"
            ).alias("_sub_subject_id"),

            normalize_key(
                "study_id"
            ).alias("_sub_study_id"),

            normalize_key(
                "site_id"
            ).alias("_sub_site_id")
        )

        .where(
            F.col("_sub_subject_id").isNotNull()
        )

        .distinct()
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

        .distinct()
    )


    # ========================================================
    # NORMALIZATION + TYPING
    # ========================================================

    typed = (

        src

        # ----------------------------------------------------
        # Canonical business identifiers
        # ----------------------------------------------------

        .withColumn(
            "visit_id",
            normalize_key("visit_id")
        )

        .withColumn(
            "subject_id",
            normalize_key("subject_id")
        )

        .withColumn(
            "study_id",
            normalize_key("study_id")
        )

        .withColumn(
            "site_id",
            normalize_key("site_id")
        )


        # ----------------------------------------------------
        # Visit name
        # ----------------------------------------------------

        .withColumn(
            "visit_name",
            F.upper(
                blank_to_null("visit_name")
            )
        )


        # ----------------------------------------------------
        # Visit status
        # ----------------------------------------------------

        .withColumn(
            "_raw_visit_status",
            F.upper(
                blank_to_null("visit_status")
            )
        )

        .withColumn(
            "visit_status",

            F.when(
                F.col("_raw_visit_status").isin(
                    "COMPLETED",
                    "COMPLETE",
                    "DONE"
                ),
                F.lit("COMPLETED")
            )

            .when(
                F.col("_raw_visit_status") == "MISSED",
                F.lit("MISSED")
            )

            .when(
                F.col("_raw_visit_status") == "RESCHEDULED",
                F.lit("RESCHEDULED")
            )

            .when(
                F.col("_raw_visit_status").isNull()
                | (F.col("_raw_visit_status") == "?"),
                F.lit("UNKNOWN")
            )

            .otherwise(
                F.col("_raw_visit_status")
            )
        )


        # ----------------------------------------------------
        # Numeric conversion
        # ----------------------------------------------------

        .withColumn(
            "visit_number",
            F.expr(
                "try_cast(visit_number AS INT)"
            )
        )

        .withColumn(
            "days_from_baseline",
            F.expr(
                "try_cast(days_from_baseline AS INT)"
            )
        )


        # ----------------------------------------------------
        # Date conversion
        # ----------------------------------------------------

        .withColumn(
            "visit_date",
            F.expr(
                "try_cast(visit_date AS DATE)"
            )
        )
    )


    # ========================================================
    # REFERENTIAL INTEGRITY
    # ========================================================

    enriched = (

        typed.alias("v")

        # ----------------------------------------------------
        # Current trusted Silver subject
        # ----------------------------------------------------

        .join(
            subjects.alias("s"),

            F.col("v.subject_id")
            == F.col("s._sub_subject_id"),

            "left"
        )


        # ----------------------------------------------------
        # Study dimension
        # ----------------------------------------------------

        .join(
            F.broadcast(
                studies
            ),

            F.col("v.study_id")
            == F.col("_st_study_id"),

            "left"
        )


        # ----------------------------------------------------
        # Site dimension
        # ----------------------------------------------------

        .join(
            F.broadcast(
                sites
            ),

            F.col("v.site_id")
            == F.col("_si_site_id"),

            "left"
        )
    )


    # ========================================================
    # DATA QUALITY RULES
    # ========================================================

    dq_failures = F.array_compact(

        F.array(

            # ------------------------------------------------
            # Business key
            # ------------------------------------------------

            F.when(
                F.col("visit_id").isNull(),
                F.lit("missing_visit_id")
            ),


            # ------------------------------------------------
            # Subject
            # ------------------------------------------------

            F.when(
                F.col("subject_id").isNull(),
                F.lit("missing_subject_id")
            ),

            F.when(
                F.col("subject_id").isNotNull()
                & F.col("_sub_subject_id").isNull(),

                F.lit(
                    "unknown_silver_subject"
                )
            ),


            # ------------------------------------------------
            # Study
            # ------------------------------------------------

            F.when(
                F.col("study_id").isNull(),
                F.lit("missing_study_id")
            ),

            F.when(
                F.col("study_id").isNotNull()
                & F.col("_st_study_id").isNull(),

                F.lit("unknown_study_id")
            ),


            # ------------------------------------------------
            # Site
            # ------------------------------------------------

            F.when(
                F.col("site_id").isNull(),
                F.lit("missing_site_id")
            ),

            F.when(
                F.col("site_id").isNotNull()
                & F.col("_si_site_id").isNull(),

                F.lit("unknown_site_id")
            ),

            F.when(
                F.col("_si_site_id").isNotNull()
                & F.col("study_id").isNotNull()
                & (
                    F.col("_si_study_id")
                    != F.col("study_id")
                ),

                F.lit("site_not_in_study")
            ),


            # ------------------------------------------------
            # Subject-study consistency
            # ------------------------------------------------

            F.when(
                F.col("_sub_subject_id").isNotNull()
                & F.col("study_id").isNotNull()
                & (
                    F.col("_sub_study_id")
                    != F.col("study_id")
                ),

                F.lit(
                    "subject_not_in_study"
                )
            ),


            # ------------------------------------------------
            # Subject-site consistency
            # ------------------------------------------------

            F.when(
                F.col("_sub_subject_id").isNotNull()
                & F.col("site_id").isNotNull()
                & (
                    F.col("_sub_site_id")
                    != F.col("site_id")
                ),

                F.lit(
                    "subject_not_in_site"
                )
            ),


            # ------------------------------------------------
            # Visit status
            # ------------------------------------------------

            F.when(
                ~F.col("visit_status").isin(
                    "COMPLETED",
                    "MISSED",
                    "RESCHEDULED"
                ),

                F.lit(
                    "invalid_visit_status"
                )
            ),


            # ------------------------------------------------
            # Status-aware visit date
            # ------------------------------------------------

            F.when(
                (
                    F.col("visit_status")
                    == "COMPLETED"
                )
                &
                F.col("visit_date").isNull(),

                F.lit(
                    "completed_visit_missing_date"
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
                F.col("_dq_failures")
            ) == 0
        )

        .select(

            "visit_id",
            "subject_id",
            "study_id",
            "site_id",

            "visit_name",
            "visit_number",
            "visit_date",
            "visit_status",
            "days_from_baseline",

            "_rescued_data",

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
# 2. VALID VISIT STREAM
# ============================================================

@dp.temporary_view(
    name="v_visits_valid"
)
def v_visits_valid():

    return (

        spark.readStream
        .table(
            "v_visits_validated"
        )

        .where(
            F.col("_is_valid") == True
        )

        .drop(
            "_dq_failures",
            "_is_valid"
        )
    )


# ============================================================
# 3. SILVER VISITS
# ============================================================

@dp.table(

    name="visits",

    comment=(
        "Validated Silver clinical visit records from the EDC "
        "append-only visit feed. Grain: one row per visit_id."
    ),

    table_properties={
        "quality": "silver"
    }
)
def visits():

    return (

        spark.readStream
        .table(
            "v_visits_valid"
        )
    )


# ============================================================
# 4. VISIT QUARANTINE
# ============================================================

@dp.table(

    name=f"{CATALOG}.quarantine.visits",

    comment=(
        "Visit records rejected by Silver clinical "
        "data-quality validation."
    ),

    table_properties={
        "quality": "quarantine"
    }
)
def quarantine_visits():

    return (

        spark.readStream
        .table(
            "v_visits_validated"
        )

        .where(
            F.col("_is_valid") == False
        )

        .withColumn(
            "dq_failure_reasons",

            F.array_join(
                F.col("_dq_failures"),
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