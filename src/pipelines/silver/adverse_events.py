# ============================================================
# SILVER: ADVERSE EVENTS
# Clinical Trial Intelligence Platform
# ============================================================
#
# Source:
#   clinical_trial_intelligence.bronze.safety_adverse_events
#
# Targets:
#
#   adverse_events
#       - validated and standardized adverse events
#
#   clinical_trial_intelligence.quarantine.adverse_events
#
# Design:
#
#   - append-only at ae_id grain based on observed source
#   - current trusted Silver subject RI
#   - site/study RI
#   - severity standardization
#   - ISO clinical date parsing
#   - clinical review flags remain non-blocking
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

BRONZE_ADVERSE_EVENTS = (
    f"{CATALOG}.bronze.safety_adverse_events"
)


# ============================================================
# 1. VALIDATED ADVERSE EVENT STREAM
# ============================================================

@dp.temporary_view(
    name="v_adverse_events_validated"
)
def v_adverse_events_validated():

    # ========================================================
    # BRONZE SOURCE
    # ========================================================

    src = spark.readStream.table(
        BRONZE_ADVERSE_EVENTS
    )


    # ========================================================
    # REFERENCE: SEVERITY
    # ========================================================

    severity_ref = (

        spark.table("ref_severity")

        .select(
            F.upper(
                F.trim(
                    F.col("raw_severity")
                )
            ).alias("_severity_key"),

            F.upper(
                F.trim(
                    F.col("standard_severity")
                )
            ).alias("_standard_severity"),

            F.col(
                "severity_rank"
            ).cast("int").alias(
                "_severity_rank"
            )
        )

        .where(
            F.col("_severity_key").isNotNull()
            & (F.col("_severity_key") != "")
        )

        .groupBy(
            "_severity_key"
        )

        .agg(
            F.max(
                "_standard_severity"
            ).alias("_standard_severity"),

            F.max(
                "_severity_rank"
            ).alias("_severity_rank")
        )
    )


    # ========================================================
    # CURRENT TRUSTED SILVER SUBJECTS
    # ========================================================

    subjects = (

        spark.table("subjects")

        .where(
            F.col("__END_AT").isNull()
        )

        .select(
            normalize_key(
                "subject_id"
            ).alias("_subject_id"),

            normalize_key(
                "study_id"
            ).alias("_subject_study_id"),

            normalize_key(
                "site_id"
            ).alias("_subject_site_id")
        )

        .where(
            F.col("_subject_id").isNotNull()
        )

        .distinct()
    )


    # ========================================================
    # TRUSTED SITE DIMENSION
    # ========================================================

    sites = (

        spark.table("dim_site")

        .select(
            normalize_key(
                "site_id"
            ).alias("_site_id"),

            normalize_key(
                "study_id"
            ).alias("_site_study_id")
        )

        .where(
            F.col("_site_id").isNotNull()
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
            "ae_id",
            normalize_key("ae_id")
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
        # AE terminology
        # ----------------------------------------------------

        .withColumn(
            "ae_term",
            blank_to_null("ae_term")
        )

        .withColumn(
            "meddra_soc",
            blank_to_null("meddra_soc")
        )


        # ----------------------------------------------------
        # Severity
        # ----------------------------------------------------

        .withColumn(
            "source_severity",
            blank_to_null("severity")
        )

        .withColumn(
            "_severity_lookup_key",
            F.upper(
                blank_to_null("severity")
            )
        )


        # ----------------------------------------------------
        # Seriousness
        # ----------------------------------------------------

        .withColumn(
            "source_serious_flag",
            F.upper(
                blank_to_null("serious_flag")
            )
        )

        .withColumn(
            "serious_flag",

            F.when(
                F.upper(
                    blank_to_null("serious_flag")
                ).isin(
                    "Y",
                    "YES",
                    "1",
                    "TRUE"
                ),
                F.lit("YES")
            )

            .when(
                F.upper(
                    blank_to_null("serious_flag")
                ).isin(
                    "N",
                    "NO",
                    "0",
                    "FALSE"
                ),
                F.lit("NO")
            )

            .otherwise(
                F.lit(None).cast("string")
            )
        )


        # ----------------------------------------------------
        # Raw clinical dates
        # ----------------------------------------------------
        #
        # Source uses yyyy-MM-dd.
        # blank_to_null converts "-" / blank to NULL.
        #
        # ----------------------------------------------------

        .withColumn(
            "_raw_onset_date",
            blank_to_null("onset_date")
        )

        .withColumn(
            "_raw_resolution_date",
            blank_to_null("resolution_date")
        )


        # ----------------------------------------------------
        # Parsed clinical dates
        # ----------------------------------------------------

        .withColumn(
            "onset_date",
            F.expr(
                """
                try_to_date(
                    _raw_onset_date,
                    'yyyy-MM-dd'
                )
                """
            )
        )

        .withColumn(
            "resolution_date",
            F.expr(
                """
                try_to_date(
                    _raw_resolution_date,
                    'yyyy-MM-dd'
                )
                """
            )
        )


        # ----------------------------------------------------
        # Outcome / causality / action
        # ----------------------------------------------------

        .withColumn(
            "outcome",
            F.upper(
                blank_to_null("outcome")
            )
        )

        .withColumn(
            "related_to_study_drug",
            F.upper(
                blank_to_null(
                    "related_to_study_drug"
                )
            )
        )

        .withColumn(
            "action_taken",
            F.upper(
                blank_to_null("action_taken")
            )
        )
    )


    # ========================================================
    # REFERENCE / ENTITY RESOLUTION
    # ========================================================

    enriched = (

        typed

        # ----------------------------------------------------
        # Severity
        # ----------------------------------------------------

        .join(
            F.broadcast(
                severity_ref
            ),

            F.col("_severity_lookup_key")
            == F.col("_severity_key"),

            "left"
        )


        # ----------------------------------------------------
        # Trusted subject
        # ----------------------------------------------------

        .join(
            subjects,

            F.col("subject_id")
            == F.col("_subject_id"),

            "left"
        )


        # ----------------------------------------------------
        # Trusted site
        # ----------------------------------------------------

        .join(
            F.broadcast(
                sites
            ),

            F.col("site_id")
            == F.col("_site_id"),

            "left"
        )
    )


    # ========================================================
    # STANDARDIZED CLINICAL ATTRIBUTES
    # ========================================================

    standardized = (

        enriched

        .withColumn(
            "standard_severity",
            F.col("_standard_severity")
        )

        .withColumn(
            "severity_rank",
            F.col("_severity_rank")
        )
    )


    # ========================================================
    # WARNING / REVIEW FLAGS
    # ========================================================
    #
    # These do NOT quarantine records.
    #
    # ========================================================

    reviewed = (

        standardized

        .withColumn(
            "severity_mapping_warning",

            F.col("source_severity").isNotNull()
            &
            F.col("standard_severity").isNull()
        )

        .withColumn(
            "resolution_before_onset_warning",

            F.col("resolution_date").isNotNull()
            &
            F.col("onset_date").isNotNull()
            &
            (
                F.col("resolution_date")
                <
                F.col("onset_date")
            )
        )

        .withColumn(
            "high_severity_nonserious_warning",

            F.col("severity_rank").isin(
                3,
                4,
                5
            )
            &
            (
                F.col("serious_flag")
                == "NO"
            )
        )

        .withColumn(
            "mild_serious_warning",

            (
                F.col("severity_rank") == 1
            )
            &
            (
                F.col("serious_flag") == "YES"
            )
        )

        .withColumn(
            "fatal_nonserious_warning",

            (
                F.col("outcome") == "FATAL"
            )
            &
            (
                F.col("serious_flag") == "NO"
            )
        )
    )


    # ========================================================
    # BLOCKING DATA QUALITY RULES
    # ========================================================

    dq_failures = F.array_compact(

        F.array(

            # ------------------------------------------------
            # AE business key
            # ------------------------------------------------

            F.when(
                F.col("ae_id").isNull(),
                F.lit("missing_ae_id")
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
                &
                F.col("_subject_id").isNull(),

                F.lit("unknown_silver_subject")
            ),

            F.when(
                F.col("_subject_id").isNotNull()
                &
                F.col("study_id").isNotNull()
                &
                (
                    F.col("_subject_study_id")
                    !=
                    F.col("study_id")
                ),

                F.lit("subject_study_mismatch")
            ),

            F.when(
                F.col("_subject_id").isNotNull()
                &
                F.col("site_id").isNotNull()
                &
                (
                    F.col("_subject_site_id")
                    !=
                    F.col("site_id")
                ),

                F.lit("subject_site_mismatch")
            ),


            # ------------------------------------------------
            # Study
            # ------------------------------------------------

            F.when(
                F.col("study_id").isNull(),
                F.lit("missing_study_id")
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
                &
                F.col("_site_id").isNull(),

                F.lit("unknown_site_id")
            ),

            F.when(
                F.col("_site_id").isNotNull()
                &
                F.col("study_id").isNotNull()
                &
                (
                    F.col("_site_study_id")
                    !=
                    F.col("study_id")
                ),

                F.lit("site_study_mismatch")
            ),


            # ------------------------------------------------
            # Invalid onset date
            # ------------------------------------------------
            #
            # Missing onset date is not classified as malformed.
            #
            # ------------------------------------------------

            F.when(
                F.col("_raw_onset_date").isNotNull()
                &
                F.col("onset_date").isNull(),

                F.lit("invalid_onset_date")
            )
        )
    )


    # ========================================================
    # FINAL VALIDATED DATASET
    # ========================================================

    return (

        reviewed

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

            "ae_id",
            "subject_id",
            "study_id",
            "site_id",

            "ae_term",
            "meddra_soc",

            "source_severity",
            "standard_severity",
            "severity_rank",

            "source_serious_flag",
            "serious_flag",

            "onset_date",
            "resolution_date",
            "outcome",

            "related_to_study_drug",
            "action_taken",

            "severity_mapping_warning",
            "resolution_before_onset_warning",
            "high_severity_nonserious_warning",
            "mild_serious_warning",
            "fatal_nonserious_warning",

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
# 2. VALID ADVERSE EVENT STREAM
# ============================================================

@dp.temporary_view(
    name="v_adverse_events_valid"
)
def v_adverse_events_valid():

    return (

        spark.readStream
        .table(
            "v_adverse_events_validated"
        )

        .where(
            F.col("_is_valid") == True
        )
    )


# ============================================================
# 3. SILVER ADVERSE EVENTS
# ============================================================

@dp.table(

    name="adverse_events",

    comment=(
        "Validated and standardized Silver adverse events "
        "from the clinical safety event feed."
    ),

    table_properties={
        "quality": "silver"
    }
)
def adverse_events():

    return (

        spark.readStream
        .table(
            "v_adverse_events_valid"
        )

        .withColumn(
            "_silver_processed_ts",
            F.current_timestamp()
        )

        .drop(
            "_dq_failures",
            "_is_valid"
        )
    )


# ============================================================
# 4. ADVERSE EVENT QUARANTINE
# ============================================================

@dp.table(

    name=f"{CATALOG}.quarantine.adverse_events",

    comment=(
        "Adverse-event records rejected by Silver "
        "clinical data-quality validation."
    ),

    table_properties={
        "quality": "quarantine"
    }
)
def quarantine_adverse_events():

    return (

        spark.readStream
        .table(
            "v_adverse_events_validated"
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