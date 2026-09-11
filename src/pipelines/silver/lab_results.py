# ============================================================
# SILVER: LAB RESULTS
# Clinical Trial Intelligence Platform
# ============================================================
#
# Source:
#   clinical_trial_intelligence.bronze.lab_results
#
# Targets:
#
#   lab_results
#       - validated and standardized laboratory results
#       - append-oriented clinical event table
#
#   clinical_trial_intelligence.quarantine.lab_results
#       - rejected laboratory-result records
#
# ============================================================
# DESIGN
# ============================================================
#
# Grain:
#   one row = one lab_result_id
#
# Observed source behavior:
#   append-only
#
# Referential integrity:
#   - trusted Silver subjects
#   - trusted Silver visits
#
# Subject SCD2 policy:
#
#   Current subject version (__END_AT IS NULL) is used for
#   referential integrity.
#
#   The subject SCD2 sequence is based on source_snapshot_date,
#   which represents source-change ordering rather than
#   clinical event effective time. Therefore collection_date
#   must NOT be compared directly with __START_AT/__END_AT.
#
# Laboratory standardization:
#   standardized_result_value
#       = result_value * conversion_factor
#
# Source abnormal flag:
#   retained for auditability
#
# Derived abnormal flag:
#   calculated from standardized result and standard reference
#   range supplied by ref_lab_test.
#
# Source reference ranges:
#   retained for auditability but are not blocking DQ fields.
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

BRONZE_LAB_RESULTS = (
    f"{CATALOG}.bronze.lab_results"
)


# ============================================================
# 1. VALIDATED LAB RESULT STREAM
# ============================================================

@dp.temporary_view(
    name="v_lab_results_validated"
)
def v_lab_results_validated():

    # ========================================================
    # BRONZE SOURCE
    # ========================================================

    src = spark.readStream.table(
        BRONZE_LAB_RESULTS
    )


    # ========================================================
    # REFERENCE: LAB TEST
    # ========================================================
    #
    # MAX() is deterministic.
    #
    # Reference validation must independently confirm that one
    # normalized lab_test_code does not map to conflicting
    # business values.
    #
    # ========================================================

    lab_test_ref = (

        spark.table("ref_lab_test")

        .select(
            normalize_key(
                "lab_test_code"
            ).alias("_ref_lab_test_code"),

            F.trim(
                F.col("lab_test_name")
            ).alias("_ref_lab_test_name"),

            F.trim(
                F.col("standard_unit")
            ).alias("_ref_standard_unit"),

            F.col(
                "reference_low"
            ).cast("double").alias(
                "_ref_reference_low"
            ),

            F.col(
                "reference_high"
            ).cast("double").alias(
                "_ref_reference_high"
            )
        )

        .where(
            F.col(
                "_ref_lab_test_code"
            ).isNotNull()
        )

        .groupBy(
            "_ref_lab_test_code"
        )

        .agg(
            F.max(
                "_ref_lab_test_name"
            ).alias("_ref_lab_test_name"),

            F.max(
                "_ref_standard_unit"
            ).alias("_ref_standard_unit"),

            F.max(
                "_ref_reference_low"
            ).alias("_ref_reference_low"),

            F.max(
                "_ref_reference_high"
            ).alias("_ref_reference_high")
        )
    )


    # ========================================================
    # REFERENCE: UNIT
    # ========================================================
    #
    # Exploration already established duplicate normalized
    # lookup keys.
    #
    # Conflict validation showed duplicate normalized keys
    # resolve to the same standard unit/conversion factor.
    #
    # Therefore deterministic MAX() is safe for this reference.
    #
    # ========================================================

    unit_ref = (

        spark.table("ref_unit")

        .select(
            normalize_key(
                "lab_test_code"
            ).alias("_unit_lab_test_code"),

            F.upper(
                F.trim(
                    F.col("raw_unit")
                )
            ).alias("_normalized_raw_unit"),

            F.trim(
                F.col("standard_unit")
            ).alias("_unit_standard_unit"),

            F.col(
                "conversion_factor"
            ).cast("double").alias(
                "_conversion_factor"
            )
        )

        .where(
            F.col(
                "_unit_lab_test_code"
            ).isNotNull()
            &
            F.col(
                "_normalized_raw_unit"
            ).isNotNull()
            &
            (
                F.col(
                    "_normalized_raw_unit"
                ) != ""
            )
        )

        .groupBy(
            "_unit_lab_test_code",
            "_normalized_raw_unit"
        )

        .agg(
            F.max(
                "_unit_standard_unit"
            ).alias("_unit_standard_unit"),

            F.max(
                "_conversion_factor"
            ).alias("_conversion_factor")
        )
    )


    # ========================================================
    # TRUSTED SILVER SUBJECT
    # ========================================================
    #
    # subjects is SCD Type 2.
    #
    # RI uses the CURRENT trusted subject version.
    #
    # __END_AT IS NULL = current SCD2 version.
    #
    # We intentionally do NOT compare collection_date against
    # __START_AT/__END_AT because those boundaries are based on
    # source_snapshot_date rather than clinical effective time.
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
            ).alias("_subject_id"),

            normalize_key(
                "study_id"
            ).alias("_subject_study_id")
        )

        .where(
            F.col(
                "_subject_id"
            ).isNotNull()
        )

        .distinct()
    )


    # ========================================================
    # TRUSTED SILVER VISIT
    # ========================================================

    visits = (

        spark.table("visits")

        .select(
            normalize_key(
                "visit_id"
            ).alias("_visit_id"),

            normalize_key(
                "subject_id"
            ).alias("_visit_subject_id"),

            normalize_key(
                "study_id"
            ).alias("_visit_study_id")
        )

        .where(
            F.col(
                "_visit_id"
            ).isNotNull()
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
            "lab_result_id",
            normalize_key(
                "lab_result_id"
            )
        )

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
            "visit_id",
            normalize_key(
                "visit_id"
            )
        )


        # ----------------------------------------------------
        # Laboratory test
        # ----------------------------------------------------

        .withColumn(
            "lab_test_code",
            normalize_key(
                "lab_test_code"
            )
        )

        .withColumn(
            "lab_test_name",
            blank_to_null(
                "lab_test_name"
            )
        )


        # ----------------------------------------------------
        # Measurement
        # ----------------------------------------------------

        .withColumn(
            "result_value",

            F.col(
                "result_value"
            ).cast("double")
        )

        .withColumn(
            "result_unit",

            blank_to_null(
                "result_unit"
            )
        )

        .withColumn(
            "_normalized_result_unit",

            F.when(
                blank_to_null(
                    "result_unit"
                ).isNull(),

                F.lit(None)
            )

            .otherwise(
                F.upper(
                    blank_to_null(
                        "result_unit"
                    )
                )
            )
        )


        # ----------------------------------------------------
        # Source reference range
        # ----------------------------------------------------
        #
        # Retained for lineage/auditability.
        #
        # The standardized reference range is authoritative for
        # derived abnormality.
        #
        # ----------------------------------------------------

        .withColumn(
            "source_reference_low",

            F.col(
                "reference_low"
            ).cast("double")
        )

        .withColumn(
            "source_reference_high",

            F.col(
                "reference_high"
            ).cast("double")
        )


        # ----------------------------------------------------
        # Source abnormal flag
        # ----------------------------------------------------

        .withColumn(
            "source_abnormal_flag",

            F.upper(
                blank_to_null(
                    "abnormal_flag"
                )
            )
        )


        # ----------------------------------------------------
        # Collection date
        # ----------------------------------------------------

        .withColumn(
            "collection_date",

            F.expr(
                """
                try_to_date(
                    trim(collection_date),
                    'dd-MMM-yyyy'
                )
                """
            )
        )


        # ----------------------------------------------------
        # Laboratory vendor
        # ----------------------------------------------------

        .withColumn(
            "lab_vendor",

            F.upper(
                blank_to_null(
                    "lab_vendor"
                )
            )
        )
    )


    # ========================================================
    # REFERENCE / ENTITY RESOLUTION
    # ========================================================

    enriched = (

        typed


        # ----------------------------------------------------
        # Lab-test reference
        # ----------------------------------------------------

        .join(
            F.broadcast(
                lab_test_ref
            ),

            F.col(
                "lab_test_code"
            )
            ==
            F.col(
                "_ref_lab_test_code"
            ),

            "left"
        )


        # ----------------------------------------------------
        # Unit reference
        # ----------------------------------------------------

        .join(
            F.broadcast(
                unit_ref
            ),

            (
                F.col(
                    "lab_test_code"
                )
                ==
                F.col(
                    "_unit_lab_test_code"
                )
            )
            &
            (
                F.col(
                    "_normalized_result_unit"
                )
                ==
                F.col(
                    "_normalized_raw_unit"
                )
            ),

            "left"
        )


        # ----------------------------------------------------
        # Trusted Silver subject
        # ----------------------------------------------------

        .join(
            subjects,

            F.col(
                "subject_id"
            )
            ==
            F.col(
                "_subject_id"
            ),

            "left"
        )


        # ----------------------------------------------------
        # Trusted Silver visit
        # ----------------------------------------------------

        .join(
            visits,

            F.col(
                "visit_id"
            )
            ==
            F.col(
                "_visit_id"
            ),

            "left"
        )
    )


    # ========================================================
    # STANDARDIZED MEASUREMENT
    # ========================================================

    standardized = (

        enriched

        .withColumn(
            "standard_unit",

            F.coalesce(
                F.col(
                    "_unit_standard_unit"
                ),
                F.col(
                    "_ref_standard_unit"
                )
            )
        )

        .withColumn(
            "conversion_factor",

            F.col(
                "_conversion_factor"
            )
        )

        .withColumn(
            "standardized_result_value",

            F.when(
                F.col(
                    "result_value"
                ).isNotNull()
                &
                F.col(
                    "conversion_factor"
                ).isNotNull(),

                F.col(
                    "result_value"
                )
                *
                F.col(
                    "conversion_factor"
                )
            )
        )

        .withColumn(
            "standard_reference_low",

            F.col(
                "_ref_reference_low"
            )
        )

        .withColumn(
            "standard_reference_high",

            F.col(
                "_ref_reference_high"
            )
        )
    )


    # ========================================================
    # DERIVED ABNORMALITY
    # ========================================================

    classified = (

        standardized

        .withColumn(
            "derived_abnormal_flag",

            F.when(
                F.col(
                    "standardized_result_value"
                ).isNull()
                |
                F.col(
                    "standard_reference_low"
                ).isNull()
                |
                F.col(
                    "standard_reference_high"
                ).isNull(),

                F.lit(None)
            )

            .when(
                (
                    F.col(
                        "standardized_result_value"
                    )
                    <
                    F.col(
                        "standard_reference_low"
                    )
                )
                |
                (
                    F.col(
                        "standardized_result_value"
                    )
                    >
                    F.col(
                        "standard_reference_high"
                    )
                ),

                F.lit("Y")
            )

            .otherwise(
                F.lit("N")
            )
        )


        # ----------------------------------------------------
        # Source-vs-derived discrepancy
        # ----------------------------------------------------

        .withColumn(
            "abnormal_flag_discrepancy",

            F.when(
                F.col(
                    "source_abnormal_flag"
                ).isin(
                    "Y",
                    "N"
                )
                &
                F.col(
                    "derived_abnormal_flag"
                ).isNotNull(),

                F.col(
                    "source_abnormal_flag"
                )
                !=
                F.col(
                    "derived_abnormal_flag"
                )
            )

            .otherwise(
                F.lit(
                    None
                ).cast(
                    "boolean"
                )
            )
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
                F.col(
                    "lab_result_id"
                ).isNull(),

                F.lit(
                    "missing_lab_result_id"
                )
            ),


            # ------------------------------------------------
            # Subject
            # ------------------------------------------------

            F.when(
                F.col(
                    "subject_id"
                ).isNull(),

                F.lit(
                    "missing_subject_id"
                )
            ),

            F.when(
                F.col(
                    "subject_id"
                ).isNotNull()
                &
                F.col(
                    "_subject_id"
                ).isNull(),

                F.lit(
                    "unknown_silver_subject"
                )
            ),

            F.when(
                F.col(
                    "_subject_id"
                ).isNotNull()
                &
                F.col(
                    "study_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "_subject_study_id"
                    )
                    !=
                    F.col(
                        "study_id"
                    )
                ),

                F.lit(
                    "subject_study_mismatch"
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


            # ------------------------------------------------
            # Visit
            # ------------------------------------------------

            F.when(
                F.col(
                    "visit_id"
                ).isNull(),

                F.lit(
                    "missing_visit_id"
                )
            ),

            F.when(
                F.col(
                    "visit_id"
                ).isNotNull()
                &
                F.col(
                    "_visit_id"
                ).isNull(),

                F.lit(
                    "unknown_silver_visit"
                )
            ),

            F.when(
                F.col(
                    "_visit_id"
                ).isNotNull()
                &
                F.col(
                    "subject_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "subject_id"
                    )
                    !=
                    F.col(
                        "_visit_subject_id"
                    )
                ),

                F.lit(
                    "visit_subject_mismatch"
                )
            ),

            F.when(
                F.col(
                    "_visit_id"
                ).isNotNull()
                &
                F.col(
                    "study_id"
                ).isNotNull()
                &
                (
                    F.col(
                        "study_id"
                    )
                    !=
                    F.col(
                        "_visit_study_id"
                    )
                ),

                F.lit(
                    "visit_study_mismatch"
                )
            ),


            # ------------------------------------------------
            # Laboratory test
            # ------------------------------------------------

            F.when(
                F.col(
                    "lab_test_code"
                ).isNull(),

                F.lit(
                    "missing_lab_test_code"
                )
            ),

            F.when(
                F.col(
                    "lab_test_code"
                ).isNotNull()
                &
                F.col(
                    "_ref_lab_test_code"
                ).isNull(),

                F.lit(
                    "unknown_lab_test_code"
                )
            ),


            # ------------------------------------------------
            # Measurement
            # ------------------------------------------------

            F.when(
                F.col(
                    "result_value"
                ).isNull(),

                F.lit(
                    "missing_result_value"
                )
            ),

            F.when(
                F.col(
                    "result_unit"
                ).isNull(),

                F.lit(
                    "missing_result_unit"
                )
            ),


            # ------------------------------------------------
            # Unit mapping
            # ------------------------------------------------

            F.when(
                F.col(
                    "result_value"
                ).isNotNull()
                &
                F.col(
                    "result_unit"
                ).isNotNull()
                &
                F.col(
                    "_conversion_factor"
                ).isNull(),

                F.lit(
                    "unmapped_test_unit"
                )
            ),


            # ------------------------------------------------
            # Standard reference range
            # ------------------------------------------------
            #
            # Source reference range is retained but no longer
            # blocks the record.
            #
            # The standard reference is required because it is
            # the range used by derived_abnormal_flag.
            #
            # ------------------------------------------------

            F.when(
                F.col(
                    "_ref_lab_test_code"
                ).isNotNull()
                &
                (
                    F.col(
                        "standard_reference_low"
                    ).isNull()
                    |
                    F.col(
                        "standard_reference_high"
                    ).isNull()
                    |
                    (
                        F.col(
                            "standard_reference_low"
                        )
                        >
                        F.col(
                            "standard_reference_high"
                        )
                    )
                ),

                F.lit(
                    "invalid_standard_reference_range"
                )
            ),


            # ------------------------------------------------
            # Collection date
            # ------------------------------------------------

            F.when(
                F.col(
                    "collection_date"
                ).isNull(),

                F.lit(
                    "invalid_collection_date"
                )
            )
        )
    )


    # ========================================================
    # FINAL VALIDATED DATASET
    # ========================================================

    return (

        classified

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

            # ------------------------------------------------
            # Business identifiers
            # ------------------------------------------------

            "lab_result_id",
            "subject_id",
            "study_id",
            "visit_id",


            # ------------------------------------------------
            # Laboratory test
            # ------------------------------------------------

            "lab_test_code",
            "lab_test_name",


            # ------------------------------------------------
            # Source measurement
            # ------------------------------------------------

            "result_value",
            "result_unit",


            # ------------------------------------------------
            # Standardized measurement
            # ------------------------------------------------

            "standardized_result_value",
            "standard_unit",
            "conversion_factor",


            # ------------------------------------------------
            # Reference ranges
            # ------------------------------------------------

            "source_reference_low",
            "source_reference_high",

            "standard_reference_low",
            "standard_reference_high",


            # ------------------------------------------------
            # Abnormality
            # ------------------------------------------------

            "source_abnormal_flag",
            "derived_abnormal_flag",
            "abnormal_flag_discrepancy",


            # ------------------------------------------------
            # Laboratory event
            # ------------------------------------------------

            "collection_date",
            "lab_vendor",


            # ------------------------------------------------
            # Rescued source content
            # ------------------------------------------------

            "_rescued_data",


            # ------------------------------------------------
            # Operational lineage
            # ------------------------------------------------

            "_source_file",
            "_source_file_name",
            "_source_file_modification_ts",
            "_ingestion_ts",
            "_ingestion_date",


            # ------------------------------------------------
            # DQ metadata
            # ------------------------------------------------

            "_dq_failures",
            "_is_valid"
        )
    )


# ============================================================
# 2. VALID LAB RESULT STREAM
# ============================================================

@dp.temporary_view(
    name="v_lab_results_valid"
)
def v_lab_results_valid():

    return (

        spark.readStream
        .table(
            "v_lab_results_validated"
        )

        .where(
            F.col(
                "_is_valid"
            ) == True
        )
    )


# ============================================================
# 3. SILVER LAB RESULTS
# ============================================================

@dp.table(

    name="lab_results",

    comment=(
        "Validated and standardized Silver laboratory "
        "results from the clinical laboratory event feed."
    ),

    table_properties={
        "quality": "silver"
    }
)
def lab_results():

    return (

        spark.readStream
        .table(
            "v_lab_results_valid"
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
# 4. LAB RESULT QUARANTINE
# ============================================================

@dp.table(

    name=f"{CATALOG}.quarantine.lab_results",

    comment=(
        "Laboratory-result records rejected by Silver "
        "clinical data-quality validation."
    ),

    table_properties={
        "quality": "quarantine"
    }
)
def quarantine_lab_results():

    return (

        spark.readStream
        .table(
            "v_lab_results_validated"
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