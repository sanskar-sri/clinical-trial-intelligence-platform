from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


# ============================================================
# GOLD DATA QUALITY METRICS
# ============================================================

@dp.materialized_view(
    name="gold_data_quality_metrics",
    comment=(
        "Gold DQ failures by entity, study, site and rule. "
        "One record may fail multiple rules; therefore failure "
        "counts are not additive across dq_rule."
    )
)
def gold_data_quality_metrics():

    # --------------------------------------------------------
    # Current trusted subject -> site mapping
    #
    # Used when a quarantine entity such as lab_results does
    # not physically contain site_id.
    # --------------------------------------------------------

    subject_sites = (
        spark.table(
            f"{CATALOG}.silver.subjects"
        )
        .where(
            F.col("__END_AT").isNull()
        )
        .select(
            F.col("subject_id").alias("_subject_id"),
            F.col("site_id").alias("_subject_site_id")
        )
        .dropDuplicates(
            ["_subject_id"]
        )
    )

    # ========================================================
    # 1. SUBJECT QUARANTINE
    # ========================================================

    q_subjects_raw = spark.table(
        f"{CATALOG}.quarantine.subjects"
    )

    q_subjects = (
        q_subjects_raw
        .select(
            F.lit("subjects").alias("source_entity"),

            F.coalesce(
                F.col("study_id"),
                F.lit("UNKNOWN")
            ).alias("study_id"),

            F.coalesce(
                F.col("site_id"),
                F.lit("UNKNOWN")
            ).alias("site_id"),

            F.col("subject_id")
                .cast("string")
                .alias("_record_id"),

            F.explode("_dq_failures")
                .alias("dq_rule")
        )
    )

    # ========================================================
    # 2. VISIT QUARANTINE
    # ========================================================

    q_visits_raw = spark.table(
        f"{CATALOG}.quarantine.visits"
    )

    q_visits = (
        q_visits_raw
        .select(
            F.lit("visits").alias("source_entity"),

            F.coalesce(
                F.col("study_id"),
                F.lit("UNKNOWN")
            ).alias("study_id"),

            F.coalesce(
                F.col("site_id"),
                F.lit("UNKNOWN")
            ).alias("site_id"),

            F.col("visit_id")
                .cast("string")
                .alias("_record_id"),

            F.explode("_dq_failures")
                .alias("dq_rule")
        )
    )

    # ========================================================
    # 3. LAB QUARANTINE
    #
    # quarantine.lab_results does NOT contain site_id.
    # Derive it through subject_id -> current Silver subject.
    # ========================================================

    q_labs_raw = (
        spark.table(
            f"{CATALOG}.quarantine.lab_results"
        )
        .alias("q")
    )

    q_labs = (
        q_labs_raw
        .join(
            subject_sites.alias("s"),
            F.col("q.subject_id")
            == F.col("s._subject_id"),
            "left"
        )
        .select(
            F.lit("lab_results")
                .alias("source_entity"),

            F.coalesce(
                F.col("q.study_id"),
                F.lit("UNKNOWN")
            ).alias("study_id"),

            F.coalesce(
                F.col("s._subject_site_id"),
                F.lit("UNKNOWN")
            ).alias("site_id"),

            F.col("q.lab_result_id")
                .cast("string")
                .alias("_record_id"),

            F.explode(
                F.col("q._dq_failures")
            ).alias("dq_rule")
        )
    )

    # ========================================================
    # 4. ADVERSE EVENT QUARANTINE
    # ========================================================

    q_ae_raw = spark.table(
        f"{CATALOG}.quarantine.adverse_events"
    )

    q_ae = (
        q_ae_raw
        .select(
            F.lit("adverse_events")
                .alias("source_entity"),

            F.coalesce(
                F.col("study_id"),
                F.lit("UNKNOWN")
            ).alias("study_id"),

            F.coalesce(
                F.col("site_id"),
                F.lit("UNKNOWN")
            ).alias("site_id"),

            F.col("ae_id")
                .cast("string")
                .alias("_record_id"),

            F.explode("_dq_failures")
                .alias("dq_rule")
        )
    )

    # ========================================================
    # 5. STANDARDIZED UNION
    #
    # Every dataframe now has exactly:
    # source_entity
    # study_id
    # site_id
    # _record_id
    # dq_rule
    # ========================================================

    all_q = (
        q_subjects
        .unionByName(q_visits)
        .unionByName(q_labs)
        .unionByName(q_ae)
    )

    # ========================================================
    # 6. DQ AGGREGATION
    # ========================================================

    return (
        all_q
        .groupBy(
            "source_entity",
            "study_id",
            "site_id",
            "dq_rule"
        )
        .agg(
            F.count("*")
                .alias("failure_count"),

            F.countDistinct("_record_id")
                .alias("affected_records")
        )
        .withColumn(
            "_gold_generated_at",
            F.current_timestamp()
        )
    )


# ============================================================
# GOLD RECONCILIATION
# ============================================================

@dp.materialized_view(
    name="gold_reconciliation",
    comment=(
        "Automated Bronze-Silver-quarantine reconciliation "
        "by clinical source entity."
    )
)
def gold_reconciliation():

    mappings = [
        ("subjects", "edc_subjects"),
        ("visits", "edc_visits"),
        ("lab_results", "lab_results"),
        ("adverse_events", "safety_adverse_events")
    ]

    rows = []

    for entity, bronze_table in mappings:

        bronze_count = (
            spark.table(
                f"{CATALOG}.bronze.{bronze_table}"
            )
            .agg(
                F.count("*")
                    .alias("bronze_records")
            )
            .withColumn(
                "source_entity",
                F.lit(entity)
            )
        )

        silver_count = (
            spark.table(
                f"{CATALOG}.silver.{entity}"
            )
            .agg(
                F.count("*")
                    .alias("silver_records")
            )
            .withColumn(
                "source_entity",
                F.lit(entity)
            )
        )

        quarantine_count = (
            spark.table(
                f"{CATALOG}.quarantine.{entity}"
            )
            .agg(
                F.count("*")
                    .alias("quarantined_records")
            )
            .withColumn(
                "source_entity",
                F.lit(entity)
            )
        )

        row = (
            bronze_count
            .join(
                silver_count,
                "source_entity"
            )
            .join(
                quarantine_count,
                "source_entity"
            )
        )

        rows.append(row)

    result = rows[0]

    for row in rows[1:]:
        result = result.unionByName(row)

    return (
        result
        .withColumn(
            "quarantine_rate",
            F.when(
                F.col("bronze_records") > 0,
                F.col("quarantined_records")
                / F.col("bronze_records")
            )
        )
        .withColumn(
            "reconciled_flag",
            F.col("bronze_records")
            ==
            (
                F.col("silver_records")
                + F.col("quarantined_records")
            )
        )
        .withColumn(
            "_gold_generated_at",
            F.current_timestamp()
        )
    )