from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_safety_metrics",
    comment="Study-site AE monitoring by SOC, severity and seriousness. Operational monitoring only; not a medical conclusion or TEAE analysis."
)
def gold_safety_metrics():

    ae = spark.table(f"{CATALOG}.silver.adverse_events")
    spine = spark.table("gold_subject_spine")

    population = (
        spine
        .groupBy("study_id", "site_id")
        .agg(
            F.countDistinct("subject_id").alias("subjects_at_risk")
        )
    )

    events = (
        ae.groupBy(
            "study_id",
            "site_id",
            "meddra_soc",
            F.col("standard_severity").alias("ae_severity"),
            "serious_flag"
        )
        .agg(
            F.count("*").alias("ae_events"),
            F.countDistinct("subject_id").alias("subjects_with_ae"),

            F.sum(
                F.when(F.upper(F.col("outcome")) == "FATAL", 1).otherwise(0)
            ).alias("fatal_events"),

            F.sum(
                F.when(F.col("resolution_date").isNull(), 1).otherwise(0)
            ).alias("ongoing_events"),

            F.sum(
                F.when(F.col("resolution_date").isNotNull(), 1).otherwise(0)
            ).alias("resolved_events")
        )
    )

    return (
        events
        .join(population, ["study_id", "site_id"], "left")

        .withColumn(
            "ae_incidence_pct",
            F.when(
                F.col("subjects_at_risk") > 0,
                F.col("subjects_with_ae")
                / F.col("subjects_at_risk") * 100
            )
        )

        .withColumn("_gold_generated_at", F.current_timestamp())
    )