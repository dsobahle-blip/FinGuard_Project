from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_customer_risk_summary",
    comment="Sliding 10-min window (2-min slide) per customer with velocity flags",
)
def gold_customer_risk_summary():
    enriched = spark.read.table("finguard.silver.silver_enriched_transactions")

    result = (
        enriched
        .withColumn(
            "base_2min",
            (F.unix_timestamp("txn_timestamp") - F.unix_timestamp("txn_timestamp") % 120)
            .cast("timestamp"),
        )
        .withColumn(
            "window_starts",
            F.expr("sequence(base_2min - INTERVAL 8 MINUTE, base_2min, INTERVAL 2 MINUTE)"),
        )
        .withColumn("window_start", F.explode("window_starts"))
        .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 10 MINUTE"))
        .filter(
            (F.col("txn_timestamp") >= F.col("window_start"))
            & (F.col("txn_timestamp") < F.col("window_end"))
        )
        .groupBy("window_start", "window_end", "customer_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.max("amount").alias("max_amount"),
        )
        .withColumn("is_velocity_flag", F.col("txn_count") >= 8)
    )

    return result
