from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_hourly_fraud_rate",
    comment="Hourly fraud rate: flagged vs total transactions",
)
def gold_hourly_fraud_rate():
    silver = spark.read.table("finguard.silver.silver_transactions")
    flagged = spark.read.table("gold_flagged_transactions")

    total = (
        silver
        .withColumn("window_start", F.date_trunc("hour", F.col("txn_timestamp")))
        .groupBy("window_start")
        .agg(F.count("*").alias("total_transactions"))
    )

    flagged_count = (
        flagged
        .withColumn("window_start", F.date_trunc("hour", F.col("txn_timestamp")))
        .groupBy("window_start")
        .agg(F.count("*").alias("flagged_transactions"))
    )

    result = (
        total.join(flagged_count, "window_start", "left")
        .withColumn("flagged_transactions",
            F.coalesce(F.col("flagged_transactions"), F.lit(0))
        )
        .withColumn("fraud_rate",
            F.col("flagged_transactions") / F.col("total_transactions")
        )
        .withColumn("window_end",
            F.col("window_start") + F.expr("INTERVAL 1 HOUR")
        )
        .select("window_start", "window_end", "total_transactions",
                "flagged_transactions", "fraud_rate")
    )

    return result
