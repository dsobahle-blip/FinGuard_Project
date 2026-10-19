from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_fraud_summary",
    comment="5-minute tumbling window fraud aggregations by merchant category",
)
def gold_fraud_summary():
    flagged = spark.read.table("gold_flagged_transactions")

    result = (
        flagged
        .withColumn("window_start",
            (F.unix_timestamp("txn_timestamp") - F.unix_timestamp("txn_timestamp") % 300)
            .cast("timestamp")
        )
        .withColumn("window_end",
            F.col("window_start") + F.expr("INTERVAL 5 MINUTE")
        )
        .groupBy("window_start", "window_end", "merchant_category")
        .agg(
            F.count("*").alias("flagged_count"),
            F.sum("amount").alias("flagged_amount"),
            F.sum(F.when(F.col("alert_severity") == "HIGH", 1).otherwise(0)).alias("high_severity_count"),
            F.sum(F.when(F.col("alert_severity") == "MEDIUM", 1).otherwise(0)).alias("medium_severity_count"),
            F.sum(F.when(F.col("alert_severity") == "LOW", 1).otherwise(0)).alias("low_severity_count"),
            F.avg("risk_score").alias("avg_risk_score"),
        )
    )

    return result
