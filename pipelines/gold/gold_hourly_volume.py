from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_hourly_volume",
    comment="Hourly transaction volume metrics",
)
def gold_hourly_volume():
    silver = spark.read.table("finguard.silver.silver_transactions")

    result = (
        silver.withColumn(
            "hour_bucket",
            F.date_format(F.col("txn_timestamp"), "yyyy-MM-dd HH:00"),
        )
        .groupBy("hour_bucket")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
    )

    return result
