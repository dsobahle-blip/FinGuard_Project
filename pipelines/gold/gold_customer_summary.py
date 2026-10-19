from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_customer_summary",
    comment="Customer-level transaction aggregations",
)
def gold_customer_summary():
    silver = spark.read.table("finguard.silver.silver_transactions")

    result = (
        silver.groupBy("customer_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_spend"),
            F.avg("amount").alias("avg_transaction"),
            F.countDistinct("merchant_id").alias("distinct_merchants"),
            F.countDistinct("merchant_category").alias("distinct_categories"),
            F.max("txn_timestamp").alias("last_transaction"),
        )
    )

    return result
