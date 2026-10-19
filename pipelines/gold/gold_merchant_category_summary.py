from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_merchant_category_summary",
    comment="Aggregations by merchant category",
)
def gold_merchant_category_summary():
    silver = spark.read.table("finguard.silver.silver_transactions")

    result = (
        silver.groupBy("merchant_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
        )
    )

    return result
