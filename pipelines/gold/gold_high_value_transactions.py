from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_high_value_transactions",
    comment="High-value transactions (threshold: 50,000 INR or equivalent)",
)
def gold_high_value_transactions():
    silver = spark.read.table("finguard.silver.silver_transactions")

    result = (
        silver.filter(F.col("amount") >= 50000)
        .select(
            "transaction_id", "customer_id", "card_number", "merchant_name",
            "merchant_category", "amount", "currency", "payment_channel",
            "city", "country", "txn_timestamp", "status",
        )
    )

    return result
