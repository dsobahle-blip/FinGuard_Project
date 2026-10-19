from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_transactions",
    comment="Parsed, validated, deduplicated credit card transactions from bronze",
)
@dp.expect_all_or_drop({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_amount": "amount > 0",
})
def silver_transactions():
    return (
        spark.readStream.table("finguard.bronze.bronze_transactions")
        .withColumn("txn_timestamp", F.to_timestamp("transaction_timestamp"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("is_high_value", F.col("amount") >= 50000)
        .withColumn("transaction_date", F.col("txn_timestamp").cast("date"))
        .withWatermark("_ingested_at", "10 minutes")
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select(
            "transaction_id",
            "customer_id",
            "card_number",
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "amount",
            "currency",
            "transaction_type",
            "payment_channel",
            "device_id",
            "city",
            "country",
            "is_international",
            "status",
            "txn_timestamp",
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",
            "is_high_value",
            "transaction_date",
        )
    )
