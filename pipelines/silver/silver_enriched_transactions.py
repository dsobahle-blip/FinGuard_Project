from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_enriched_transactions",
    comment="Enriched transactions with customer data, unusual location and flagged status",
)
def silver_enriched_transactions():
    silver = spark.read.table("silver_transactions")

    customers = (
        spark.read.table("finguard.silver.silver_customers")
        .select(
            F.col("customer_id"),
            F.col("full_name").alias("customer_full_name"),
            F.col("risk_profile").alias("customer_risk_profile"),
            F.col("is_high_risk").alias("customer_is_high_risk"),
            F.col("account_type").alias("customer_account_type"),
            F.col("city").alias("customer_home_city"),
            F.col("country").alias("customer_home_country"),
        )
    )

    result = (
        silver
        .withColumnRenamed("city", "txn_city")
        .withColumnRenamed("country", "txn_country")
        .join(customers, "customer_id", "left")
        .withColumn(
            "is_unusual_location",
            F.when(
                (F.col("customer_home_city").isNotNull())
                & (F.col("txn_city") != F.col("customer_home_city")),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_flagged",
            F.col("transaction_id").startswith("TXF"),
        )
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
            "txn_city",
            "txn_country",
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
            "customer_full_name",
            "customer_risk_profile",
            "customer_is_high_risk",
            "customer_account_type",
            "customer_home_city",
            "customer_home_country",
            "is_unusual_location",
            "is_flagged",
        )
    )

    return result
