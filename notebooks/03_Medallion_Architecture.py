# Databricks notebook source
# DBTITLE 1,Medallion Architecture Title
# MAGIC %md
# MAGIC # Medallion Architecture - Credit Card Transactions
# MAGIC
# MAGIC Bronze → Silver → Gold pipeline for real-time credit card transaction processing from Kafka. Credentials are loaded from the `finguard-scope` secret scope.

# COMMAND ----------

# DBTITLE 1,Load Kafka Config from Secret Scope
import json

# ==========================================
# Load Kafka connection details from secret scope
# ==========================================
kafka_secret = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
kafka_config = json.loads(kafka_secret)

bootstrap_servers = kafka_config["bootstrap_server"]
topic_name = kafka_config["topic_name"]
api_key = kafka_config["api_key"]
api_secret = kafka_config["api_secret"]

jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="{api_key}" password="{api_secret}";'
)

print(f"Loaded Kafka config for topic: {topic_name}")

# COMMAND ----------

# DBTITLE 1,Setup Catalogs and Transaction Schema
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

# ==========================================
# Create catalogs and schemas if they don't exist
# ==========================================
spark.sql("CREATE CATALOG IF NOT EXISTS finguard")
spark.sql("CREATE DATABASE IF NOT EXISTS finguard.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS finguard.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS finguard.gold")

# ==========================================
# Define the transaction JSON schema (from Kafka message value)
# ==========================================
transaction_schema = StructType([
    StructField("transaction_id",       StringType(),  True),
    StructField("customer_id",          StringType(),  True),
    StructField("card_number",           StringType(),  True),
    StructField("merchant_id",           StringType(),  True),
    StructField("merchant_name",         StringType(),  True),
    StructField("merchant_category",     StringType(),  True),
    StructField("amount",                DoubleType(),  True),
    StructField("currency",              StringType(),  True),
    StructField("transaction_type",      StringType(),  True),
    StructField("payment_channel",       StringType(),  True),
    StructField("device_id",             StringType(),  True),
    StructField("city",                  StringType(),  True),
    StructField("country",               StringType(),  True),
    StructField("transaction_timestamp", StringType(),  True),
    StructField("is_international",      BooleanType(), True),
    StructField("status",                StringType(),  True),
])

print("Catalogs, schemas, and transaction schema ready.")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Stream Raw Kafka to Delta
from pyspark.sql.functions import col

# ==========================================
# BRONZE LAYER: Raw ingestion from Kafka
# ==========================================
# Reads raw messages from Kafka and writes them as-is to a Delta table.
# Uses trigger(availableNow=True) for a bounded one-time drain.

bronze_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_name)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("startingOffsets", "earliest")
    .load()
)

# Select raw Kafka columns as strings
bronze_parsed = bronze_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
)

# Write to bronze Delta table with checkpointing
bronze_query = (
    bronze_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/finguard/source/transactions/bronze_checkpoint/")
    .trigger(availableNow=True)
    .toTable("finguard.bronze.bronze_transactions")
)

bronze_query.awaitTermination()
print(f"Bronze layer ingestion complete. Query ID: {bronze_query.id}")

# COMMAND ----------

# DBTITLE 1,Bronze Verification
# ==========================================
# Verify Bronze layer
# ==========================================
bronze_count = spark.read.table("finguard.bronze.bronze_transactions").count()
print(f"Bronze table row count: {bronze_count}")
display(spark.read.table("finguard.bronze.bronze_transactions").limit(5))

# COMMAND ----------

# DBTITLE 1,Silver Layer - Parse, Validate, Deduplicate
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, row_number
)
from pyspark.sql.window import Window

# ==========================================
# SILVER LAYER: Parse JSON, validate, deduplicate
# ==========================================
# Reads from bronze, parses the JSON value column into structured columns,
# applies data quality checks, and deduplicates by transaction_id.

bronze_data = spark.read.table("finguard.bronze.bronze_transactions")

# Parse JSON value column into structured columns
silver_df = (
    bronze_data
    .withColumn("parsed", from_json(col("value"), transaction_schema))
    .select(
        col("parsed.transaction_id"),
        col("parsed.customer_id"),
        col("parsed.card_number"),
        col("parsed.merchant_id"),
        col("parsed.merchant_name"),
        col("parsed.merchant_category"),
        col("parsed.amount"),
        col("parsed.currency"),
        col("parsed.transaction_type"),
        col("parsed.payment_channel"),
        col("parsed.device_id"),
        col("parsed.city"),
        col("parsed.country"),
        col("parsed.is_international"),
        col("parsed.status"),
        to_timestamp(col("parsed.transaction_timestamp")).alias("txn_timestamp"),
        col("key").alias("kafka_key"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp"),
    )
)

# Data quality: filter out invalid records
silver_clean = silver_df.filter(
    col("transaction_id").isNotNull()
    & (col("amount") > 0)
    & col("customer_id").isNotNull()
)

# Deduplicate by transaction_id (keep the record with the highest kafka_offset)
dedup_window = Window.partitionBy("transaction_id").orderBy(col("kafka_offset").desc())
silver_deduped = (
    silver_clean
    .withColumn("row_num", row_number().over(dedup_window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# Write to silver Delta table
silver_deduped.write.mode("overwrite").saveAsTable("finguard.silver.silver_transactions")
print(f"Silver layer complete. Row count: {silver_deduped.count()}")

# COMMAND ----------

# DBTITLE 1,Silver Verification
# ==========================================
# Verify Silver layer
# ==========================================
print(f"Silver table row count: {spark.read.table('finguard.silver.silver_transactions').count()}")
display(spark.read.table("finguard.silver.silver_transactions").limit(5))

# COMMAND ----------

# DBTITLE 1,Gold Layer - Aggregated Analytics Tables
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max,
    countDistinct, date_format
)

# ==========================================
# GOLD LAYER: Aggregated analytics tables
# ==========================================

silver = spark.read.table("finguard.silver.silver_transactions")

# ------------------------------------------
# Gold 1: Merchant Category Summary
# ------------------------------------------
gold_merchant = (
    silver.groupBy("merchant_category")
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        _max("amount").alias("max_amount"),
    )
    .orderBy(col("total_amount").desc())
)
gold_merchant.write.mode("overwrite").saveAsTable("finguard.gold.gold_merchant_category_summary")
print("Gold: merchant_category_summary written.")

# ------------------------------------------
# Gold 2: Customer Transaction Summary
# ------------------------------------------
gold_customer = (
    silver.groupBy("customer_id")
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_spend"),
        avg("amount").alias("avg_transaction"),
        countDistinct("merchant_id").alias("distinct_merchants"),
        countDistinct("merchant_category").alias("distinct_categories"),
        _max("txn_timestamp").alias("last_transaction"),
    )
    .orderBy(col("total_spend").desc())
)
gold_customer.write.mode("overwrite").saveAsTable("finguard.gold.gold_customer_summary")
print("Gold: customer_summary written.")

# ------------------------------------------
# Gold 3: Hourly Transaction Volume
# ------------------------------------------
gold_hourly = (
    silver.groupBy(date_format(col("txn_timestamp"), "yyyy-MM-dd HH:00").alias("hour_bucket"))
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
    )
    .orderBy("hour_bucket")
)
gold_hourly.write.mode("overwrite").saveAsTable("finguard.gold.gold_hourly_volume")
print("Gold: hourly_volume written.")

# ------------------------------------------
# Gold 4: High-Value / Fraud Candidate Transactions
# ------------------------------------------
HIGH_VALUE_THRESHOLD = 50000  # INR

gold_high_value = (
    silver.filter(col("amount") >= HIGH_VALUE_THRESHOLD)
    .select(
        "transaction_id", "customer_id", "card_number", "merchant_name",
        "merchant_category", "amount", "currency", "payment_channel",
        "city", "country", "txn_timestamp", "status",
    )
    .orderBy(col("amount").desc())
)
gold_high_value.write.mode("overwrite").saveAsTable("finguard.gold.gold_high_value_transactions")
print("Gold: high_value_transactions written.")

# COMMAND ----------

# DBTITLE 1,Gold Verification - Summary of All Tables
# ==========================================
# Verify all Gold tables
# ==========================================

gold_tables = [
    "finguard.gold.gold_merchant_category_summary",
    "finguard.gold.gold_customer_summary",
    "finguard.gold.gold_hourly_volume",
    "finguard.gold.gold_high_value_transactions",
]

for table_name in gold_tables:
    row_count = spark.read.table(table_name).count()
    print(f"{table_name}: {row_count} rows")

print("\n--- Merchant Category Summary (top 5) ---")
display(spark.read.table("finguard.gold.gold_merchant_category_summary").limit(5))

print("\n--- High-Value Transactions (top 5) ---")
display(spark.read.table("finguard.gold.gold_high_value_transactions").limit(5))

# COMMAND ----------

# DBTITLE 1,Gold Layer - Spec Fraud Detection Tables
from pyspark.sql.functions import (
    col, when, lit, concat_ws, current_timestamp, date_format,
    count, sum as _sum
)

# ==========================================
# GOLD LAYER: Spec-defined fraud detection tables
# ==========================================
# These tables implement the fraud detection rules from the project spec.
# They are distinct from the analytics gold tables above.

silver = spark.read.table("finguard.silver.silver_transactions")

HIGH_VALUE_THRESHOLD = 80000  # INR — p90 of transaction amounts
HIGH_RISK_CATEGORIES = ["Jewellery", "Electronics", "Travel"]

# ------------------------------------------
# Gold 5: Flagged Transactions (Fraud Detection)
# ------------------------------------------
# Applies rule-based fraud detection with weighted risk scoring.
# Rules:
#   1. HIGH_VALUE        — amount >= 80000              → +40 points
#   2. INTERNATIONAL      — is_international = true        → +30 points
#   3. HIGH_RISK_CATEGORY — Jewellery/Electronics/Travel  → +20 points
#   4. ONLINE_HIGH_VALUE  — ONLINE type + amount >= 80000  → +10 points
# Severity: HIGH (>=70), MEDIUM (>=40), LOW (>=20)

flagged = (
    silver
    .withColumn("high_value", when(col("amount") >= HIGH_VALUE_THRESHOLD, 1).otherwise(0))
    .withColumn("international", when(col("is_international") == True, 1).otherwise(0))
    .withColumn("high_risk_cat", when(col("merchant_category").isin(HIGH_RISK_CATEGORIES), 1).otherwise(0))
    .withColumn("online_hv", when((col("transaction_type") == "ONLINE") & (col("amount") >= HIGH_VALUE_THRESHOLD), 1).otherwise(0))
    .withColumn("risk_score",
        col("high_value") * 40
        + col("international") * 30
        + col("high_risk_cat") * 20
        + col("online_hv") * 10
    )
    .filter(col("risk_score") > 0)
    .withColumn("fraud_reason", concat_ws(" | ",
        when(col("high_value") == 1, lit("HIGH_VALUE")).otherwise(lit(None)),
        when(col("international") == 1, lit("INTERNATIONAL")).otherwise(lit(None)),
        when(col("high_risk_cat") == 1, lit("HIGH_RISK_CATEGORY")).otherwise(lit(None)),
        when(col("online_hv") == 1, lit("ONLINE_HIGH_VALUE")).otherwise(lit(None)),
    ))
    .withColumn("alert_severity",
        when(col("risk_score") >= 70, lit("HIGH"))
        .when(col("risk_score") >= 40, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
    .withColumn("flagged_at", current_timestamp())
    .select(
        "transaction_id", "customer_id", "card_number", "merchant_name",
        "merchant_category", "amount", "currency", "transaction_type",
        "payment_channel", "city", "country", "is_international",
        "txn_timestamp", "status", "fraud_reason", "risk_score",
        "alert_severity", "flagged_at",
    )
    .orderBy(col("risk_score").desc())
)

flagged.write.mode("overwrite").saveAsTable("finguard.gold.gold_flagged_transactions")
print(f"Gold 5: gold_flagged_transactions — {flagged.count()} flagged transactions.")

# ------------------------------------------
# Gold 6: Fraud Summary by Hour
# ------------------------------------------
flagged_data = spark.read.table("finguard.gold.gold_flagged_transactions")

fraud_summary = (
    flagged_data
    .groupBy(date_format(col("txn_timestamp"), "yyyy-MM-dd HH:00").alias("hour_bucket"))
    .agg(
        count("*").alias("flagged_count"),
        _sum("amount").alias("flagged_amount"),
        _sum(when(col("alert_severity") == "HIGH", 1).otherwise(0)).alias("high_severity_count"),
        _sum(when(col("alert_severity") == "MEDIUM", 1).otherwise(0)).alias("medium_severity_count"),
        _sum(when(col("alert_severity") == "LOW", 1).otherwise(0)).alias("low_severity_count"),
    )
    .orderBy("hour_bucket")
)

fraud_summary.write.mode("overwrite").saveAsTable("finguard.gold.gold_fraud_summary")
print(f"Gold 6: gold_fraud_summary — {fraud_summary.count()} hour buckets.")

# ------------------------------------------
# Gold 7: Alert Log (schema only — populated by alert pipeline)
# ------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS finguard.gold.gold_alert_log (
    alert_id STRING,
    transaction_id STRING,
    customer_id STRING,
    amount DOUBLE,
    risk_score DOUBLE,
    alert_severity STRING,
    fraud_reason STRING,
    alert_status STRING,
    sent_at TIMESTAMP,
    error_message STRING
)
""")

print("Gold 7: gold_alert_log table created (empty — will be populated by alert pipeline).")

# COMMAND ----------

# DBTITLE 1,Spec Gold Tables Verification
from pyspark.sql.functions import col

# ==========================================
# Verify spec gold fraud detection tables
# ==========================================

print("=== Gold Flagged Transactions ===")
flagged_count = spark.read.table("finguard.gold.gold_flagged_transactions").count()
print(f"Total flagged: {flagged_count}")

print("\nBy severity:")
spark.read.table("finguard.gold.gold_flagged_transactions").groupBy("alert_severity").count().orderBy(col("alert_severity").desc()).show()

print("Top 5 highest risk:")
display(spark.read.table("finguard.gold.gold_flagged_transactions").select(
    "transaction_id", "customer_id", "amount", "risk_score", "alert_severity", "fraud_reason"
).limit(5))

print("\n=== Gold Fraud Summary ===")
display(spark.read.table("finguard.gold.gold_fraud_summary"))

print("\n=== Gold Alert Log ===")
alert_count = spark.read.table("finguard.gold.gold_alert_log").count()
print(f"Alert log rows: {alert_count} (empty — populated by alert pipeline)")