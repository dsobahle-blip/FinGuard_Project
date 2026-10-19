"""
FinGuard SDP Pipeline #1: Kafka -> Bronze Transactions

Streams credit card transactions from Confluent Cloud Kafka, parses the JSON
payload, and writes to the bronze_transactions streaming table with data quality
expectations. Raw Kafka metadata columns are preserved alongside parsed fields.

Source:  Kafka topic 'credit_card_transactions' on Confluent Cloud
Target:  finguard.bronze.bronze_transactions (streaming table)
"""

import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

# ==========================================
# Kafka credentials from secret scope
# ==========================================
_kafka_secret = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
_kafka_config = json.loads(_kafka_secret)

_bootstrap_servers = _kafka_config["bootstrap_server"]
_topic_name = _kafka_config["topic_name"]
_api_key = _kafka_config["api_key"]
_api_secret = _kafka_config["api_secret"]

_jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="{_api_key}" password="{_api_secret}";'
)

# ==========================================
# Transaction JSON schema (Kafka message value)
# ==========================================
_transaction_schema = StructType([
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


@dp.table(
    name="bronze_transactions",
    comment="Raw credit card transactions from Confluent Cloud Kafka with parsed JSON fields and Kafka metadata",
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def bronze_transactions():
    """Stream raw transactions from Kafka, parse JSON payload, preserve metadata."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", _bootstrap_servers)
        .option("subscribe", _topic_name)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", _jaas_config)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            # Parsed transaction fields
            F.from_json(
                F.col("value").cast("string"),
                _transaction_schema,
            ).alias("parsed"),
            # Raw Kafka metadata
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .select("parsed.*", "kafka_key", "raw_value", "kafka_topic",
                "kafka_partition", "kafka_offset", "kafka_timestamp",
                "kafka_timestamp_type", "_ingested_at")
    )
