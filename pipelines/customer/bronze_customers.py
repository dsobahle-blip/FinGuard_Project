"""
FinGuard SDP Pipeline #2: Customer Master Data Ingestion (Bronze)

Uses Auto Loader to incrementally ingest customer JSON files from a UC volume,
applies data quality expectations, and writes to the bronze_customers streaming table.

Source:  /Volumes/finguard/bronze/raw_files/customers/*.json
Target:  finguard.bronze.bronze_customers (streaming table)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

# ==========================================
# Customer JSON schema (12 fields)
# ==========================================
_customer_schema = StructType([
    StructField("customer_id",       StringType(),  True),
    StructField("first_name",        StringType(),  True),
    StructField("last_name",         StringType(),  True),
    StructField("email",             StringType(),  True),
    StructField("phone",             StringType(),  True),
    StructField("address",           StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("account_type",      StringType(),  True),
    StructField("registration_date",  StringType(),  True),
    StructField("risk_profile",      StringType(),  True),
    StructField("is_active",         BooleanType(), True),
])


@dp.table(
    name="bronze_customers",
    comment="Raw customer master data ingested from JSON files via Auto Loader",
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_email", "email IS NOT NULL")
def bronze_customers():
    """Auto Loader streaming table for customer JSON files."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/finguard/bronze/raw_files/customers/schema/")
        .option("path", "/Volumes/finguard/bronze/raw_files/customers/")
        .schema(_customer_schema)
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
    )
