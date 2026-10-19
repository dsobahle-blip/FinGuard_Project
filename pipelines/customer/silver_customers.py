"""
FinGuard SDP Pipeline #2: Customer Cleansing (Silver)

Creates a materialized view from bronze_customers that cleanses and enriches
customer data for fraud detection enrichment.

Source:  finguard.bronze.bronze_customers
Target:  finguard.silver.silver_customers (materialized view)

Note: The pipeline default schema is 'bronze'. This table uses a fully-qualified
name (finguard.silver.silver_customers) to publish to a different schema.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="finguard.silver.silver_customers",
    comment="Cleansed customer master data with derived columns for fraud detection",
)
def silver_customers():
    """Materialized view: cleanse and enrich customer data from bronze."""
    return (
        spark.read.table("finguard.bronze.bronze_customers")
        .select(
            F.col("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("phone"),
            F.col("address"),
            F.col("city"),
            F.col("country"),
            F.col("account_type"),
            F.to_date(F.col("registration_date")).alias("registration_date"),
            F.when(F.col("risk_profile").isNull(), F.lit("medium")).otherwise(F.col("risk_profile")).alias("risk_profile"),
            F.col("is_active"),
            # Derived columns
            F.concat_ws(" ", F.trim(F.col("first_name")), F.trim(F.col("last_name"))).alias("full_name"),
            F.datediff(F.current_date(), F.to_date(F.col("registration_date"))).alias("customer_age_days"),
            F.when(F.col("risk_profile") == "high", True).otherwise(False).alias("is_high_risk"),
        )
    )
