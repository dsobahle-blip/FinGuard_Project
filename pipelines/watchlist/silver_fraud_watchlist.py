from pyspark import pipelines as dp
from pyspark.sql.functions import to_date, lit, col

"""
Silver layer: Cleaned and deduplicated fraud watchlist.
Reads from bronze_fraud_watchlist (batch), writes to finguard.silver.silver_fraud_watchlist.
"""


@dp.materialized_view(
    name="finguard.silver.silver_fraud_watchlist",
    comment="Deduplicated fraud watchlist with only high/critical severity entries",
)
def silver_fraud_watchlist():
    return (
        spark.read.table("bronze_fraud_watchlist")
        .dropDuplicates(["watchlist_id"])
        .withColumn("flagged_date", to_date(col("flagged_date")))
        .filter(col("severity").isin("high", "critical"))
        .withColumn("is_active", lit(True))
    )
