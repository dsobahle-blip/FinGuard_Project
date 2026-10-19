from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

"""
Bronze layer: Fraud watchlist ingestion via Auto Loader.
Reads JSON files from UC Volume, writes raw data to bronze_fraud_watchlist.
"""


@dp.table(
    name="bronze_fraud_watchlist",
    comment="Raw fraud watchlist entries ingested from JSON files via Auto Loader",
)
@dp.expect_or_drop("valid_watchlist_id", "watchlist_id IS NOT NULL")
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
def bronze_fraud_watchlist():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/finguard/bronze/raw_files/watchlist/")
        .withColumn("_ingested_at", current_timestamp())
    )
