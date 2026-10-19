from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold_flagged_transactions",
    comment="Flagged transactions with 8-rule weighted fraud scoring (risk_score >= 60)",
)
def gold_flagged_transactions():
    enriched = spark.read.table("finguard.silver.silver_enriched_transactions")
    watchlist = spark.read.table("finguard.silver.silver_fraud_watchlist")

    # Watchlist lookups
    wl_customers = watchlist.filter(F.col("entity_type") == "CUSTOMER").select(
        F.col("entity_id").alias("wl_customer_id")
    )
    wl_merchants = watchlist.filter(F.col("entity_type") == "MERCHANT").select(
        F.col("entity_id").alias("wl_merchant_id")
    )

    # Velocity: count txns in past 10 minutes per customer
    vel_window = Window.partitionBy("customer_id").orderBy(
        F.col("txn_timestamp").cast("long")
    ).rangeBetween(-600, 0)

    result = (
        enriched
        .join(wl_customers, F.col("customer_id") == F.col("wl_customer_id"), "left")
        .join(wl_merchants, F.col("merchant_id") == F.col("wl_merchant_id"), "left")
        # Rule 1: HIGH_VALUE (per-currency, +40)
        .withColumn("high_value_score",
            F.when(
                (F.col("currency") == "INR") & (F.col("amount") >= 95000), 40)
                .when((F.col("currency") == "USD") & (F.col("amount") >= 1200), 40)
                .when((F.col("currency") == "EUR") & (F.col("amount") >= 1100), 40)
                .otherwise(0)
        )
        # Rule 2: INTERNATIONAL (+30)
        .withColumn("international_score",
            F.when(F.col("is_international") == True, 30).otherwise(0))
        # Rule 3: HIGH_RISK_CATEGORY (+20)
        .withColumn("high_risk_cat_score",
            F.when(F.col("merchant_category").isin("Jewellery", "Electronics", "Travel"), 20)
            .otherwise(0))
        # Rule 4: ONLINE_HIGH_VALUE (+10)
        .withColumn("online_hv_score",
            F.when(
                (F.col("transaction_type") == "ONLINE") &
                (((F.col("currency") == "INR") & (F.col("amount") >= 95000)) |
                 ((F.col("currency") == "USD") & (F.col("amount") >= 1200)) |
                 ((F.col("currency") == "EUR") & (F.col("amount") >= 1100))),
                10
            ).otherwise(0))
        # Rule 5: VELOCITY (>=8 txns/10min, +50)
        .withColumn("velocity_count", F.count("*").over(vel_window))
        .withColumn("velocity_score",
            F.when(F.col("velocity_count") >= 8, 50).otherwise(0))
        # Rule 6: UNUSUAL_LOCATION (+15)
        .withColumn("unusual_location_score",
            F.when(F.col("is_unusual_location") == True, 15).otherwise(0))
        # Rule 7: CUSTOMER_HIGH_RISK (+30)
        .withColumn("customer_high_risk_score",
            F.when(F.col("customer_risk_profile") == "high", 30).otherwise(0))
        # Rule 8: WATCHLIST_MATCH (+60)
        .withColumn("watchlist_match_score",
            F.when(
                F.col("wl_customer_id").isNotNull() | F.col("wl_merchant_id").isNotNull(), 60
            ).otherwise(0))
        # Compute total risk score
        .withColumn("risk_score",
            F.col("high_value_score") + F.col("international_score") +
            F.col("high_risk_cat_score") + F.col("online_hv_score") +
            F.col("velocity_score") + F.col("unusual_location_score") +
            F.col("customer_high_risk_score") + F.col("watchlist_match_score")
        )
        .filter(F.col("risk_score") >= 60)
        .withColumn("fraud_reason", F.concat_ws(" | ",
            F.when(F.col("high_value_score") > 0, F.lit("HIGH_VALUE")),
            F.when(F.col("international_score") > 0, F.lit("INTERNATIONAL")),
            F.when(F.col("high_risk_cat_score") > 0, F.lit("HIGH_RISK_CATEGORY")),
            F.when(F.col("online_hv_score") > 0, F.lit("ONLINE_HIGH_VALUE")),
            F.when(F.col("velocity_score") > 0, F.lit("VELOCITY")),
            F.when(F.col("unusual_location_score") > 0, F.lit("UNUSUAL_LOCATION")),
            F.when(F.col("customer_high_risk_score") > 0, F.lit("CUSTOMER_HIGH_RISK")),
            F.when(F.col("watchlist_match_score") > 0, F.lit("WATCHLIST_MATCH")),
        ))
        .withColumn("alert_severity",
            F.when(F.col("risk_score") >= 80, F.lit("HIGH"))
            .when(F.col("risk_score") >= 60, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )
        .withColumn("fraud_status", F.lit("SUSPECTED"))
        .withColumn("flagged_at", F.current_timestamp())
        .select(
            "transaction_id", "customer_id", "card_number", "merchant_name",
            "merchant_category", "amount", "currency", "transaction_type",
            "payment_channel", "txn_city", "txn_country", "is_international",
            "txn_timestamp", "status", "fraud_reason", "risk_score",
            "alert_severity", "fraud_status", "flagged_at",
        )
    )

    return result
