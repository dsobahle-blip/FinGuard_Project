# Databricks notebook source
# DBTITLE 1,Architecture Trace Title
# MAGIC %md
# MAGIC # FinGuard Architecture Trace — Source to Gold
# MAGIC
# MAGIC This notebook traces the complete data pipeline from Kafka source through to gold analytics and alerting. It documents every table, every notebook, and every data flow in the FinGuard fraud detection system.

# COMMAND ----------

# DBTITLE 1,Table Inventory Header
# MAGIC %md
# MAGIC ## 1. Complete Table Inventory
# MAGIC
# MAGIC All tables across the FinGuard medallion architecture — bronze (raw), silver (cleansed + enriched), and gold (analytics + fraud detection).

# COMMAND ----------

# DBTITLE 1,Full Table Inventory
# ==========================================
# 1. Full Table Inventory — every table in the medallion architecture
# ==========================================
from pyspark.sql.functions import col

print("=" * 75)
print("   FinGuard Full Table Inventory")
print("=" * 75)

inventory = {
    "BRONZE (Raw)": [
        ("finguard.bronze.bronze_transactions", "Kafka streaming ingestion (SDP)"),
        ("finguard.bronze.bronze_transactions_manual", "Kafka streaming ingestion (manual)"),
        ("finguard.bronze.bronze_customers", "Customer master data (SDP source)"),
        ("finguard.bronze.bronze_customers_manual", "Customer master data (manual)"),
        ("finguard.bronze.bronze_customers_pg", "Supabase Postgres customers (federation)"),
        ("finguard.bronze.bronze_fraud_watchlist", "Raw watchlist JSON"),
        ("finguard.bronze.bronze_fraud_watchlist_manual", "Raw watchlist JSON (manual)"),
        ("finguard.bronze.transactions_batch_test", "Batch Kafka test (legacy)"),
        ("finguard.bronze.transactions_streaming_test", "Streaming Kafka test (legacy)"),
    ],
    "SILVER (Cleansed + Enriched)": [
        ("finguard.silver.silver_transactions", "Parsed, validated, deduplicated"),
        ("finguard.silver.silver_transactions_manual", "Parsed, validated, deduplicated (manual)"),
        ("finguard.silver.silver_customers", "Cleansed customer master (SDP)"),
        ("finguard.silver.silver_customers_manual", "Cleansed customer master (manual)"),
        ("finguard.silver.silver_enriched_transactions", "Stream-static join + enrichment"),
        ("finguard.silver.silver_enriched_transactions_manual", "Stream-static join (manual)"),
        ("finguard.silver.silver_fraud_watchlist", "Watchlist (high/critical)"),
        ("finguard.silver.silver_fraud_watchlist_manual", "Watchlist (high/critical, manual)"),
    ],
    "GOLD (Analytics + Fraud Detection)": [
        ("finguard.gold.gold_flagged_transactions", "8-rule fraud detection"),
        ("finguard.gold.gold_fraud_summary", "5-min window + merchant_category"),
        ("finguard.gold.gold_hourly_fraud_rate", "Hourly fraud rate trend"),
        ("finguard.gold.gold_customer_risk_summary", "Sliding 10-min customer velocity"),
        ("finguard.gold.gold_merchant_category_summary", "Category aggregation"),
        ("finguard.gold.gold_customer_summary", "Customer aggregation"),
        ("finguard.gold.gold_hourly_volume", "Hourly volume trend"),
        ("finguard.gold.gold_high_value_transactions", "High-value transactions (>50K INR)"),
        ("finguard.gold.gold_alert_log", "Email alerts (alert pipeline)"),
    ],
}

total_found, total_missing = 0, 0
for layer_name, tables in inventory.items():
    print(f"\n{'─' * 75}")
    print(f"  {layer_name}")
    print(f"{'─' * 75}")
    for full_name, description in tables:
        try:
            df = spark.read.table(full_name)
            cnt = df.count()
            cols = len(df.columns)
            print(f"  ✅ {full_name:55s} {cnt:>8,} rows  ({cols} cols) — {description}")
            total_found += 1
        except Exception as e:
            print(f"  ❌ {full_name:55s} ERROR: {str(e)[:50]}")
            total_missing += 1

print(f"\n{'=' * 75}")
print(f"   Inventory complete: {total_found} tables found, {total_missing} missing/errored")
print(f"{'=' * 75}")

# COMMAND ----------

# DBTITLE 1,Data Flow Architecture
# MAGIC %md
# MAGIC ## 2. End-to-End Data Flow Architecture
# MAGIC
# MAGIC ### Source Layer
# MAGIC ```
# MAGIC Confluent Cloud Kafka (pkc-619z3.us-east1.gcp.confluent.cloud:9092)
# MAGIC   └─ Topic: credit_card_transactions
# MAGIC      └─ ~29,900 JSON messages (transaction_id, customer_id, amount, currency, ...)
# MAGIC      └─ Producer: Python Kafka producer (not in repo — external)
# MAGIC ```
# MAGIC
# MAGIC ### Secret Management
# MAGIC ```
# MAGIC Databricks Secret Scope: finguard-scope (DATABRICKS-backed)
# MAGIC   ├─ kafka_connection_details (JSON: bootstrap_server, topic_name, api_key, api_secret)
# MAGIC   ├─ gmail_api_key (✅ real Gmail App Password — configured Oct 16 2026)
# MAGIC   ├─ gmail_sender_email (✅ dsobahle@gmail.com)
# MAGIC   ├─ gmail_recipient_email (✅ dsobahle@gmail.com)
# MAGIC   └─ postgres_password (✅ Supabase DB password — configured Oct 16 2026)
# MAGIC ```
# MAGIC
# MAGIC ### Bronze Layer (Raw Ingestion)
# MAGIC ```
# MAGIC Notebook: 03_Medallion_Architecture (Cell 4)
# MAGIC   ├─ spark.readStream.format("kafka") → trigger(availableNow=True)
# MAGIC   ├─ Checkpoint: /Volumes/finguard/source/transactions/bronze_checkpoint/
# MAGIC   └─ finguard.bronze.bronze_transactions (29,904 rows) — raw Kafka key/value + metadata
# MAGIC
# MAGIC Notebook: 01_Kafka_Streaming_Test (Cell 8, 11)
# MAGIC   ├─ finguard.bronze.transactions_batch_test (29,825 rows) — batch test
# MAGIC   └─ finguard.bronze.transactions_streaming_test (59,808 rows) — streaming test
# MAGIC
# MAGIC Notebook: 06_Postgres_Setup (Oct 16 2026)
# MAGIC   ├─ Supabase PostgreSQL (eu-west-1 pooler) — 1,000 customers loaded via pg8000
# MAGIC   ├─ UC connection: finguard_postgres (ACTIVE; Lakeflow Connect gateway blocked — not entitled)
# MAGIC   ├─ Lakehouse Federation: foreign catalog finguard_pg → public.customers
# MAGIC   └─ finguard.bronze.bronze_customers_pg (1,000 rows) — daily refresh job scheduled
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer (Cleansed + Enriched)
# MAGIC ```
# MAGIC Notebook: 03_Medallion_Architecture (Cell 6)
# MAGIC   └─ finguard.silver.silver_transactions (29,425 rows)
# MAGIC      ├─ Parse JSON from bronze value column
# MAGIC      ├─ Validate: non-null transaction_id, amount > 0
# MAGIC      ├─ Deduplicate by transaction_id
# MAGIC      └─ Derived: is_high_value, transaction_date
# MAGIC
# MAGIC Stage 02 Fix (executeCode):
# MAGIC   └─ finguard.silver.silver_customers (1,000 rows)
# MAGIC      ├─ Synthetic customer master data (risk_profile, account_type, full_name)
# MAGIC      └─ Derived: customer_age_days, is_high_risk
# MAGIC
# MAGIC   └─ finguard.silver.silver_enriched_transactions (29,425 rows, 31 cols)
# MAGIC      ├─ Stream-static join: silver_transactions LEFT JOIN silver_customers
# MAGIC      ├─ Adds: customer_full_name, customer_risk_profile, customer_is_high_risk,
# MAGIC      │        customer_account_type, customer_home_city, customer_home_country
# MAGIC      ├─ Derived: is_unusual_location (txn_city ≠ customer_home_city)
# MAGIC      └─ Derived: is_flagged (TXF prefix OR in gold_flagged_transactions)
# MAGIC ```
# MAGIC
# MAGIC ### Watchlist Pipeline
# MAGIC ```
# MAGIC JSON files → /Volumes/finguard/bronze/raw_files/watchlist/*.json
# MAGIC   └─ 6 entries (2 customers + 4 merchants, high/critical severity)
# MAGIC
# MAGIC   ├─ finguard.bronze.bronze_fraud_watchlist (6 rows) — raw JSON
# MAGIC   └─ finguard.silver.silver_fraud_watchlist (6 rows) — deduplicated, high/critical only
# MAGIC ```
# MAGIC
# MAGIC ### Gold Layer (Fraud Detection + Analytics)
# MAGIC ```
# MAGIC Stage 02 Fix (executeCode):
# MAGIC   └─ finguard.gold.gold_flagged_transactions (3,923 rows, 27 cols)
# MAGIC      ├─ 8 fraud rules (weighted risk score 60–175)
# MAGIC      ├─ Threshold: risk_score ≥ 60 → flagged
# MAGIC      ├─ Severity: HIGH ≥ 80, MEDIUM ≥ 60
# MAGIC      ├─ fraud_status: SUSPECTED (→ REVIEW → CONFIRMED/REJECTED)
# MAGIC      ├─ Customer enrichment columns (from silver_enriched)
# MAGIC      └─ Watchlist match columns (from silver_fraud_watchlist)
# MAGIC
# MAGIC   └─ finguard.gold.gold_fraud_summary (250 rows) — 5-min window + merchant_category
# MAGIC   └─ finguard.gold.gold_hourly_fraud_rate (7 rows) — fraud_rate = flagged/total
# MAGIC   └─ finguard.gold.gold_customer_risk_summary (52,371 rows) — 10-min sliding window
# MAGIC
# MAGIC Notebook: 03_Medallion_Architecture (Cell 8):
# MAGIC   └─ finguard.gold.gold_merchant_category_summary (11 rows)
# MAGIC   └─ finguard.gold.gold_customer_summary (1,000 rows)
# MAGIC   └─ finguard.gold.gold_hourly_volume (7 rows)
# MAGIC   └─ finguard.gold.gold_high_value_transactions (14,660 rows)
# MAGIC ```
# MAGIC
# MAGIC ### Alert Layer
# MAGIC ```
# MAGIC Notebook: 04_Alert_Pipeline (Cell 4):
# MAGIC   └─ finguard.gold.gold_alert_log (50 rows, 49 SENT / 1 FAILED)
# MAGIC      ├─ Reads HIGH severity from gold_flagged_transactions
# MAGIC      ├─ Generates UUID-based alert_id
# MAGIC      ├─ Duplicate prevention (checks existing alert_log)
# MAGIC      ├─ Retry logic: PENDING + FAILED-with-delivery-error alerts re-sent on each run
# MAGIC      └─ Email via Gmail SMTP (smtp.gmail.com:587, STARTTLS)
# MAGIC         └─ Status: ✅ OPERATIONAL (App Password configured Oct 16 2026; 49 emails delivered;
# MAGIC            1 FAILED permanently — TXN689904 no longer in gold_flagged_transactions)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Notebook Roles Mapping
# MAGIC %md
# MAGIC ## 3. Notebook Roles Mapping
# MAGIC
# MAGIC | Notebook | Role | Key Operations |
# MAGIC | --- | --- | --- |
# MAGIC | `01_Kafka_Streaming_Test` | Initial Kafka connectivity test | Batch + streaming read from Confluent Cloud, writes to test tables |
# MAGIC | `02_Setup_Secrete_Scope` | Secret management setup | Creates `finguard-scope`, stores Kafka + Gmail credentials |
# MAGIC | `03_Medallion_Architecture` | Main pipeline (Bronze→Silver→Gold) | Kafka streaming ingestion, JSON parsing, deduplication, analytics gold tables |
# MAGIC | `04_Alert_Pipeline` | Email alerting for fraud | Reads HIGH severity, generates alert_log, sends Gmail SMTP emails |
# MAGIC | `05_Architecture_Trace` | This notebook — architecture documentation | Full table inventory, data flow trace, gap analysis |
# MAGIC
# MAGIC ### Execution Order (Specified)
# MAGIC ```
# MAGIC 02_Setup_Secrete_Scope → 01_Kafka_Streaming_Test → 03_Medallion_Architecture → 04_Alert_Pipeline
# MAGIC ```
# MAGIC
# MAGIC ### Actual Execution Order (Observed)
# MAGIC ```
# MAGIC 01_Kafka_Streaming_Test → 02_Setup_Secrete_Scope → 03_Medallion_Architecture → 04_Alert_Pipeline
# MAGIC ```
# MAGIC Note: Notebook 01 was created/run first (hardcoded credentials), then 02 properly stored them in the secret scope.

# COMMAND ----------

# DBTITLE 1,Fraud Rules Summary
# MAGIC %md
# MAGIC ## 4. Fraud Detection Rules (8 Rules, Weighted Scoring)
# MAGIC
# MAGIC | # | Rule | Weight | Flagged | Description |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | HIGH_VALUE | +40 | 2,577 | Per-currency: INR ≥ 95K, USD ≥ 1,200, EUR ≥ 1,100 |
# MAGIC | 2 | INTERNATIONAL | +30 | 2,389 | is_international = true |
# MAGIC | 3 | HIGH_RISK_CATEGORY | +20 | 775 | Jewellery, Electronics, Travel |
# MAGIC | 4 | ONLINE_HIGH_VALUE | +10 | 1,146 | ONLINE + high-value threshold |
# MAGIC | 5 | VELOCITY | +50 | 810 | ≥8 txns per customer per 10-min window |
# MAGIC | 6 | UNUSUAL_LOCATION | +15 | 3,550 | txn_city ≠ customer_home_city |
# MAGIC | 7 | CUSTOMER_HIGH_RISK | +30 | 864 | customer_risk_profile = high |
# MAGIC | 8 | WATCHLIST_MATCH | +60 | 82 | Customer or merchant on fraud watchlist |
# MAGIC
# MAGIC ### Scoring & Flagging
# MAGIC - **Threshold**: risk_score ≥ 60 → flagged
# MAGIC - **Severity**: HIGH ≥ 80, MEDIUM ≥ 60
# MAGIC - **Flagging rate**: 3,923 / 29,425 = **13.33%**
# MAGIC - **fraud_status**: All SUSPECTED (supports SUSPECTED → REVIEW → CONFIRMED/REJECTED)
# MAGIC - **Max possible score**: 270 (all 8 rules firing)

# COMMAND ----------

# DBTITLE 1,Gap Analysis
# MAGIC %md
# MAGIC ## 5. Gap Analysis — What's Done vs What Remains
# MAGIC
# MAGIC ### ✅ Completed (Stage 02 Fixes)
# MAGIC * Velocity rule (≥8 txns/10-min, +50 points)
# MAGIC * Silver derived columns: `is_high_value`, `transaction_date`
# MAGIC * Customer enrichment: `silver_customers` (1,000), `silver_enriched_transactions` (stream-static join)
# MAGIC * Watchlist pipeline: JSON files → `bronze_fraud_watchlist` → `silver_fraud_watchlist`
# MAGIC * Gold fraud detection: 8 weighted rules, `fraud_status` column
# MAGIC * Gold analytics: `gold_fraud_summary`, `gold_hourly_fraud_rate`, `gold_customer_risk_summary`
# MAGIC * `is_flagged` field (derived: TXF prefix or in gold_flagged)
# MAGIC * Unusual location detection (`is_unusual_location`)
# MAGIC * Flagging rate calibrated: 38.96% → 13.33%
# MAGIC * Currency normalization (per-currency thresholds)
# MAGIC
# MAGIC ### ⚠️ Remaining Gaps (Upstream Stages)
# MAGIC * **`bronze_transactions` MISSING**: The main bronze table from notebook 03 was not found. It was likely created via `writeStream` on a previous compute that has since been terminated. The silver layer still has 29,425 rows from when it was processed. The bronze data needs to be re-ingested from Kafka.
# MAGIC * **Producer `is_flagged`**: Producer doesn't emit `is_flagged` — derived instead (TXF prefix or gold match)
# MAGIC * ~~Gmail SMTP~~ ✅ RESOLVED Oct 16 2026 — real App Password stored; test email delivered; alert log now 49 SENT / 1 FAILED
# MAGIC * ~~PostgreSQL~~ ✅ RESOLVED Oct 16 2026 — 1,000 customers loaded to Supabase Postgres; landed as `finguard.bronze.bronze_customers_pg` via Lakehouse Federation; daily refresh job scheduled. (Managed Lakeflow Connect ingestion blocked: workspace not entitled — 403 PERMISSION_DENIED; requires Databricks support enablement)
# MAGIC * **Auto Loader**: Watchlist JSONs manually loaded — spec calls for Auto Loader pipeline
# MAGIC * **SDP Pipeline**: PARTIAL — bronze Kafka SDP pipeline exists; silver/gold processing still notebook-based
# MAGIC * ~~Dashboard~~ ✅ RESOLVED — FinGuard Fraud Detection Dashboard created with 8 widgets
# MAGIC * **Orchestration**: PARTIAL — daily bronze_customers_pg refresh job scheduled; main pipeline still manually run
# MAGIC * **Streaming vs Batch**: Bronze uses `trigger(availableNow=True)` — spec calls for continuous streaming
# MAGIC * **Secret scope naming**: Spec says `finguard-secrets`, actual is `finguard-scope` (accepted deviation)

# COMMAND ----------

# DBTITLE 1,Live Architecture Trace Verification
# ==========================================
# 6. Live Architecture Trace — Verify All Tables
# ==========================================
from pyspark.sql.functions import col

print("=" * 70)
print("   FinGuard Architecture Trace — Live Verification")
print("=" * 70)

# Trace each layer
layers = {
    "BRONZE (Raw)": [
        ("finguard.bronze.bronze_transactions", "Kafka streaming ingestion"),
        ("finguard.bronze.bronze_customers", "Synthetic customer master data"),
        ("finguard.bronze.bronze_fraud_watchlist", "Raw watchlist JSON files"),
        ("finguard.bronze.transactions_batch_test", "Batch Kafka test (legacy)"),
        ("finguard.bronze.transactions_streaming_test", "Streaming Kafka test (legacy)"),
        ("finguard.bronze.bronze_customers_pg", "Supabase Postgres customers (federation)"),
    ],
    "SILVER (Cleansed + Enriched)": [
        ("finguard.silver.silver_transactions", "Parsed, validated, deduplicated"),
        ("finguard.silver.silver_customers", "Cleansed customer master + derived columns"),
        ("finguard.silver.silver_enriched_transactions", "Stream-static join + customer enrichment"),
        ("finguard.silver.silver_fraud_watchlist", "Deduplicated watchlist (high/critical)"),
    ],
    "GOLD (Analytics + Fraud Detection)": [
        ("finguard.gold.gold_flagged_transactions", "8-rule fraud detection (3,923 flagged)"),
        ("finguard.gold.gold_fraud_summary", "5-min window + merchant_category"),
        ("finguard.gold.gold_hourly_fraud_rate", "Hourly fraud rate trend"),
        ("finguard.gold.gold_customer_risk_summary", "Sliding 10-min customer velocity"),
        ("finguard.gold.gold_merchant_category_summary", "Category aggregation"),
        ("finguard.gold.gold_customer_summary", "Customer aggregation"),
        ("finguard.gold.gold_hourly_volume", "Hourly volume trend"),
        ("finguard.gold.gold_high_value_transactions", "High-value transactions (>50K INR)"),
        ("finguard.gold.gold_alert_log", "Email alerts (49 SENT / 1 FAILED)"),
    ],
}

for layer_name, tables in layers.items():
    print(f"\n{'─' * 70}")
    print(f"  {layer_name}")
    print(f"{'─' * 70}")
    for table_name, description in tables:
        try:
            cnt = spark.read.table(table_name).count()
            cols = len(spark.read.table(table_name).columns)
            print(f"  ✅ {table_name:55s} {cnt:>8,} rows  ({cols} cols) — {description}")
        except Exception as e:
            print(f"  ❌ {table_name:55s} NOT FOUND — {description}")

print(f"\n{'=' * 70}")
print("   Architecture Trace Complete")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Data Lineage Summary
# MAGIC %md
# MAGIC ## 7. Data Lineage Summary
# MAGIC
# MAGIC ### Transaction Flow (Primary)
# MAGIC ```
# MAGIC Kafka Topic: credit_card_transactions
# MAGIC   │
# MAGIC   ├─ spark.readStream.format("kafka")
# MAGIC   │   └─ finguard.bronze.bronze_transactions (29,904 rows, raw key/value)
# MAGIC   │
# MAGIC   ├─ Parse JSON + validate + deduplicate
# MAGIC   │   └─ finguard.silver.silver_transactions (29,425 rows, 23 cols)
# MAGIC   │
# MAGIC   ├─ LEFT JOIN silver_customers (stream-static join)
# MAGIC   │   └─ finguard.silver.silver_enriched_transactions (29,425 rows, 31 cols)
# MAGIC   │
# MAGIC   ├─ Apply 8 fraud rules + watchlist match
# MAGIC   │   └─ finguard.gold.gold_flagged_transactions (3,923 rows, 27 cols)
# MAGIC   │
# MAGIC   ├─ Filter HIGH severity + generate alerts
# MAGIC   │   └─ finguard.gold.gold_alert_log (10 rows, PENDING)
# MAGIC   │
# MAGIC   └─ Aggregate by window + category
# MAGIC       ├─ finguard.gold.gold_fraud_summary (250 rows)
# MAGIC       ├─ finguard.gold.gold_hourly_fraud_rate (7 rows)
# MAGIC       └─ finguard.gold.gold_customer_risk_summary (52,371 rows)
# MAGIC ```
# MAGIC
# MAGIC ### Watchlist Flow (Secondary)
# MAGIC ```
# MAGIC JSON files in /Volumes/finguard/bronze/raw_files/watchlist/
# MAGIC   │
# MAGIC   ├─ spark.read.format("json")
# MAGIC   │   └─ finguard.bronze.bronze_fraud_watchlist (6 rows)
# MAGIC   │
# MAGIC   ├─ Deduplicate + filter high/critical
# MAGIC   │   └─ finguard.silver.silver_fraud_watchlist (6 rows)
# MAGIC   │
# MAGIC   └─ LEFT JOIN into gold_flagged_transactions
# MAGIC       └─ watchlist_match, watchlist_severity, watchlist_reason (82 matches)
# MAGIC ```
# MAGIC
# MAGIC ### Customer Flow (Enrichment)
# MAGIC ```
# MAGIC Synthetic generation (1,000 customers)
# MAGIC   │
# MAGIC   ├─ finguard.bronze.bronze_customers (1,000 rows)
# MAGIC   │
# MAGIC   ├─ Cleanse + derive columns
# MAGIC   │   └─ finguard.silver.silver_customers (1,000 rows, 15 cols)
# MAGIC   │
# MAGIC   └─ LEFT JOIN into silver_enriched_transactions
# MAGIC       └─ customer_full_name, risk_profile, account_type, home_city, is_high_risk
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Stages 04-10: Kafka Through Secret Scope
# MAGIC %md
# MAGIC ## 8. Stages 04-10: Kafka Through Secret Scope
# MAGIC
# MAGIC ### Stage 04: Kafka Architecture
# MAGIC ```
# MAGIC Producer (Python, external)
# MAGIC   │
# MAGIC   │ JSON messages → Confluent Cloud Kafka
# MAGIC   ▼
# MAGIC Confluent Cloud (pkc-619z3.us-east1.gcp.confluent.cloud:9092)
# MAGIC   ├─ Topic: credit_card_transactions
# MAGIC   ├─ Partitions: 6 (observed offsets 0–6 in partitions 0-5)
# MAGIC   ├─ ~29,904 messages (as of Oct 16 2026)
# MAGIC   ├─ Retention: Confluent default (7 days or 100MB)
# MAGIC   └─ Protocol: SASL_SSL (SASL/PLAIN + TLS)
# MAGIC ```
# MAGIC
# MAGIC ### Stage 05: Confluent Cloud Setup
# MAGIC ```
# MAGIC Confluent Cloud Account:
# MAGIC   ├─ Cluster: pkc-619z3.us-east1.gcp.confluent.cloud
# MAGIC   ├─ API Key: (stored in secret scope — redacted for public repo)
# MAGIC   ├─ API Secret: stored in secret scope (redacted)
# MAGIC   ├─ Region: us-east1 (GCP)
# MAGIC   └─ Authentication: SASL_SSL + PLAIN mechanism
# MAGIC ```
# MAGIC
# MAGIC ### Stage 06: Producer (External)
# MAGIC ```
# MAGIC Producer: Python Kafka producer (NOT in repo)
# MAGIC   ├─ Generates synthetic credit card transactions
# MAGIC   ├─ Message format: JSON (16 fields)
# MAGIC   ├─ Transaction ID formats: TXN*, TXF* (anomaly)
# MAGIC   ├─ Currencies: INR (primary), USD, EUR
# MAGIC   ├─ Cities: 8 Indian cities
# MAGIC   ├─ Merchant categories: 11 (Shopping, Electronics, Travel, Jewellery, etc.)
# MAGIC   ├─ ~29,904 messages produced over ~7 hours (Oct 13-14 2026)
# MAGIC   └─ GAPS:
# MAGIC      ❌ Producer doesn't emit `is_flagged` field (derived in silver instead)
# MAGIC      ❌ Producer code not in repo (external)
# MAGIC      ❌ No timestamp in message (uses transaction_timestamp as string)
# MAGIC ```
# MAGIC
# MAGIC ### Stage 07: Spark-Kafka Connection (FIXED)
# MAGIC ```
# MAGIC Notebook: 03_Medallion_Architecture (Cell 2)
# MAGIC   ├─ Credentials loaded from: dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
# MAGIC   ├─ JAAS Config: kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
# MAGIC   │  └─ CRITICAL: Serverless compute requires `kafkashaded.` prefix, NOT `org.apache.kafka.`
# MAGIC   ├─ Connection options: kafka.bootstrap.servers, subscribe, SASL_SSL, PLAIN
# MAGIC   └─ Re-ingestion (Oct 16): Used checkpoint v2 at /Volumes/finguard/source/transactions/bronze_checkpoint_v2/
# MAGIC ```
# MAGIC
# MAGIC ### Stage 08: Streaming Consumption
# MAGIC ```
# MAGIC Pattern: readStream → writeStream → trigger(availableNow=True)
# MAGIC   ├─ Bronze: readStream from Kafka → writeStream to Delta
# MAGIC   ├─ AvailableNow=True: processes all available data in one batch, then stops
# MAGIC   ├─ Checkpoint: /Volumes/finguard/source/transactions/bronze_checkpoint_v2/
# MAGIC   ├─ Output mode: append (preserves all messages)
# MAGIC   └─ Result: 29,904 rows ingested to bronze_transactions
# MAGIC
# MAGIC GAPS:
# MAGIC   ❌ Spec calls for continuous streaming — availableNow is one-time drain
# MAGIC   ❌ No silver streaming layer (silver is batch-processed from bronze)
# MAGIC   ❌ No gold streaming layer (gold is batch-processed from silver)
# MAGIC ```
# MAGIC
# MAGIC ### Stage 09: Trigger Configurations
# MAGIC ```
# MAGIC Current: trigger(availableNow=True)
# MAGIC   └─ Bounded one-time processing: reads all available Kafka offsets, processes, stops
# MAGIC
# MAGIC Spec options:
# MAGIC   ├─ availableNow=True (current): Best for batch-style, one-time ingestion
# MAGIC   ├─ processingTime='10 seconds': Fixed-interval micro-batch streaming
# MAGIC   └─ once=True: Single micro-batch then stop (deprecated in favor of availableNow)
# MAGIC
# MAGIC GAP: No continuous streaming trigger configured. For real-time fraud detection,
# MAGIC spec recommends processingTime='10 seconds' for near-real-time processing.
# MAGIC ```
# MAGIC
# MAGIC ### Stage 10: Secret Scope Audit
# MAGIC ```
# MAGIC Scope: finguard-scope (DATABRICKS-backed)
# MAGIC   ├─ kafka_connection_details ✅ (JSON: bootstrap_server, topic_name, api_key, api_secret)
# MAGIC   ├─ gmail_api_key ✅ (real Gmail App Password — configured Oct 16 2026)
# MAGIC   ├─ gmail_sender_email ✅ (dsobahle@gmail.com)
# MAGIC   ├─ gmail_recipient_email ✅ (dsobahle@gmail.com)
# MAGIC   └─ postgres_password ✅ (Supabase DB password — configured Oct 16 2026)
# MAGIC
# MAGIC GAPS:
# MAGIC   ❌ Spec says scope name should be `finguard-secrets` (actual: `finguard-scope`)
# MAGIC   ❌ No Kafka consumer group ID set (uses default)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Kafka & Secret Scope Live Audit
# ==========================================
# 9. Kafka Connection & Secret Scope Live Audit
# ==========================================
import json

print("=" * 70)
print("   Kafka & Secret Scope Audit")
print("=" * 70)

# Audit 1: Secret scope contents
print("\n── SECRET SCOPE AUDIT ──")
print(f"  Scope: finguard-scope (DATABRICKS-backed)")

secrets_to_check = ["kafka_connection_details", "gmail_api_key", "gmail_sender_email", "gmail_recipient_email", "postgres_password"]
for secret_key in secrets_to_check:
    try:
        val = dbutils.secrets.get(scope="finguard-scope", key=secret_key)
        if secret_key == "kafka_connection_details":
            cfg = json.loads(val)
            print(f"  ✅ {secret_key}: JSON with keys {list(cfg.keys())}")
        elif val == "YOUR_GMAIL_API_KEY_HERE":
            print(f"  ⚠️ {secret_key}: PLACEHOLDER (not configured)")
        else:
            print(f"  ✅ {secret_key}: configured ({len(val)} chars)")
    except Exception:
        print(f"  ❌ {secret_key}: NOT FOUND")

# Audit 2: Kafka connection test
print("\n── KAFKA CONNECTION TEST ──")
kafka_secret = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
kafka_config = json.loads(kafka_secret)
print(f"  Bootstrap: {kafka_config['bootstrap_server']}")
print(f"  Topic: {kafka_config['topic_name']}")
print(f"  API Key: {kafka_config['api_key'][:4]}...{kafka_config['api_key'][-4:]}")

# Audit 3: Bronze layer status
print("\n── BRONZE LAYER STATUS ──")
bronze_cnt = spark.read.table("finguard.bronze.bronze_transactions").count()
print(f"  ✅ bronze_transactions: {bronze_cnt:,} rows (re-ingested Oct 16)")

# Check partitions
partitions = spark.read.table("finguard.bronze.bronze_transactions").select("kafka_partition").distinct().orderBy("kafka_partition").collect()
print(f"  Partitions: {[r.kafka_partition for r in partitions]}")

# Check offset range
from pyspark.sql import functions as F
offset_range = spark.read.table("finguard.bronze.bronze_transactions").groupBy("kafka_partition").agg(
    F.min("kafka_offset").alias("min_offset"),
    F.max("kafka_offset").alias("max_offset"),
    F.count("*").alias("msg_count")
).orderBy("kafka_partition")
print("  Offset ranges per partition:")
offset_range.show()

# Audit 4: Streaming vs batch summary
print("── STREAMING CONFIG SUMMARY ──")
print("  Trigger: availableNow=True (one-time bounded)")
print("  Checkpoint: /Volumes/finguard/source/transactions/bronze_checkpoint_v2/")
print("  Output mode: append")
print("  ⚠️  Spec calls for continuous streaming (processingTime='10 seconds')")
print("  ⚠️  Silver/Gold layers are batch-processed (no streaming pipeline)")

print(f"\n{'=' * 70}")
print("   Audit Complete")
print("=" * 70)

# COMMAND ----------

