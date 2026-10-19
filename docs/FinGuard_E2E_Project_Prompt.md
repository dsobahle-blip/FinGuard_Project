# FinGuard: End-to-End Real-Time Fraud Detection System
## The Ultimate Practical Prompt for Building a Medallion Architecture ETL Project on Databricks

---

> **How to use this prompt:** Copy everything below the line and paste it into your AI assistant (Databricks Genie Code, ChatGPT, Claude, etc.) or use it as a master blueprint to build the project step by step. Each section maps to a phase in the project lifecycle. Execute sequentially.

---

## MASTER PROMPT

I want to build a complete, production-grade, end-to-end **FinGuard** — a real-time fraud detection system — using **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks**. This project must cover stream processing, batch ingestion, real-time alerting, dashboards, and full orchestration. Below is the complete specification. Build every component.

---

## PHASE 0: PROJECT OVERVIEW & ARCHITECTURE

### 0.1 What is Stream Processing?

Stream processing is the continuous, real-time processing of data as it arrives, rather than waiting for batches. Unlike batch processing (which processes data in scheduled intervals), stream processing enables near-real-time insights, fraud detection, anomaly detection, and instant alerting.

**Key concepts to implement:**
- **Structured Streaming** — Spark's declarative API for streaming workloads (micro-batch or continuous processing)
- **Event-time processing** — using the timestamp embedded in the data, not the processing time
- **Watermarks** — handling late-arriving data with a threshold
- **Stateful operations** — aggregations, joins, and deduplication that require maintaining state across batches
- **Trigger types** — `processingTime`, `once`, `availableNow` for different latency/throughput trade-offs
- **Checkpointing** — fault-tolerant state management via checkpoint locations

### 0.2 FinGuard — Fraud Detection Use Case

**Business problem:** A financial services company needs to detect fraudulent transactions in real-time. The system must:
1. Ingest live transaction streams from Kafka
2. Enrich transactions with customer data (from Postgres)
3. Cross-reference against a fraud watchlist (streamed JSON files)
4. Flag suspicious transactions based on rules + windowed aggregations
5. Send real-time email alerts to the fraud team for flagged transactions
6. Provide a near-real-time dashboard for monitoring
7. Be fully orchestrated with Lakeflow Jobs

### 0.3 Overall Project Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              FINGUARD ARCHITECTURE                                     │
│                                                                                        │
│  ┌──────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────┐               │
│  │  Kafka   │────▶│  SDP Bronze  │────▶│  SDP Silver  │────▶│  Gold    │               │
│  │(Confluent│     │  (Raw Land)  │     │ (Cleaned +  │     │(Aggreg.  │               │
│  │  Cloud)  │     │              │     │  Enriched)  │     │ + Alerts)│               │
│  └──────────┘     └──────────────┘     └─────────────┘     └──────────┘               │
│       ▲                                                            │                    │
│       │                                                            ▼                    │
│  ┌──────────┐     ┌──────────────┐     ┌─────────────────────┐  ┌──────────┐          │
│  │  Postgres│────▶│ Lakeflow     │────▶│  SDP Bronze        │  │Dashboard │          │
│  │  (Batch) │     │  Connect     │     │  (Customer Data)   │  │(Near RT) │          │
│  └──────────┘     └──────────────┘     └───────────────────┘  └──────────┘          │
│                                                                                        │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────────────┐                   │
│  │ JSON Files   │────▶│  Auto Loader │────▶│  SDP Bronze          │                   │
│  │ (Watchlist)  │     │  (Streaming) │     │  (Fraud Watchlist)  │                   │
│  └──────────────┘     └──────────────┘     └───────────────────┘                   │
│                                                                                        │
│  ┌────────────────────────────────────────────────────────────┐                      │
│  │              Lakeflow Jobs (Orchestration)                  │                      │
│  │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │                      │
│  │  │ SDP     │  │ SDP      │  │ SDP      │  │ Dashboard   │  │                      │
│  │  │ Pipeline│  │ Pipeline │  │ Pipeline │  │ Refresh Job │  │                      │
│  │  │ #1      │  │ #2       │  │ #3       │  │             │  │                      │
│  │  └─────────┘  └──────────┘  └──────────┘  └─────────────┘  │                      │
│  └────────────────────────────────────────────────────────────┘                      │
│                                                                                        │
│  ┌────────────────────────────────────────────────────────────┐                      │
│  │           Real-Time Email Alert (Gmail SMTP)                │                      │
│  │  Triggered by SDP pipeline → foreachBatch → smtplib         │                      │
│  └────────────────────────────────────────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### 0.4 Databricks Workspace Setup

**Unity Catalog structure:**
```
Catalog: finguard
├── Schema: bronze        -- Raw landed data (all sources)
│   ├── bronze_transactions        -- From Kafka (streaming)
│   ├── bronze_customers           -- From Postgres via Lakeflow Connect
│   └── bronze_fraud_watchlist     -- From JSON files via Auto Loader
├── Schema: silver        -- Cleaned, deduplicated, enriched
│   ├── silver_transactions       -- Parsed + validated + customer-enriched
│   ├── silver_customers          -- Cleansed customer master data
│   └── silver_fraud_watchlist    -- Parsed + deduplicated watchlist
├── Schema: gold          -- Aggregated, business-ready
│   ├── gold_flagged_transactions  -- Transactions flagged as fraud
│   ├── gold_fraud_summary         -- Aggregated fraud metrics by window
│   └── gold_alert_log            -- Log of all alerts sent
```

**Volume structure:**
```
Volume: finguard.bronze.checkpoints
Volume: finguard.bronze.raw_files (for JSON watchlist drops)
Volume: finguard.silver.staging
```

**Secret Scope (Databricks):**
```
Scope: finguard-secrets
├── kafka-bootstrap-servers    -- Confluent Kafka bootstrap servers
├── kafka-sasl-username         -- Confluent API key
├── kafka-sasl-password         -- Confluent API secret
├── postgres-username           -- Postgres DB username
├── postgres-password           -- Postgres DB password
├── postgres-jdbc-url           -- Postgres JDBC connection string
├── gmail-app-password          -- Gmail SMTP app password
└── gmail-sender-email          -- Gmail sender email address
```

---

## PHASE 1: KAFKA SETUP (Confluent Cloud + Producer)

### 1.1 Confluent Kafka Cluster Setup

Set up a Confluent Cloud Kafka cluster:
1. Create a Confluent Cloud account (free tier available)
2. Create a Basic or Standard Kafka cluster
3. Create an API key + secret for authentication
4. Create the following topics:
   - `transactions` (partitions: 3, retention: 7 days)
   - `fraud-alerts` (partitions: 1)
5. Note the bootstrap servers, API key, and API secret
6. Configure SASL/SSL authentication:
   - Protocol: `SASL_SSL`
   - Mechanism: `PLAIN`
   - JAAS config: `org.apache.kafka.common.security.plain.PlainLoginModule required username="<API_KEY>" password="<API_SECRET>";`

### 1.2 Kafka Producer Setup

Create a Python Kafka producer that simulates real-time financial transactions:

**Transaction schema (JSON):**
```json
{
  "transaction_id": "txn_000001",
  "customer_id": "cust_12345",
  "amount": 1250.75,
  "currency": "USD",
  "merchant": "Amazon",
  "merchant_category": "E-commerce",
  "location": "New York, USA",
  "latitude": 40.7128,
  "longitude": -74.0060,
  "timestamp": "2026-10-14T10:30:00.000Z",
  "channel": "online",
  "device_id": "dev_abc123",
  "is_flagged": false
}
```

**Producer requirements:**
- Use `confluent-kafka` Python library
- Generate 1 transaction every 0.5–2 seconds (randomized)
- Inject ~5% fraudulent transactions (unusual amounts > $10,000, unusual locations, rapid succession)
- Serialize as JSON, send to `transactions` topic
- Use the SASL/SSL config from Confluent
- Run as a standalone Python script (can be run from a Databricks notebook or locally)
- Include proper error handling, logging, and graceful shutdown

**Configuration to retrieve from Databricks Secret Scope:**
- `kafka-bootstrap-servers`
- `kafka-sasl-username`
- `kafka-sasl-password`

---

## PHASE 2: SPARK-KAFKA DATA CONNECTION

### 2.1 Spark Streaming — Kafka Data Consumption

Create a Spark Structured Streaming query to consume from Kafka:

**Read from Kafka:**
- Format: `kafka`
- Options: `kafka.bootstrap.servers`, `subscribe`, `startingOffsets=latest`, `kafka.security.protocol=SASL_SSL`, `kafka.sasl.mechanism=PLAIN`, `kafka.sasl.jaas.config`
- Pull Kafka credentials from Databricks Secret Scope using `dbutils.secrets.get()`
- The raw Kafka DataFrame has columns: `key`, `value` (binary), `topic`, `partition`, `offset`, `timestamp`, `timestampType`
- Cast `value` to STRING and parse JSON to extract transaction fields

**Schema for parsing Kafka messages:**
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("location", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("is_flagged", BooleanType(), True)
])
```

### 2.2 Spark Streaming — Trigger Types

Implement and understand all three trigger types:

| Trigger | Description | Use Case |
|---|---|---|
| `processingTime('10 seconds')` | Fixed interval micro-batches | Near real-time with controlled latency |
| `once()` | Process one batch then stop | Testing / debugging / one-time loads |
| `availableNow()` | Process all available data then stop | Bounded drain for cost-sensitive workloads |

**Default behavior:** If no trigger is specified, Spark uses `processingTime` with no fixed interval (processes data as fast as possible).

For FinGuard, use `processingTime('10 seconds')` for near-real-time fraud detection.

---

## PHASE 3: SECRET SCOPE SETUP

### 3.1 Databricks Secret Scope

Create a Databricks-backed secret scope to store all sensitive credentials:

1. Create secret scope: `finguard-secrets`
2. Add secrets:
   - `kafka-bootstrap-servers` → Confluent bootstrap servers
   - `kafka-sasl-username` → Confluent API key
   - `kafka-sasl-password` → Confluent API secret
   - `postgres-username` → Postgres username
   - `postgres-password` → Postgres password
   - `postgres-jdbc-url` → Postgres JDBC URL
   - `gmail-app-password` → Gmail app password
   - `gmail-sender-email` → Sender email

3. In all notebooks and SDP pipelines, retrieve secrets using:
   ```python
   dbutils.secrets.get(scope="finguard-secrets", key="kafka-bootstrap-servers")
   ```

4. Grant the secret scope access to the service principal or user running the SDP pipelines.

---

## PHASE 4: LAKEFLOW SPARK DECLARATIVE PIPELINE (SDP) — KAFKA INGESTION

### 4.1 SDP Pipeline #1: Kafka → Bronze Transactions

Create a **Lakeflow Spark Declarative Pipeline** that:
1. Reads from Kafka topic `transactions` using Structured Streaming
2. Parses the binary `value` column as JSON
3. Writes raw data to `finguard.bronze.bronze_transactions` as a **streaming table**
4. Uses a checkpoint location: `/Volumes/finguard/bronze/checkpoints/bronze_transactions`
5. Sets expectations (data quality rules):
   - `transaction_id IS NOT NULL`
   - `amount > 0`
   - `customer_id IS NOT NULL`
6. Configure pipeline settings:
   - Serverless compute
   - `channel: CURRENT`
   - Pipeline mode: `continuous` (for streaming)

**SDP code structure (Python):**
- Use `@dlt.table` or `@dlt.streaming_table` decorators
- Use `@dlt.expect` or `@dlt.expect_or_drop` for data quality
- Read Kafka options from secret scope
- Parse JSON using `from_json()`
- Include all raw fields + Kafka metadata (topic, partition, offset, kafka_timestamp)

### 4.2 SDP: Bronze → Silver Transactions (Streaming)

Create a **streaming table** `finguard.silver.silver_transactions` that:
1. Reads from `finguard.bronze.bronze_transactions` (streaming read)
2. Cleanses and validates:
   - Cast data types properly (timestamp, double, etc.)
   - Filter out null transaction_ids and non-positive amounts
   - Deduplicate by `transaction_id` using watermark
   - Standardize currency to uppercase
   - Parse `timestamp` string to `TIMESTAMP` type
3. Adds derived columns:
   - `ingestion_timestamp` — current processing time
   - `transaction_date` — date extracted from timestamp
   - `is_high_value` — boolean: amount > 10000
   - `risk_score` — simple heuristic: high amount + unusual merchant category → higher score
4. Uses watermark: `withWatermark("timestamp", "10 minutes")`
5. Writes as a streaming table with checkpoint location
6. Add expectations:
   - `amount > 0`
   - `transaction_id IS NOT NULL`
   - `timestamp IS NOT NULL`

---

## PHASE 5: POSTGRESQL + LAKEFLOW CONNECT (BATCH INGESTION)

### 5.1 Postgres SQL Setup

1. Set up a PostgreSQL database (local, cloud, or Supabase/Neon free tier)
2. Create a `customers` table:
   ```sql
   CREATE TABLE customers (
     customer_id VARCHAR(50) PRIMARY KEY,
     first_name VARCHAR(100),
     last_name VARCHAR(100),
     email VARCHAR(255),
     phone VARCHAR(20),
     address TEXT,
     city VARCHAR(100),
     country VARCHAR(100),
     account_type VARCHAR(20),  -- 'personal', 'business', 'premium'
     registration_date TIMESTAMP,
     risk_profile VARCHAR(20),  -- 'low', 'medium', 'high'
     is_active BOOLEAN DEFAULT true
   );
   ```
3. Insert ~10,000 customer records (use a Python script to generate synthetic data)
4. Ensure the Postgres instance is accessible from Databricks (network/firewall)

### 5.2 Ingest Historical Data from Postgres Using Lakeflow Connect

Use **Lakeflow Connect** (managed ingestion pipeline) to ingest from Postgres:
1. Create an ingestion pipeline with `sourceType: POSTGRESQL`
2. Configure connection:
   - JDBC URL (from secret scope)
   - Username + Password (from secret scope)
   - Table: `customers`
3. Target: `finguard.bronze.bronze_customers` (Unity Catalog table)
4. Set ingestion mode: `full` (snapshot) for initial load
5. Schedule: can be triggered manually or scheduled

### 5.3 SDP: Bronze → Silver Customer Data

Create a **materialized view** `finguard.silver.silver_customers` that:
1. Reads from `finguard.bronze.bronze_customers` (batch read — this is a static table)
2. Cleanses:
   - Trim whitespace from all string columns
   - Standardize email to lowercase
   - Validate `is_active` is boolean
   - Fill null risk_profile with 'medium'
3. Adds derived columns:
   - `full_name` — concatenation of first_name + last_name
   - `customer_age_days` — days since registration_date
   - `is_high_risk` — boolean: risk_profile = 'high'
4. Writes as a materialized view (refreshed on pipeline update)
5. Add expectations:
   - `customer_id IS NOT NULL`
   - `email IS NOT NULL`

---

## PHASE 6: STREAM-STATIC JOIN (Transactions × Customers)

### 6.1 Enrich Transactions with Customer Data

Perform a **stream-static join** inside the SDP pipeline:
1. Streaming source: `finguard.silver.silver_transactions` (streaming table)
2. Static source: `finguard.silver.silver_customers` (materialized view)
3. Join on: `customer_id`
4. Add customer enrichment columns:
   - `customer_full_name`
   - `customer_risk_profile`
   - `customer_is_high_risk`
   - `customer_account_type`
5. Write the enriched result to a new streaming table: `finguard.silver.silver_enriched_transactions`
6. This is a **stream-static join** — Spark caches the static side and joins it with each streaming micro-batch

**Key consideration:** Stream-static joins do NOT require a watermark. The static table is read fresh for each micro-batch.

---

## PHASE 7: STREAMING JSON FILES + AUTO LOADER (Fraud Watchlist)

### 7.1 Streaming JSON Files Setup

1. Create a directory in a UC Volume: `/Volumes/finguard/bronze/raw_files/watchlist/`
2. Drop JSON files into this directory periodically (simulating a fraud watchlist feed)
3. Each JSON file contains flagged entities:
   ```json
   {
     "watchlist_id": "wl_001",
     "entity_type": "merchant",
     "entity_name": "DarkWebShop",
     "entity_id": "mkt_999",
     "reason": "Known fraudulent merchant",
     "flagged_date": "2026-10-14",
     "severity": "high",
     "source": "INTERPOL"
   }
   ```

### 7.2 Streaming File Ingestion Using Auto Loader

Use **Auto Loader** (`cloudFiles` format) to incrementally ingest JSON files:
- Format: `cloudFiles`
- Source: `/Volumes/finguard/bronze/raw_files/watchlist/`
- `cloudFiles.format`: `json`
- `cloudFiles.schemaLocation`: `/Volumes/finguard/bronze/checkpoints/watchlist_schema`
- `cloudFiles.inferSchema`: `true` (or define explicit schema)
- `cloudFiles.useNotifications`: `false` (use directory listing for simplicity)

### 7.3 SDP Pipeline #2: Auto Loader → Bronze → Silver (Fraud Watchlist)

**Bronze layer:**
- Create a streaming table `finguard.bronze.bronze_fraud_watchlist` using Auto Loader
- Store raw JSON + file metadata (file path, modification time)
- Checkpoint: `/Volumes/finguard/bronze/checkpoints/bronze_watchlist`

**Silver layer:**
- Create a streaming table `finguard.silver.silver_fraud_watchlist`
- Deduplicate by `watchlist_id` using watermark
- Parse `flagged_date` to DATE type
- Filter: `severity IN ('high', 'critical')`
- Add `is_active` = true column
- Watermark: `withWatermark("flagged_date", "1 day")`

---

## PHASE 8: STATELESS vs STATEFUL STREAM PROCESSING

### 8.1 Stateless Processing

Operations that do NOT require maintaining state across batches:
- `select`, `filter`, `withColumn`, `map`, `flatMap`
- Simple transformations applied per-row
- Used in: bronze → silver cleansing, type casting, filtering

**Example:** Filtering high-value transactions is stateless — each row is evaluated independently.

### 8.2 Stateful Processing

Operations that DO require maintaining state across batches:
- **Aggregations** — `groupBy().agg()` with watermark (sum, count, avg over windows)
- **Stream-stream joins** — joining two streaming DataFrames
- **Deduplication** — dropping duplicate keys across batches
- Used in: gold layer aggregations, fraud detection windows, stream-stream joins

**Key concept:** Stateful operations require a **watermark** to limit how long state is kept, preventing unbounded memory growth.

---

## PHASE 9: WATERMARKING IN STREAMING

### 9.1 Using Watermark

Watermarks define how late data can arrive and still be processed:

```python
(df
  .withWatermark("event_timestamp", "10 minutes")  # Allow 10 min late data
  .groupBy(
      window("event_timestamp", "5 minutes"),  # 5-min tumbling windows
      "customer_id"
  )
  .agg(count("*").alias("txn_count"), sum("amount").alias("total_amount"))
)
```

**Rules for FinGuard:**
- Transaction stream: watermark = `10 minutes` on `timestamp`
- Watchlist stream: watermark = `1 day` on `flagged_date`
- Any data arriving later than the watermark is dropped (or sent to a late data handler)

**Why it matters:** Without a watermark, Spark keeps state forever → OOM errors. The watermark lets Spark discard old state safely.

---

## PHASE 10: STREAM-STREAM JOIN

### 10.1 Joining Two Streaming Sources

Join `silver_enriched_transactions` (streaming) with `silver_fraud_watchlist` (streaming):

- **Left stream:** `silver_enriched_transactions` — watermark: `10 minutes` on `timestamp`
- **Right stream:** `silver_fraud_watchlist` — watermark: `1 day` on `flagged_date`
- **Join condition:** `transactions.merchant == watchlist.entity_name` (or by entity_id)
- **Join type:** `left outer` — all transactions, with watchlist info if matched
- **Time constraint:** Must specify time range for stream-stream joins:
  ```python
  .join(watchlist_stream, 
        expr("""
            transactions.merchant = watchlist.entity_name AND
            transactions.timestamp >= watchlist.flagged_date
        """),
        "leftOuter")
  ```

**Result:** Transactions that match a watchlist entry get flagged with `watchlist_match = true` and `watchlist_severity`.

### 10.2 Gold Layer: Flagged Transactions

Write flagged transactions to `finguard.gold.gold_flagged_transactions`:
- Filter: `is_high_value = true OR watchlist_match = true OR risk_score > 0.7`
- Include all enrichment columns + watchlist match info
- This is a streaming table

---

## PHASE 11: REAL-TIME EMAIL ALERTS

### 11.1 Gmail SMTP App Password Setup

1. Enable 2-Step Verification on the Gmail account
2. Generate an App Password (Google Account → Security → App Passwords)
3. Store in Databricks Secret Scope: `gmail-app-password`, `gmail-sender-email`
4. Use `smtplib` Python library for sending emails

### 11.2 Real-Time Alert Using SDP Pipeline

Create an alerting mechanism within the SDP pipeline:
1. Define a streaming query on `gold_flagged_transactions`
2. Use `foreachBatch` to process flagged transactions in each micro-batch
3. For each flagged transaction, send an email alert:
   - **Subject:** `🚨 FINGUARD ALERT: Fraudulent Transaction Detected — {transaction_id}`
   - **Body:** Include transaction details, customer info, watchlist match, risk score
   - **To:** Fraud team email (e.g., `fraud-team@finguard.com`)
   - **From:** Sender email (from secret scope)
   - **SMTP:** `smtp.gmail.com:587` with TLS
4. Log the alert in `finguard.gold.gold_alert_log` table

**Alert email template:**
```
🚨 FINGUARD FRAUD ALERT

Transaction ID: {transaction_id}
Customer: {customer_full_name} ({customer_id})
Amount: {currency} {amount}
Merchant: {merchant} ({merchant_category})
Location: {location}
Timestamp: {timestamp}
Channel: {channel}

Risk Score: {risk_score}
Watchlist Match: {watchlist_match}
Watchlist Severity: {watchlist_severity}
Customer Risk Profile: {customer_risk_profile}

Action Required: Review and block transaction if confirmed fraudulent.
```

---

## PHASE 12: WINDOWED AGGREGATIONS ON STREAMING DATA

### 12.1 Types of Windows in Streaming

| Window Type | Description | Overlap | Use Case |
|---|---|---|---|
| **Tumbling Window** | Fixed-length, non-overlapping | No overlap | Total transactions per 5-min interval |
| **Sliding Window** | Fixed-length, sliding by interval | Overlaps | Rolling 10-min transaction count, updated every 5 min |
| **Session Window** | Dynamic, based on activity gaps | Variable | User session analysis |

### 12.2 Aggregations on Streaming Data

Create gold-layer aggregation tables:

**1. `gold_fraud_summary` — Tumbling window aggregation:**
- Group by: `window(timestamp, "5 minutes")`, `merchant_category`
- Aggregates: `count(*)`, `sum(amount)`, `avg(risk_score)`, `count(is_flagged)`
- Watermark: `10 minutes` on `timestamp`
- Output mode: `update` (only changed rows)

**2. `gold_customer_risk_summary` — Sliding window:**
- Group by: `window(timestamp, "10 minutes", "2 minutes")`, `customer_id`
- Aggregates: `count(*)`, `sum(amount)`, `max(risk_score)`
- Detect rapid succession: if `count > 5` in a 10-min window → flag
- Watermark: `10 minutes`

**3. `gold_hourly_fraud_rate` — Tumbling 1-hour window:**
- Group by: `window(timestamp, "1 hour")`
- Aggregates: `total_transactions`, `flagged_transactions`, `fraud_rate = flagged/total`
- Used for dashboard

---

## PHASE 13: END-TO-END STREAMING DATA FLOW

### 13.1 Complete Data Flow Summary

```
Kafka Producer (Python)
    │
    ▼ (every 0.5–2s)
Kafka Topic: transactions
    │
    ▼ (Structured Streaming, 10s trigger)
SDP Pipeline #1: Bronze Transactions (streaming table)
    │
    ▼ (cleanse, deduplicate, watermark 10min)
SDP Pipeline #1: Silver Transactions (streaming table)
    │
    ▼ (stream-static join with customers)
SDP Pipeline #1: Silver Enriched Transactions (streaming table)
    │
    ▼ (stream-stream join with watchlist)
SDP Pipeline #1: Gold Flagged Transactions (streaming table)
    │
    ├──▶ foreachBatch → Email Alert (Gmail SMTP)
    └──▶ Gold Alert Log (Delta table)

Postgres (batch) → Lakeflow Connect → Bronze Customers → Silver Customers
                                                                     │ (stream-static join ▲)

JSON Files (drop) → Auto Loader → Bronze Fraud Watchlist → Silver Fraud Watchlist
                                                                     │ (stream-stream join ▲)

Gold Aggregations (tumbling/sliding windows) → Dashboard
```

---

## PHASE 14: DASHBOARD WITH NEAR REAL-TIME DATA REFRESH

### 14.1 Create an AI/BI Dashboard

Build a Databricks AI/BI Dashboard (Lakeview) with the following visualizations:

**Page 1: Real-Time Fraud Overview**
1. **KPI Cards:**
   - Total transactions (last 1 hour)
   - Flagged transactions (last 1 hour)
   - Fraud rate %
   - Total amount flagged ($)
2. **Line chart:** Transaction volume over time (5-min tumbling windows)
3. **Bar chart:** Flagged transactions by merchant category
4. **Map/scatter:** Flagged transactions by location (latitude/longitude)

**Page 2: Fraud Analysis**
5. **Table:** Latest 50 flagged transactions (transaction_id, customer, amount, merchant, risk_score, watchlist_match)
6. **Bar chart:** Top 10 high-risk customers (by risk_score)
7. **Line chart:** Fraud rate trend (hourly windows)
8. **Pie chart:** Flag distribution by channel (online, POS, mobile)

**Page 3: Alert Log**
9. **Table:** All alerts sent (timestamp, transaction_id, recipient, status)
10. **KPI:** Total alerts sent today

**Dashboard refresh:**
- Set auto-refresh interval: 10 seconds (near real-time)
- All queries run against gold-layer tables
- Use SQL warehouse (serverless) for dashboard queries

---

## PHASE 15: END-TO-END ORCHESTRATION WITH LAKEFLOW JOBS

### 15.1 Orchestration Architecture

Create a **Lakeflow Job** that orchestrates the entire FinGuard pipeline:

```
Lakeflow Job: FinGuard-E2E-Orchestration
├── Task 1: Start SDP Pipeline #1 (Kafka → Bronze → Silver → Gold Transactions + Alerts)
│   └── Type: Pipeline task
│   └── Pipeline: FinGuard-Transactions-Pipeline
│   └── Depends on: (none)
│
├── Task 2: Trigger Lakeflow Connect Ingestion (Postgres → Bronze Customers)
│   └── Type: Pipeline task (ingestion pipeline)
│   └── Depends on: (none)
│   └── Schedule: Daily at 2:00 AM (full refresh)
│
├── Task 3: Start SDP Pipeline #2 (Auto Loader → Bronze → Silver Watchlist)
│   └── Type: Pipeline task
│   └── Pipeline: FinGuard-Watchlist-Pipeline
│   └── Depends on: (none)
│
├── Task 4: Start Kafka Producer (if running in Databricks)
│   └── Type: Notebook task / Python script task
│   └── Depends on: (none)
│
├── Task 5: Refresh Dashboard Queries (SQL warehouse)
│   └── Type: SQL task / Notebook task
│   └── Depends on: Task 1 (after pipeline is running)
│
└── Task 6: Health Check + Alert (if any pipeline fails)
    └── Type: Notebook task (send failure email)
    └── Depends on: Task 1, Task 2, Task 3 (on failure)
```

### 15.2 Job Configuration

- **Job name:** `FinGuard-E2E-Orchestration`
- **Schedule:**
  - Task 1 & 3 (streaming pipelines): Start on job trigger, run continuously
  - Task 2 (Postgres batch): Daily at 2:00 AM
  - Task 4 (Kafka producer): Start on job trigger, run continuously
  - Task 5 (Dashboard refresh): Every 5 minutes
- **Compute:** Serverless for all tasks
- **Alerts:** Email notification on job failure
- **Retry policy:** 3 retries with 5-min delay
- **Timeout:** No timeout (continuous streaming)

### 15.3 Orchestration Flow

```
[Job Start]
    │
    ├──▶ Task 1: SDP Pipeline #1 (Streaming — runs continuously)
    │        └── Kafka → Bronze → Silver → Gold → Alerts
    │
    ├──▶ Task 2: Lakeflow Connect (Batch — daily 2AM)
    │        └── Postgres → Bronze Customers → Silver Customers
    │
    ├──▶ Task 3: SDP Pipeline #2 (Streaming — runs continuously)
    │        └── JSON Files → Bronze Watchlist → Silver Watchlist
    │
    ├──▶ Task 4: Kafka Producer (Runs continuously)
    │        └── Simulates transactions → Kafka topic
    │
    └──▶ Task 5: Dashboard Refresh (Every 5 min)
             └── Refreshes dashboard query cache
```

---

## PHASE 16: TESTING & VALIDATION CHECKLIST

### 16.1 End-to-End Testing Steps

1. **Kafka Producer:** Run producer → verify messages in Confluent Cloud UI
2. **SDP Pipeline #1:** Start pipeline → verify Bronze table has rows flowing in
3. **Bronze → Silver:** Verify cleansing, dedup, type casting working
4. **Stream-Static Join:** Verify customer enrichment columns populated
5. **Postgres + Lakeflow Connect:** Run ingestion → verify Bronze Customers table populated
6. **Auto Loader:** Drop JSON file → verify Bronze Watchlist table picks it up
7. **Stream-Stream Join:** Drop a watchlist JSON with matching merchant → verify `watchlist_match = true` in Gold table
8. **Email Alert:** Trigger a flagged transaction → verify email received
9. **Dashboard:** Open dashboard → verify KPIs updating in near real-time
10. **Lakeflow Job:** Trigger job → verify all tasks run and complete

### 16.2 Data Quality Checks

- [ ] No null transaction_ids in Silver layer
- [ ] No duplicate transaction_ids (dedup working)
- [ ] All amounts > 0 in Silver
- [ ] Customer enrichment populated for all transactions with valid customer_id
- [ ] Watchlist matches correctly flag transactions
- [ ] Email alerts sent for all flagged transactions
- [ ] Dashboard KPIs match Gold table counts
- [ ] Checkpoint locations are being written to
- [ ] Watermarks are dropping late data as expected
- [ ] No OOM errors in pipeline logs

---

## PHASE 17: PRODUCTION CONSIDERATIONS

### 17.1 Performance Optimization
- Use **liquid clustering** on Silver/Gold tables on `customer_id` and `timestamp`
- Partition Gold tables by `transaction_date` for efficient dashboard queries
- Enable **Z-Order** or **liquid clustering** on frequently filtered columns
- Use **serverless compute** for auto-scaling
- Set appropriate **shuffle partitions** for streaming joins
- Monitor **bytes behind latest** Kafka metric for lag detection

### 17.2 Monitoring
- Monitor SDP pipeline flow graphs and data quality metrics
- Set up **alerts** on pipeline failures via Lakeflow Jobs
- Monitor Kafka consumer lag (offsets behind latest)
- Monitor checkpoint location health
- Monitor email alert delivery rate

### 17.3 Security
- All credentials in Databricks Secret Scope — never hardcoded
- Use Unity Catalog for table-level RBAC
- Enable table ACLs for Bronze/Silver/Gold schemas
- Restrict Gold tables to fraud team + dashboard service principal
- Use service principals for job execution (not personal tokens)

---

## SUMMARY: COMPLETE BUILD ORDER

| Step | Phase | Component | Type |
|------|-------|-----------|------|
| 1 | 0 | Unity Catalog + Volumes + Secret Scope | Setup |
| 2 | 1 | Confluent Kafka Cluster + Topics | Kafka Setup |
| 3 | 1 | Kafka Producer (Python) | Data Simulation |
| 4 | 2 | Spark-Kafka Connection (readStream) | Streaming |
| 5 | 3 | Secret Scope (all credentials) | Security |
| 6 | 4 | SDP Pipeline #1: Kafka → Bronze Transactions | SDP Streaming |
| 7 | 4 | SDP: Bronze → Silver Transactions | SDP Streaming |
| 8 | 5 | Postgres + Lakeflow Connect (Customers) | Batch Ingestion |
| 9 | 5 | SDP: Bronze → Silver Customers | SDP Batch |
| 10 | 6 | Stream-Static Join (Txn × Customer) | Streaming Join |
| 11 | 7 | JSON Files + Auto Loader (Watchlist) | File Streaming |
| 12 | 7 | SDP Pipeline #2: Auto Loader → Bronze → Silver Watchlist | SDP Streaming |
| 13 | 8-9 | Stateless vs Stateful + Watermarking | Concepts |
| 14 | 10 | Stream-Stream Join (Txn × Watchlist) | Streaming Join |
| 15 | 10 | Gold: Flagged Transactions | Gold Layer |
| 16 | 11 | Gmail SMTP + Real-Time Email Alerts | Alerting |
| 17 | 12 | Windowed Aggregations (Tumbling/Sliding) | Gold Aggregations |
| 18 | 13 | End-to-End Data Flow Verification | Testing |
| 19 | 14 | AI/BI Dashboard (near real-time) | Visualization |
| 20 | 15 | Lakeflow Jobs Orchestration | Orchestration |
| 21 | 16 | End-to-End Testing + Validation | QA |
| 22 | 17 | Performance + Security + Monitoring | Production |

---

## KEY TECHNOLOGIES USED

| Technology | Purpose |
|---|---|
| Confluent Cloud Kafka | Real-time transaction streaming source |
| Apache Spark Structured Streaming | Stream processing engine |
| Databricks Lakeflow SDP (Spark Declarative Pipelines) | Medallion ETL pipelines |
| Databricks Auto Loader | Incremental JSON file ingestion |
| Databricks Lakeflow Connect | Managed Postgres → Unity Catalog ingestion |
| Databricks Secret Scope | Credential management |
| Unity Catalog | Governance + table management |
| Delta Lake | ACID storage format |
| Gmail SMTP (smtplib) | Real-time email alerts |
| Databricks AI/BI Dashboards | Near real-time visualization |
| Databricks Lakeflow Jobs | End-to-end orchestration |
| PostgreSQL | Customer master data (batch source) |

---

Now build this entire system step by step. Start with Phase 0 (workspace setup), then proceed through each phase sequentially. For each phase, provide the complete code, configuration, and explanations. Use Databricks best practices throughout.