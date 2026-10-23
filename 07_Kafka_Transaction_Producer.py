# Databricks notebook source
# DBTITLE 1,Kafka Transaction Producer
# MAGIC %md
# MAGIC # Kafka Transaction Producer
# MAGIC
# MAGIC Generates synthetic credit card transactions matching the existing `credit_card_transactions` message schema and publishes them to the Confluent Cloud Kafka topic. Kafka credentials are read at runtime from the `finguard-scope` secret scope — nothing is hardcoded. Set `batch_size` in the first cell and re-run all cells to send more transactions.

# COMMAND ----------

# DBTITLE 1,Configuration and Secrets
import json

# ------------------------------------------------------------------
# Parameters — change batch_size and re-run all cells to send more.
# ------------------------------------------------------------------
batch_size = 100  # messages per run (the scheduled producer job runs this every 15 min)
run_verification = False  # True = interactive runs also wait for bronze ingestion (not for scheduled runs)

# Kafka connection details are read at runtime from the Databricks secret
# scope `finguard-scope` (key: kafka_connection_details). Credentials are
# never hardcoded in this notebook.
_kafka_cfg = json.loads(
    dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
)
bootstrap_server = _kafka_cfg.get("bootstrap_server") or _kafka_cfg.get("bootstrap_servers")
topic_name = _kafka_cfg["topic_name"]
api_key = _kafka_cfg["api_key"]
api_secret = _kafka_cfg["api_secret"]

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f'required username="{api_key}" password="{api_secret}";'
)

print(f"batch_size = {batch_size}")
print(f"bootstrap_server = {bootstrap_server}")
print(f"topic = {topic_name}")
print(f"api_key = {api_key[:4]}{'*' * (len(api_key) - 4)} (rest hidden)")

# COMMAND ----------

# DBTITLE 1,Load Reference Data
from pyspark.sql import functions as F

# Reference data pulled from existing tables so generated transactions stay
# consistent with the real data (merchant joins, customer joins, no id
# collisions with existing transaction ids).
merchant_list = [
    (r["merchant_id"], r["merchant_name"], r["merchant_category"])
    for r in spark.table("finguard.bronze.bronze_transactions")
    .select("merchant_id", "merchant_name", "merchant_category")
    .distinct()
    .collect()
]

customer_ids = [
    r["customer_id"]
    for r in spark.table("finguard.silver.silver_customers")
    .select("customer_id")
    .distinct()
    .orderBy("customer_id")
    .collect()
]

existing_txn_ids = {
    r["transaction_id"]
    for r in spark.table("finguard.bronze.bronze_transactions")
    .select("transaction_id")
    .collect()
}

print(
    f"Loaded {len(merchant_list)} merchants, {len(customer_ids)} customers, "
    f"and {len(existing_txn_ids)} existing transaction ids for collision-free generation"
)

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Transactions
import random
from datetime import datetime, timezone

# Distributions observed in the existing topic data (checked 2026-10-20):
# 11 merchant categories; ~96% of transactions in India; amounts roughly
# uniform between ~3 and ~100,000 in every currency; device id follows the
# customer number (CUST000228 -> DEV0228); statuses are all APPROVED.
COUNTRY_WEIGHTS = [
    ("India", 28666), ("UAE", 589), ("Singapore", 466),
    ("United Kingdom", 155), ("United States", 28),
]
CITIES_BY_COUNTRY = {
    "India": ["Ahmedabad", "Bengaluru", "Chennai", "Delhi", "Hyderabad",
              "Jaipur", "Kolkata", "Lucknow", "Mumbai", "Pune"],
    "UAE": ["Dubai"],
    "Singapore": ["Singapore"],
    "United Kingdom": ["London"],
    "United States": ["New York"],
}
CURRENCY_WEIGHTS = [("INR", 27984), ("USD", 993), ("EUR", 927)]
TXN_TYPES = ["ONLINE", "PURCHASE"]
CHANNELS = ["INTERNET_BANKING", "MOBILE_APP", "POS"]


def generate_transactions(n):
    txns, used_ids = [], set()
    while len(txns) < n:
        txn_id = f"TXN{random.randint(100000, 999999)}"
        if txn_id in existing_txn_ids or txn_id in used_ids:
            continue
        used_ids.add(txn_id)

        customer_id = random.choice(customer_ids)
        merchant_id, merchant_name, merchant_category = random.choice(merchant_list)
        country = random.choices(
            [c for c, _ in COUNTRY_WEIGHTS], weights=[w for _, w in COUNTRY_WEIGHTS]
        )[0]
        currency = random.choices(
            [c for c, _ in CURRENCY_WEIGHTS], weights=[w for _, w in CURRENCY_WEIGHTS]
        )[0]

        txns.append({
            "transaction_id": txn_id,
            "customer_id": customer_id,
            "card_number": str(random.randint(10 ** 15, 10 ** 16 - 1)),
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "merchant_category": merchant_category,
            "amount": round(random.uniform(3.0, 100000.0), 2),
            "currency": currency,
            "transaction_type": random.choice(TXN_TYPES),
            "payment_channel": random.choice(CHANNELS),
            "device_id": f"DEV{customer_id[-4:]}",
            "city": random.choice(CITIES_BY_COUNTRY[country]),
            "country": country,
            "transaction_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "is_international": country != "India",
            "status": "APPROVED",
        })
    return txns


generated = generate_transactions(batch_size)
print(f"Generated {len(generated)} synthetic transactions "
      f"({generated[0]['transaction_id']} ... {generated[-1]['transaction_id']})")
print(json.dumps(generated[0], indent=2))

# COMMAND ----------

# DBTITLE 1,Publish to Kafka
# Baseline bronze count BEFORE publishing (used later to verify ingestion)
bronze_baseline = spark.table("finguard.bronze.bronze_transactions").count()
print(f"Bronze baseline before publish: {bronze_baseline} rows")

# Publish: key = transaction_id, value = full JSON message (schema matches
# the existing credit_card_transactions messages exactly)
message_rows = [(t["transaction_id"], json.dumps(t)) for t in generated]
messages_df = spark.createDataFrame(message_rows, "key STRING, value STRING")

messages_df.write.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("topic", topic_name) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .save()

print(f"Published {len(message_rows)} messages to topic {topic_name}")

# COMMAND ----------

# DBTITLE 1,Verify Delivery (Kafka Topic)
# Verify delivery at the Kafka topic level: read the whole topic back and
# check that the ids we just sent appear in the newest messages.
# Skipped when run_verification is False (scheduled producer runs — the
# whole-topic re-read would grow more expensive as the topic grows).
if run_verification:
    topic_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic_name)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .load()
    )
    total_in_topic = topic_df.count()

    sent_ids = {t["transaction_id"] for t in generated}
    tail_ids = {
        r["transaction_id"]
        for r in topic_df
        .orderBy(F.col("timestamp").desc())
        .limit(len(sent_ids) + 50)
        .select(
            F.regexp_extract(F.col("value").cast("string"),
                             r'"transaction_id":\s*"([^"]+)"', 1).alias("transaction_id")
        )
        .collect()
    }
    delivered = len(sent_ids & tail_ids)
    print(f"Total messages in topic {topic_name}: {total_in_topic}")
    print(f"Delivery check: {delivered}/{len(sent_ids)} sent transaction ids found in the "
          f"newest {len(sent_ids) + 50} topic messages")
else:
    print("run_verification is False — skipping the Kafka topic delivery check.")

# COMMAND ----------

# DBTITLE 1,Verify Bronze Ingestion
# Interactive verification: polls until the next scheduled pipeline update
# lands this batch in finguard.bronze.bronze_transactions. Skipped when
# run_verification is False (e.g. in the scheduled producer job, which must
# not sit waiting ~15-20 minutes inside every run).
import time

if run_verification:
    expected = bronze_baseline + len(generated)
    deadline = time.time() + 20 * 60  # wait up to 20 minutes
    bronze_count = None
    while time.time() < deadline:
        bronze_count = spark.table("finguard.bronze.bronze_transactions").count()
        if bronze_count >= expected:
            break
        time.sleep(60)

    print(f"Bronze rows: {bronze_count} (was {bronze_baseline}, expected {expected})")
    if bronze_count is not None and bronze_count >= expected:
        print("SUCCESS: the bronze pipeline ingested the new transactions.")
        display(
            spark.table("finguard.bronze.bronze_transactions")
            .orderBy(F.col("transaction_timestamp").desc())
            .limit(5)
        )
    else:
        print("The next scheduled pipeline update has not landed yet — "
              "re-run this cell after a few more minutes.")
else:
    print("run_verification is False — skipping the bronze polling wait.")

# COMMAND ----------

# DBTITLE 1,Quick Bronze Check
# Quick re-check (no waiting): current bronze row count + newest 5 rows
bronze_count_now = spark.table("finguard.bronze.bronze_transactions").count()
print(f"Bronze rows now: {bronze_count_now}")
display(
    spark.table("finguard.bronze.bronze_transactions")
    .select("transaction_id", "customer_id", "merchant_category", "amount",
            "currency", "city", "transaction_timestamp")
    .orderBy(F.col("transaction_timestamp").desc())
    .limit(5)
)