# Databricks notebook source
# DBTITLE 1,Postgres SQL Setup
# MAGIC %md
# MAGIC # Postgres SQL Setup & Lakeflow Connect Ingestion
# MAGIC
# MAGIC Sets up a PostgreSQL database with historical customer master data, then uses Lakeflow Connect to ingest it into the Databricks bronze layer. This replaces the synthetic JSON-based customer ingestion with a real database source.
# MAGIC
# MAGIC **Video events covered:**
# MAGIC - `01:57:50` — Postgres SQL Setup
# MAGIC - `02:02:20` — Ingest historical data from Postgres using Lakeflow Connect
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. A Supabase Postgres instance (free tier works — the connection cell auto-discovers your project's pooler region)
# MAGIC 2. The Postgres DB password stored in secret scope `finguard-scope` under key `postgres_password`.
# MAGIC    Note: `dbutils.secrets.put()` is not available in notebooks — store it from a terminal:
# MAGIC    `databricks secrets put-secret finguard-scope postgres_password --string-value '<password>'`
# MAGIC 3. For alert emails: a 16-character Gmail App Password stored as `gmail_api_key`

# COMMAND ----------

# DBTITLE 1,Install pg8000 and restart Python (run first)
# MAGIC %pip install pg8000
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Generate Postgres DDL
# ── Generate Postgres DDL from existing customer schema ──
# Matches finguard.bronze.bronze_customers_manual structure (1,000 rows)

postgres_ddl = """
-- FinGuard Customer Master Data
CREATE TABLE IF NOT EXISTS customers (
    customer_id       VARCHAR(20) PRIMARY KEY,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    phone             VARCHAR(20),
    address           VARCHAR(255),
    city              VARCHAR(100),
    country           VARCHAR(100),
    account_type      VARCHAR(50),
    registration_date DATE,
    risk_profile      VARCHAR(20),
    is_active         BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_customers_city  ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_risk ON customers(risk_profile);
"""

print("=== POSTGRES DDL ===")
print(postgres_ddl)

# Verify schema matches existing Databricks data
print("=== Current Databricks Schema ===")
spark.sql("DESCRIBE finguard.bronze.bronze_customers_manual").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Export customer data to CSV
# ── Export customer data to CSV in UC volume for backup / Postgres import ──

csv_path = "/Volumes/finguard/bronze/raw_files/customers/postgres_export"

customers_df = spark.sql("SELECT * FROM finguard.bronze.bronze_customers_manual")
row_count = customers_df.count()

customers_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(csv_path)

# Find the generated CSV file
files = dbutils.fs.ls(csv_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]
print(f"✅ Exported {row_count} rows to {csv_file}")

# COMMAND ----------

# DBTITLE 1,Postgres connection configuration (Supabase pooler)
# ╔══════════════════════════════════════════════════════════════╗
# ║  POSTGRES CONNECTION CONFIGURATION (Supabase pooler)          ║
# ║  db.<ref>.supabase.co is IPv6-only and unreachable from      ║
# ║  serverless compute — connect via the IPv4 pooler instead.   ║
# ╚══════════════════════════════════════════════════════════════╝

PG_REF      = "hrncvobvdvneudrmflri"   # Supabase project ref (region: eu-west-1, Ireland)
PG_PORT     = "5432"                   # session-mode pooler port
PG_DATABASE = "postgres"               # Supabase default database name
PG_USER     = f"postgres.{PG_REF}"     # pooler username: postgres.<project-ref>

# ── Retrieve Postgres password from Databricks secrets ──
PG_PASSWORD = None
try:
    PG_PASSWORD = dbutils.secrets.get(scope="finguard-scope", key="postgres_password")
    print("✅ Postgres password retrieved from Databricks secrets")
except Exception:
    print("❌ Secret 'postgres_password' not found in scope 'finguard-scope'")
    print("   Store it from a terminal (dbutils.secrets.put is not available):")
    print("   databricks secrets put-secret finguard-scope postgres_password --string-value '<password>'")

# ── Auto-discover the pooler region by testing a real login ──
PG_HOST = None
if PG_PASSWORD:
    import pg8000, ssl
    # Supabase signs its Postgres certs with its own CA (self-signed root, not in
    # the system trust store). Keep TLS encryption but skip CA verification.
    # For strict verification, download the CA from the Supabase Connect dialog
    # and pass it as ssl.create_default_context(cafile=<path>) instead.
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE
    for region in ["eu-west-1",   # confirmed from Supabase dashboard (Primary Database: West EU, Ireland)
                   "eu-central-1", "us-east-1", "us-west-1",
                   "ap-southeast-1", "ap-northeast-1"]:
        host = f"aws-0-{region}.pooler.supabase.com"
        try:
            c = pg8000.connect(host=host, port=int(PG_PORT), database=PG_DATABASE,
                               user=PG_USER, password=PG_PASSWORD,
                               timeout=10, ssl_context=SSL_CTX)
            c.close()
            PG_HOST = host
            print(f"✅ Connected via {host}")
            break
        except Exception as e:
            print(f"   ✗ {host}: {str(e)[:100]}")
    if not PG_HOST:
        print("❌ No pooler accepted these credentials — check the password and project ref")

PG_READY = PG_HOST is not None

print(f"\nPostgres target: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
print(f"Postgres ready: {PG_READY}")

# COMMAND ----------

# DBTITLE 1,Create Postgres table and load data
# ── Load customer data into Postgres using pg8000 (pure-Python driver) ──
import pg8000

if not PG_READY:
    raise RuntimeError("Postgres is not ready — store the postgres_password secret and re-run the connection configuration cell, then this cell.")

# Connect to Postgres
conn = pg8000.connect(
    host=PG_HOST, port=int(PG_PORT), database=PG_DATABASE,
    user=PG_USER, password=PG_PASSWORD,
    timeout=30, ssl_context=SSL_CTX
)
conn.autocommit = True
cursor = conn.cursor()

# Create the customers table
cursor.execute(postgres_ddl)
print("✅ Table 'customers' created in Postgres")

# Fetch all customer data from Databricks
rows = spark.sql("""
    SELECT customer_id, first_name, last_name, email, phone, address,
           city, country, account_type, registration_date, risk_profile, is_active
    FROM finguard.bronze.bronze_customers_manual
""").collect()

data = [(
    r['customer_id'], r['first_name'], r['last_name'], r['email'],
    r['phone'], r['address'], r['city'], r['country'],
    r['account_type'], r['registration_date'], r['risk_profile'], r['is_active']
) for r in rows]

# Bulk insert with ON CONFLICT to handle re-runs
insert_sql = """
    INSERT INTO customers (customer_id, first_name, last_name, email, phone,
                          address, city, country, account_type, registration_date,
                          risk_profile, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING
"""
cursor.executemany(insert_sql, data)
conn.commit()

# Verify
cursor.execute("SELECT COUNT(*) FROM customers")
pg_count = cursor.fetchone()[0]
print(f"✅ Loaded {pg_count} customer records into Postgres")

cursor.execute("SELECT customer_id, first_name, last_name, city, risk_profile FROM customers LIMIT 5")
print("\n=== SAMPLE FROM POSTGRES ===")
for row in cursor.fetchall():
    print(f"  {row}")

cursor.close()
conn.close()

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog connection
# ── Create Unity Catalog connection to Postgres for Lakeflow Connect ──

import requests
import json

if not PG_READY:
    raise RuntimeError("Postgres is not ready — fix PG_HOST and/or postgres_password secret, then re-run cell 4 and this cell.")

notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = notebook_context.apiToken().get()
workspace_url = notebook_context.apiUrl().get()

headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

connection_spec = {
    "name": "finguard_postgres",
    "connection_type": "POSTGRESQL",
    "options": {
        "host": PG_HOST,
        "port": PG_PORT,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "trustServerCertificate": "true"   # Supabase CA is self-signed; Lakeflow Connect does not accept a 'database' option
    }
}

response = requests.post(
    f"{workspace_url}/api/2.1/unity-catalog/connections",
    headers=headers,
    json=connection_spec
)

if response.status_code == 200:
    conn_info = response.json()
    print(f"✅ UC connection created: {conn_info.get('name', 'finguard_postgres')}")
else:
    print(f"❌ Error ({response.status_code}): {response.text}")
    # If already exists, list to get connection name
    list_resp = requests.get(f"{workspace_url}/api/2.1/unity-catalog/connections", headers=headers)
    print(f"Existing connections: {list_resp.json()}")

# COMMAND ----------

# DBTITLE 1,Create Lakeflow Connect gateway and pipeline
# ── Create Lakeflow Connect ingestion gateway + pipeline (Postgres → Bronze) ──

if not PG_READY:
    raise RuntimeError("Postgres is not ready — fix PG_HOST and/or postgres_password secret, then re-run cell 4 and this cell.")

api_url = f"{workspace_url}/api/2.0/pipelines"

# Step 0: Resolve the connection's UUID (the gateway API requires it, not the name)
conn_resp = requests.get(f"{workspace_url}/api/2.1/unity-catalog/connections/finguard_postgres", headers=headers)
conn_json = conn_resp.json()
conn_id = conn_json.get("connection_id") or conn_json.get("id") or "finguard_postgres"
print(f"Using connection id: {conn_id}")

# Step 1: Create the ingestion gateway (runs continuously to capture CDC)
gateway_spec = {
    "name": "finguard_postgres_gateway",
    "catalog": "finguard",
    "schema": "bronze",
    "gateway_definition": {
        "connection_id": conn_id,
        "gateway_storage_catalog": "finguard",
        "gateway_storage_schema": "bronze",
        "gateway_storage_name": "finguard_postgres_gateway"
    }
}

gw_response = requests.post(url=api_url, headers=headers, json=gateway_spec)
print(f"Gateway response ({gw_response.status_code}): {gw_response.json()}")
gateway_id = gw_response.json().get("pipeline_id") if gw_response.status_code == 200 else None

# Step 2: Create the ingestion pipeline (Postgres customers → bronze_customers_pg)
pipeline_spec = {
    "name": "finguard_postgres_ingestion",
    "catalog": "finguard",
    "schema": "bronze",
    "ingestion_definition": {
        "ingestion_gateway_id": gateway_id,
        "objects": [
            {
                "table": {
                    "source_schema": "public",
                    "source_table": "customers",
                    "destination_catalog": "finguard",
                    "destination_schema": "bronze",
                    "destination_table": "bronze_customers_pg"
                }
            }
        ]
    }
}

pipeline_id = None
if gateway_id:
    pipe_response = requests.post(url=api_url, headers=headers, json=pipeline_spec)
    print(f"Pipeline response ({pipe_response.status_code}): {pipe_response.json()}")
    pipeline_id = pipe_response.json().get("pipeline_id")

# Step 3: Start the pipeline
if pipeline_id:
    start_response = requests.post(
        f"{api_url}/{pipeline_id}/updates",
        headers=headers,
        json={"full_refresh": False, "validate_only": False, "cause": "API_CALL"}
    )
    print(f"Start response ({start_response.status_code}): {start_response.json()}")
    print(f"\n✅ Pipeline ID: {pipeline_id}")
    print(f"Gateway ID: {gateway_id}")
    print(f"Target table: finguard.bronze.bronze_customers_pg")
else:
    # Lakeflow Connect managed PostgreSQL ingestion is not enabled in this workspace
    # (403 UNSUPPORTED_FEATURE_FOR_WORKSPACE — only Databricks support can enable it).
    # Lakehouse Federation over the SAME finguard_postgres connection lands the same
    # data in the same target table.
    print("ℹ️ Managed PostgreSQL ingestion not enabled in this workspace — using Lakehouse Federation fallback")
    spark.sql("CREATE FOREIGN CATALOG IF NOT EXISTS finguard_pg USING CONNECTION finguard_postgres OPTIONS (database 'postgres')")
    spark.sql("""
        CREATE OR REPLACE TABLE finguard.bronze.bronze_customers_pg AS
        SELECT customer_id, first_name, last_name, email, phone, address,
               city, country, account_type,
               CAST(registration_date AS DATE) AS registration_date,
               risk_profile, is_active
        FROM finguard_pg.public.customers
    """)
    n = spark.sql("SELECT COUNT(*) AS n FROM finguard.bronze.bronze_customers_pg").first()["n"]
    print(f"✅ finguard.bronze.bronze_customers_pg created via federation — {n} rows")