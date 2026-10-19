# Databricks notebook source
# DBTITLE 1,Refresh bronze_customers_pg from Supabase
# MAGIC %md
# MAGIC # Refresh bronze_customers_pg from Supabase
# MAGIC
# MAGIC Daily refresh: re-lands the Supabase `public.customers` master data (1,000 customers) into `finguard.bronze.bronze_customers_pg` via Lakehouse Federation over the `finguard_postgres` UC connection. Replaces the Lakeflow Connect managed ingestion, which is not enabled in this workspace.

# COMMAND ----------

# DBTITLE 1,Refresh bronze table from Supabase
# Re-land Supabase customers into the bronze layer (full snapshot refresh)
spark.sql("""
    CREATE OR REPLACE TABLE finguard.bronze.bronze_customers_pg AS
    SELECT customer_id, first_name, last_name, email, phone, address,
           city, country, account_type,
           CAST(registration_date AS DATE) AS registration_date,
           risk_profile, is_active
    FROM finguard_pg.public.customers
""")

n = spark.sql("SELECT COUNT(*) AS n FROM finguard.bronze.bronze_customers_pg").first()["n"]
print(f"✅ Refreshed finguard.bronze.bronze_customers_pg — {n} rows")
if n == 0:
    raise RuntimeError("Refresh produced 0 rows — check the finguard_pg foreign catalog and Supabase availability")

# COMMAND ----------

