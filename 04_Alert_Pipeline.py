# Databricks notebook source
# DBTITLE 1,Alert Pipeline Title
# MAGIC %md
# MAGIC # Alert Pipeline - Fraud Detection Email Alerts
# MAGIC
# MAGIC Reads HIGH severity flagged transactions from `gold_flagged_transactions`, generates alert records in `gold_alert_log`, and sends email notifications to the fraud team via Gmail SMTP.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Update `gmail_api_key` secret in `finguard-scope` with your actual Gmail app password
# MAGIC 2. Add `gmail_sender_email` secret with the sender Gmail address
# MAGIC 3. Add `gmail_recipient_email` secret with the fraud team's email address
# MAGIC
# MAGIC Until configured, alerts will be created with PENDING status.

# COMMAND ----------

# DBTITLE 1,Load Gmail Credentials from Secret Scope
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==========================================
# Load Gmail credentials from secret scope
# ==========================================

gmail_configured = True
config_issues = []

# Gmail app password (stored as gmail_api_key)
try:
    gmail_app_password = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")
    if gmail_app_password == "YOUR_GMAIL_API_KEY_HERE":
        config_issues.append("gmail_api_key is still placeholder — update with real app password")
        gmail_configured = False
except Exception:
    config_issues.append("gmail_api_key secret not found")
    gmail_app_password = None
    gmail_configured = False

# Sender email
try:
    sender_email = dbutils.secrets.get(scope="finguard-scope", key="gmail_sender_email")
except Exception:
    config_issues.append("gmail_sender_email secret not found")
    sender_email = None
    gmail_configured = False

# Recipient email (fraud team)
try:
    recipient_email = dbutils.secrets.get(scope="finguard-scope", key="gmail_recipient_email")
except Exception:
    config_issues.append("gmail_recipient_email secret not found")
    recipient_email = None
    gmail_configured = False

if config_issues:
    print("⚠️  Gmail configuration issues:")
    for issue in config_issues:
        print(f"   - {issue}")
    print("\nAlerts will be created with PENDING status until credentials are configured.")
else:
    print("✅ Gmail credentials loaded successfully.")

# COMMAND ----------

# DBTITLE 1,Define Email Sending Function
# ==========================================
# Email sending function
# ==========================================

def send_fraud_alert(sender, app_password, recipient, txn, ts_str):
    """Send a fraud alert email for a single flagged transaction.
    
    Args:
        sender: Sender email address
        app_password: Gmail app password
        recipient: Recipient email address
        txn: Dict of transaction fields
        ts_str: Formatted transaction timestamp string
    
    Returns:
        None (raises exception on failure)
    """
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = (
        f"[FinGuard HIGH] Fraud Alert - {txn['transaction_id']} "
        f"(Risk: {txn['risk_score']}, Amount: {txn['amount']} {txn['currency']})"
    )

    html_body = f"""
    <html><body>
    <h2>🚨 FinGuard Fraud Alert</h2>
    <p><b>Alert Severity:</b> {txn['alert_severity']}</p>
    <p><b>Risk Score:</b> {txn['risk_score']}</p>
    <p><b>Fraud Reason:</b> {txn['fraud_reason']}</p>
    <hr>
    <h3>Transaction Details</h3>
    <table border="1" cellpadding="5" style="border-collapse: collapse;">
      <tr><td><b>Transaction ID</b></td><td>{txn['transaction_id']}</td></tr>
      <tr><td><b>Customer ID</b></td><td>{txn['customer_id']}</td></tr>
      <tr><td><b>Amount</b></td><td>{txn['amount']} {txn['currency']}</td></tr>
      <tr><td><b>Merchant</b></td><td>{txn['merchant_name']} ({txn['merchant_category']})</td></tr>
      <tr><td><b>Type</b></td><td>{txn['transaction_type']} via {txn['payment_channel']}</td></tr>
      <tr><td><b>Location</b></td><td>{txn.get('txn_city', 'N/A')}, {txn.get('txn_country', 'N/A')}</td></tr>
      <tr><td><b>International</b></td><td>{'Yes' if txn['is_international'] else 'No'}</td></tr>
      <tr><td><b>Timestamp</b></td><td>{ts_str}</td></tr>
      <tr><td><b>Status</b></td><td>{txn['status']}</td></tr>
    </table>
    <p><i>This is an automated alert from the FinGuard Fraud Detection System.</i></p>
    </body></html>
    """

    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender, app_password)
        server.sendmail(sender, recipient, msg.as_string())

print("Email function `send_fraud_alert()` defined.")

# COMMAND ----------

# DBTITLE 1,Generate and Send Fraud Alerts
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# ==========================================
# ALERT PIPELINE: Generate and send fraud alerts
# ==========================================

ALERT_SEVERITY = "HIGH"   # Only alert on HIGH severity
MAX_ALERTS_PER_RUN = 10   # Limit emails per run to avoid spam

# --- Ensure gold_alert_log is a Delta table (not a materialized view) ---
# The gold_aggregations pipeline may recreate gold_alert_log as a materialized view,
# which cannot be UPDATEd or appended to. Convert it back to a Delta table.
try:
    _obj_type = spark.sql("""
        SELECT table_type FROM system.information_schema.tables
        WHERE table_catalog = 'finguard' AND table_schema = 'gold'
          AND table_name = 'gold_alert_log'
    """).collect()
    if _obj_type and _obj_type[0][0] == 'MATERIALIZED_VIEW':
        print("gold_alert_log is a materialized view — dropping and recreating as Delta table...")
        spark.sql("DROP MATERIALIZED VIEW finguard.gold.gold_alert_log")
        spark.sql("""
            CREATE TABLE finguard.gold.gold_alert_log (
                alert_id STRING,
                transaction_id STRING,
                customer_id STRING,
                amount DOUBLE,
                risk_score DOUBLE,
                alert_severity STRING,
                fraud_reason STRING,
                alert_status STRING,
                sent_at TIMESTAMP,
                error_message STRING
            )
        """)
        print("Recreated gold_alert_log as Delta table.")
    elif not _obj_type:
        print("gold_alert_log does not exist — creating Delta table...")
        spark.sql("""
            CREATE TABLE finguard.gold.gold_alert_log (
                alert_id STRING,
                transaction_id STRING,
                customer_id STRING,
                amount DOUBLE,
                risk_score DOUBLE,
                alert_severity STRING,
                fraud_reason STRING,
                alert_status STRING,
                sent_at TIMESTAMP,
                error_message STRING
            )
        """)
        print("Created gold_alert_log Delta table.")
except Exception as _e:
    print(f"Warning: could not verify/convert gold_alert_log: {_e}")

# --- Step 0: Retry previously PENDING alerts (e.g. sent before Gmail was configured) ---
try:
    # PENDING = never sent. FAILED with 'city' in the error = delivery failure caused by
    # the renamed-column bug (city → txn_city, fixed in send_fraud_alert) — safe to retry.
    pending_rows = (
        spark.read.table("finguard.gold.gold_alert_log")
        .filter(
            (col("alert_status") == "PENDING")
            | ((col("alert_status") == "FAILED") & col("error_message").contains("city"))
        )
        .collect()
    )
except Exception:
    pending_rows = []

if pending_rows and gmail_configured:
    print(f"Retrying {len(pending_rows)} PENDING alerts...")
    retried_sent, retried_failed = 0, 0
    for row in pending_rows:
        try:
            txn_row = (
                spark.read.table("finguard.gold.gold_flagged_transactions")
                .filter(col("transaction_id") == row.transaction_id)
                .limit(1)
                .collect()
            )
            if not txn_row:
                raise RuntimeError("transaction not found in gold_flagged_transactions")
            txn_dict = txn_row[0].asDict()
            txn_ts = txn_dict.get("txn_timestamp")
            ts_str = txn_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if txn_ts else "N/A"
            send_fraud_alert(sender_email, gmail_app_password, recipient_email, txn_dict, ts_str)
            new_status, new_err = "SENT", None
            retried_sent += 1
        except Exception as e:
            new_status, new_err = "FAILED", str(e)[:200]
            retried_failed += 1
        err_sql = "NULL" if new_err is None else "'{}'".format(new_err.replace("'", "''"))
        try:
            spark.sql(f"""
                UPDATE finguard.gold.gold_alert_log
                SET alert_status = '{new_status}', error_message = {err_sql}, sent_at = current_timestamp()
                WHERE alert_id = '{row.alert_id}'
            """)
        except Exception as update_err:
            print(f"  Failed to update alert log for {row.transaction_id}: {update_err}")
    print(f"  Retry result — SENT: {retried_sent} | FAILED: {retried_failed}")
elif pending_rows:
    print(f"{len(pending_rows)} alerts remain PENDING — Gmail credentials not configured")

# Read HIGH severity flagged transactions (highest risk first)
high_severity_txns = (
    spark.read.table("finguard.gold.gold_flagged_transactions")
    .filter(col("alert_severity") == ALERT_SEVERITY)
    .orderBy(col("risk_score").desc(), col("amount").desc())
)

# --- Duplicate prevention: exclude transactions that already have alerts ---
try:
    existing_alerts = spark.read.table("finguard.gold.gold_alert_log")
    existing_txn_ids = [
        row.transaction_id
        for row in existing_alerts.select("transaction_id").distinct().collect()
    ]
except Exception:
    existing_txn_ids = []

if existing_txn_ids:
    new_txns = high_severity_txns.filter(~col("transaction_id").isin(existing_txn_ids))
else:
    new_txns = high_severity_txns

# Limit to MAX_ALERTS_PER_RUN
new_txns_limited = new_txns.limit(MAX_ALERTS_PER_RUN)

# Collect to driver for email sending
txn_rows = new_txns_limited.collect()

if not txn_rows:
    print("No new HIGH severity transactions to alert on. All existing alerts are up to date.")
else:
    alerts = []
    sent_count = 0
    pending_count = 0
    failed_count = 0

    for txn in txn_rows:
        alert_id = str(uuid.uuid4())
        txn_dict = txn.asDict()

        # Format timestamp for email
        txn_ts = txn_dict.get("txn_timestamp")
        ts_str = txn_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if txn_ts else "N/A"

        try:
            if gmail_configured:
                send_fraud_alert(
                    sender_email, gmail_app_password,
                    recipient_email, txn_dict, ts_str
                )
                alert_status = "SENT"
                error_msg = None
                sent_count += 1
            else:
                alert_status = "PENDING"
                error_msg = "Gmail credentials not configured"
                pending_count += 1
        except Exception as e:
            alert_status = "FAILED"
            error_msg = str(e)
            failed_count += 1

        alerts.append((
            alert_id,
            txn_dict["transaction_id"],
            txn_dict["customer_id"],
            float(txn_dict["amount"]),
            float(txn_dict["risk_score"]),
            txn_dict["alert_severity"],
            txn_dict["fraud_reason"],
            alert_status,
            datetime.now(timezone.utc),
            error_msg,
        ))

    # Write alerts to gold_alert_log (append mode)
    alert_schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("alert_severity", StringType(), True),
        StructField("fraud_reason", StringType(), True),
        StructField("alert_status", StringType(), True),
        StructField("sent_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ])

    alert_df = spark.createDataFrame(alerts, schema=alert_schema)
    try:
        alert_df.write.mode("append").saveAsTable("finguard.gold.gold_alert_log")
    except Exception as write_err:
        print(f"Failed to append alerts to gold_alert_log: {write_err}")

    print(f"Alerts generated: {len(alerts)}")
    print(f"  SENT: {sent_count} | PENDING: {pending_count} | FAILED: {failed_count}")
    if failed_count > 0:
        print("\nFailed alert details:")
        for a in alerts:
            if a[7] == "FAILED":
                print(f"  {a[1]}: {a[9]}")

# COMMAND ----------

# DBTITLE 1,Alert Log Verification
from pyspark.sql.functions import col

# ==========================================
# Verify alert log
# ==========================================

print("=== Gold Alert Log ===")
total_alerts = spark.read.table("finguard.gold.gold_alert_log").count()
print(f"Total alerts: {total_alerts}")

if total_alerts > 0:
    print("\nBy status:")
    spark.read.table("finguard.gold.gold_alert_log").groupBy("alert_status").count().orderBy(col("alert_status").desc()).show()

    print("Recent alerts:")
    display(
        spark.read.table("finguard.gold.gold_alert_log")
        .select("alert_id", "transaction_id", "amount", "risk_score", "alert_status", "error_message")
        .orderBy(col("sent_at").desc())
        .limit(10)
    )
else:
    print("No alerts in log yet.")

# COMMAND ----------

