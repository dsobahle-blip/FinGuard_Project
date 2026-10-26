# FinGuard — Real-Time Credit Card Fraud Detection

**Real-time credit card fraud detection pipeline on Databricks**  
Kafka streaming → Medallion Architecture → Multi-rule risk scoring → Automated alerts + AI/BI dashboards

---

## Project Overview

FinGuard is an end-to-end **streaming lakehouse** solution that detects fraudulent credit card transactions in near real-time.

It ingests live transaction events from **Confluent Cloud Kafka**, processes them through a **Bronze → Silver → Gold** medallion architecture on Databricks, scores every transaction using a weighted multi-rule fraud engine, and automatically sends email alerts for high-risk activity.

A scheduled orchestration job keeps the entire pipeline running every 15 minutes.

---

## Architecture


**Key Components**
- **Catalog:** `finguard` (schemas: `bronze`, `silver`, `gold`)
- **Compute:** Serverless + Photon-enabled Spark Declarative Pipelines
- **Orchestration:** Databricks Workflow (6 chained tasks every 15 min)

---

## Fraud Detection Engine

Every transaction is scored against **8 weighted rules**. Risk score ≥ 60 is flagged.

| Rule                  | Condition                                      | Weight |
|-----------------------|------------------------------------------------|--------|
| HIGH_VALUE            | Currency-specific high amount thresholds         | +40    |
| INTERNATIONAL         | Foreign transaction                            | +30    |
| HIGH_RISK_CATEGORY    | Jewellery / Electronics / Travel               | +20    |
| ONLINE_HIGH_VALUE     | Online + high value                            | +10    |
| VELOCITY              | ≥ 8 transactions in 10 minutes per card        | +50    |
| UNUSUAL_LOCATION      | Transaction city ≠ customer home city          | +15    |
| CUSTOMER_HIGH_RISK    | Customer risk profile = high                   | +30    |
| WATCHLIST_MATCH       | Customer or merchant on fraud watchlist        | +60    |

**Severity Levels**
- **HIGH** → Risk ≥ 80
- **MEDIUM** → Risk ≥ 60

**Live Performance** (sample run):  
~12.8% of transactions flagged (HIGH ≈ 2.2K | MEDIUM ≈ 1.6K out of ~29.4K transactions)

---

## Repository Structure





---

## Gold Layer Tables

| Table                             | Purpose                                              |
|-----------------------------------|------------------------------------------------------|
| `gold_flagged_transactions`       | Every flagged txn with rules hit + risk score        |
| `gold_fraud_summary`              | 5-min tumbling fraud summary by merchant category    |
| `gold_customer_risk_summary`      | Sliding window (10 min) customer risk                |
| `gold_hourly_fraud_rate`          | Hourly flagged vs total ratio                        |
| `gold_hourly_volume`              | Hourly transaction volume                            |
| `gold_merchant_category_summary`  | Spend + risk by merchant category                    |
| `gold_customer_summary`           | Per-customer spend profile                           |
| `gold_high_value_transactions`    | Transactions above value threshold                   |
| `gold_alert_log`                  | Alert dispatch status (SENT / PENDING / FAILED)      |

---

## Tech Stack

| Category          | Technology                                      |
|-------------------|-------------------------------------------------|
| Streaming         | Apache Spark Structured Streaming + Kafka       |
| Lakehouse         | Delta Lake + Unity Catalog                      |
| Compute           | Databricks Serverless + Photon                  |
| Orchestration     | Databricks Workflows / Lakeflow Jobs            |
| External Systems  | Confluent Cloud, Supabase (PostgreSQL), Gmail   |
| Visualization     | Databricks AI/BI Dashboard                      |

---

## Setup & Security

1. All credentials are stored in Databricks **Secret Scope** (`finguard-scope`) — never hardcoded.
2. Run `02_Setup_Secret_Scope.py` first.
3. Deploy the Spark Declarative Pipelines.
4. Schedule the end-to-end orchestration job.

> **Security Note:** Any credentials previously stored directly in notebooks should be rotated.

---

## Author

**Dumisani Sobahle**  
Advanced Data Analyst | Data Engineering | Microsoft Fabric & Databricks  

[LinkedIn](https://www.linkedin.com/in/dumisani-sobahle-81aa48144)

---

⭐ If you find this project useful, consider giving it a star!
