# FinGuard_Project

**Real-time credit card fraud detection on Databricks** — a streaming lakehouse pipeline that ingests transaction events from Confluent Cloud Kafka, processes them through a medallion architecture, scores every transaction against a rules-based fraud engine, and sends email alerts for high-risk activity.

## Overview

FinGuard streams credit card transactions through bronze → silver → gold lakehouse layers, flags suspicious activity with a weighted multi-rule fraud engine, and maintains operational gold tables for merchant, customer, and hourly fraud analytics. A scheduled orchestration job keeps the whole flow running end-to-end every 15 minutes.

## Architecture

```
Confluent Cloud Kafka (credit_card_transactions)
        │  Structured Streaming (trigger=availableNow)
        ▼
┌──────────────┐   ┌────────────────────────────────┐
│ BRONZE       │──▶│ SILVER                          │
│ raw events   │   │ parsed JSON, validated,         │
│ + customers  │   │ deduplicated, enriched with     │
│ + watchlist  │   │ customer profile + watchlist    │
└──────────────┘   └───────────────┬────────────────┘
                                   ▼
┌──────────────────────────────────────────────────┐
│ GOLD (Spark Declarative Pipelines)               │
│  • fraud engine: 8 weighted rules → risk score   │
│  • flagged transactions (HIGH / MEDIUM / LOW)    │
│  • fraud summaries, hourly volume & fraud rate   │
│  • merchant / customer / risk aggregates         │
│  • alert log → Gmail SMTP notifications          │
└──────────────────────────────────────────────────┘
```

- **Catalog:** `finguard` (schemas: `bronze`, `silver`, `gold`)
- **Compute:** serverless, Photon-enabled Spark Declarative Pipelines
- **Orchestration:** [Lakeflow Job](docs/FinGuard_E2E_Project_Prompt.md) with 6 chained tasks running every 15 minutes — ingest → silver → customer sync → watchlist sync → gold aggregations → alert dispatch

## Fraud Detection Engine

Every transaction is scored against 8 weighted rules; risk ≥ 60 gets flagged:

| Rule | Condition | Weight |
|---|---|---|
| HIGH_VALUE | per-currency threshold (INR ≥ 95K, USD ≥ 1200, EUR ≥ 1100) | +40 |
| INTERNATIONAL | foreign transaction | +30 |
| HIGH_RISK_CATEGORY | Jewellery / Electronics / Travel | +20 |
| ONLINE_HIGH_VALUE | online + high value | +10 |
| VELOCITY | ≥ 8 txns per 10 min per card | +50 |
| UNUSUAL_LOCATION | txn city ≠ customer home city | +15 |
| CUSTOMER_HIGH_RISK | risk_profile = high | +30 |
| WATCHLIST_MATCH | customer/merchant on fraud watchlist | +60 |

**Severity:** HIGH ≥ 80, MEDIUM ≥ 60. Flagging rate in live runs: ~12.8% (HIGH ≈ 2.2K, MEDIUM ≈ 1.6K of ~29.4K transactions).

## Repository Structure

```
├── notebooks/            # Databricks notebooks (Python source)
│   ├── 01_Kafka_Streaming_Test.py      # Kafka connectivity & streaming read test
│   ├── 02_Setup_Secrete_Scope.py       # Databricks secret scope provisioning
│   ├── 03_Medallion_Architecture.py    # Bronze/silver/gold streaming pipeline
│   ├── 04_Alert_Pipeline.py            # Gmail SMTP fraud alert dispatch
│   ├── 05_FinGuard_Architecture_Trace.py # End-to-end architecture walkthrough
│   ├── 06_Postgres_Setup.py            # Supabase/Postgres ingestion + federation
│   ├── 07_Kafka_Transaction_Producer.py # Synthetic transaction generator → Kafka
│   ├── Refresh_bronze_customers_pg_from_Supabase.py # Daily customer refresh job
│   └── FinGuard_Executive_Presentation.py # Executive summary presentation
├── dashboards/
│   └── finguard_executive_fraud_intelligence.lvdash.json  # AI/BI dashboard (importable)
├── pipelines/            # Spark Declarative Pipeline transformations
│   ├── kafka_bronze/     # bronze_transactions (Auto Loader from Kafka)
│   ├── silver/           # silver_transactions, silver_enriched_transactions
│   ├── customer/         # bronze_customers, silver_customers
│   ├── watchlist/        # bronze_fraud_watchlist, silver_fraud_watchlist
│   └── gold/             # 9 gold tables (fraud engine + aggregates + alert log)
└── docs/
    └── FinGuard_E2E_Project_Prompt.md  # Full end-to-end project specification
```

## Gold Tables

| Table | Purpose |
|---|---|
| `gold_flagged_transactions` | Every flagged transaction with rule hits, risk score, severity |
| `gold_fraud_summary` | 5-min tumbling fraud summary by merchant category |
| `gold_customer_risk_summary` | Sliding-window (10 min) per-customer risk |
| `gold_hourly_fraud_rate` | Hourly flagged/total ratio |
| `gold_hourly_volume` | Hourly transaction volume |
| `gold_merchant_category_summary` | Spend + risk by merchant category |
| `gold_customer_summary` | Per-customer spend profile |
| `gold_high_value_transactions` | Transactions above the value threshold |
| `gold_alert_log` | Alert dispatch log (SENT / PENDING / FAILED) |

## Setup

1. **Secrets** — all credentials live in the `finguard-scope` Databricks secret scope (never hardcode keys):
   - `kafka_connection_details` — JSON with bootstrap server, topic, Confluent API key/secret
   - `gmail_api_key` — Gmail app password for SMTP alerts
   - `postgres_password` — Supabase/Postgres credentials
2. **Run `02_Setup_Secrete_Scope`** to provision the scope, then deploy the pipelines in `pipelines/`.
3. **Schedule** the orchestration job (bronze ingest → silver → gold → alerts).

> 🔒 **Security note:** credentials referenced in notebooks are placeholders pointing at the secret scope. Any API keys stored directly in workspace notebooks should be rotated.

## Tech Stack

Apache Spark Structured Streaming · Spark Declarative Pipelines (medallion) · Delta Lake on Unity Catalog · Confluent Cloud Kafka · Databricks Workflows · Lakehouse Federation (Supabase PostgreSQL) · Gmail SMTP alerting · AI/BI dashboards

## Auto-Sync

Notebooks in this repository are automatically synced from the Databricks workspace (`/Users/dsobahle@gmail.com/Finguard_Project/`) to the Git folder (`/Workspace/Users/dsobahle@gmail.com/FinGuard_Git/`) and pushed to GitHub on a recurring schedule. The sync copies all 9 notebooks, commits any changes, and pushes to the `main` branch.