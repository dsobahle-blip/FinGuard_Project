# gold_alert_log is managed by the 04_Alert_Pipeline notebook, NOT by this pipeline.
# It must be a regular Delta table (not a materialized view) because the notebook
# needs to UPDATE and APPEND to it. This file is intentionally left as a no-op.
# If the materialized view still exists in Unity Catalog, it should be dropped:
#   DROP MATERIALIZED VIEW IF EXISTS finguard.gold.gold_alert_log;
