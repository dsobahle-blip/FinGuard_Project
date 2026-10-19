# Databricks notebook source
bootstrap_servers = 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'
topic_name = 'credit_card_transactions'
api_key = 'REDACTED_PUBLIC_REPO'  # load from secret scope: dbutils.secrets.get('finguard-scope', 'kafka_connection_details')
api_secret = 'REDACTED_PUBLIC_REPO'  # load from secret scope: dbutils.secrets.get('finguard-scope', 'kafka_connection_details')

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# COMMAND ----------

sample_batch=(spark.read.format("kafka")
             .option("kafka.bootstrap.servers",bootstrap_servers) 
             .option("subscribe", topic_name)
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("kafka.sasl.jaas.config", jaas_config)
             .option("startingOffsets", "earliest")
             .load()
             )            

# COMMAND ----------

sample_batch.count()

# COMMAND ----------

display(sample_batch)

# COMMAND ----------

from pyspark.sql.functions import col 
parsed_batch=sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

display(parsed_batch)

# COMMAND ----------

parsed_batch.write.mode("overwrite").saveAsTable("finguard.bronze.transactions_batch_test")

# COMMAND ----------

streaming_df=(spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers",bootstrap_servers) 
             .option("subscribe", topic_name)
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("kafka.sasl.jaas.config", jaas_config)
             .option("startingOffsets", "earliest")
             .load()
             )            

# COMMAND ----------

from pyspark.sql.functions import col 
parsed_streaming_df=streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

streaming_query=(parsed_streaming_df.writeStream.format("delta")
.outputMode("append")
.option("checkpointLocation", "/Volumes/finguard/source/transactions/checkpoint_pt/")
.trigger(once=True)
.toTable("finguard.bronze.transactions_streaming_test")
)
print("Query Started with query id", streaming_query.id)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.transactions_streaming_test