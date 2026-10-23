# Databricks notebook source
# ==========================================
# 1 Define Connection Variables (EDIT THESE)
# ==========================================

# Secrete Scope Name (will be created if it doesn't exist)
secrete_scope = "finguard-scope"

# COMMAND ----------

# python cell in the same workspace notebook
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url=ctx.apiUrl().getOrElse(None)      # e.g. https://adb-...azuredatabricks.net
api_token=ctx.apiToken().getOrElse(None)  # personal access token for this session

print(api_url)
print(api_token)    # handle securely, do not log in real code 


import requests
import json

# --------------------------------
# Configuration
# --------------------------------
DATABRICKS_INSTANCE = api_url # replace with your workspace URL
DATABRICKS_TOKEN = api_token  # replace with your PAT

scope_name = secrete_scope  # Scope to be created
backened_type = "DATABRICKS"    # Use "AZURE_KEYVAULT" if intergrating with Key Vault

# COMMAND ----------



# -------------------------------
# API Endepoint
# -------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name
}

# -------------------------------
# Send request
# -------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

#--------------------------------
# Handle response
# -------------------------------
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print(f"Failed to create secret scope.")
    print(f"Status code:", response.status_code)
    print(f"Response:", response.text)



# COMMAND ----------

kafka_bootstrap_server = 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'
kafka_topic_name = 'credit_card_transactions'
kafka_api_key = 'NYD4XESCNIGSSWZ2'
kafka_api_secret = 'cfltO49NPHAwiVxOAdnu2wUkAeYb0Df71S/Yp+mTl1R+ZLAb0gHwpHq5wdn4H1kQ'

kafka_connection_details = json.dumps({
    "bootstrap_server": kafka_bootstrap_server,
    "topic_name": kafka_topic_name,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})


# COMMAND ----------

print(kafka_connection_details)

# COMMAND ----------

import requests
import json

scope = scope_name        # Already existing scope
secret_key = "kafka_connection_details"
secret_value = kafka_connection_details

# -------------------------
# API Endpoint
# -------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
  'Authorization': f"Bearer {DATABRICKS_TOKEN}",
  'Content-Type': 'application/json'
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

#---------------------------
# send request
# --------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

#---------------------------
# Output
# --------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully. in scope '{scope}',")
else:
    print(f"Failed to create secret.")
    print(f"Status code:", response.status_code)
    print(f"Response:", response.text)

# COMMAND ----------

# ==========================================
# Verify Secret Retrieval
# ==========================================

try:
    retrieve_json = dbutils.secrets.get(
        scope = secrete_scope, 
        key = 'kafka_connection_details'
        )

    print("secret retrieved successfully")
    
    parsed=json.loads(retrieve_json)
    print("Parsed json:")
    print(parsed)

except Exception as e:
    print("Secret verification failed:")
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Store Gmail API Key Secret
import requests
import json

scope = secrete_scope          # Already existing scope
secret_key = "gmail_api_key"
secret_value = "YOUR_GMAIL_API_KEY_HERE"  # TODO: Replace with your actual Gmail API key

# -------------------------
# API Endpoint
# -------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
  'Authorization': f"Bearer {DATABRICKS_TOKEN}",
  'Content-Type': 'application/json'
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

#---------------------------
# send request
# --------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

#---------------------------
# Output
# --------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print(f"Failed to create secret.")
    print(f"Status code:", response.status_code)
    print(f"Response:", response.text)






