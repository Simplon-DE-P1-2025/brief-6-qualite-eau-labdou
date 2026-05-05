# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Ingestion Hub'Eau
# MAGIC Ingestion incrémentale depuis l'API Hub'Eau qualité eau potable via `dlt` (dlthub).
# MAGIC
# MAGIC - API : https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis
# MAGIC - Destination : `qualite_eau.bronze.resultats_dis`
# MAGIC - Mode : merge incrémental sur `date_prelevement`

# COMMAND ----------
%pip install "dlt[databricks,rest_api]>=1.4" --quiet
dbutils.library.restartPython()

# COMMAND ----------
import os
import dlt  # dlthub — pas le module Databricks DLT
from dlt.sources.rest_api import rest_api_source

os.environ["DATABRICKS_SERVER_HOSTNAME"] = dbutils.secrets.get("qualite-eau", "databricks_host")
os.environ["DATABRICKS_HTTP_PATH"]       = dbutils.secrets.get("qualite-eau", "databricks_http_path")
os.environ["DATABRICKS_ACCESS_TOKEN"]    = dbutils.secrets.get("qualite-eau", "databricks_token")

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"

# COMMAND ----------

source = rest_api_source({
    "client": {
        "base_url": BASE_URL,
    },
    "resources": [
        {
            "name": "resultats_dis",
            "endpoint": {
                "path": "resultats_dis",
                "params": {"size": 10000},
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "next",
                },
                "data_selector": "data",
                "incremental": {
                    "cursor_path": "date_prelevement",
                    "initial_value": "2020-01-01",
                    "param": "date_min_prelevement",
                },
            },
            "primary_key": ["code_prelevement", "code_parametre"],
            "write_disposition": "merge",
        }
    ],
})

# COMMAND ----------
pipeline = dlt.pipeline(
    pipeline_name="qualite_eau_hubeau",
    destination="databricks",
    dataset_name="qualite_eau.bronze",
)

load_info = pipeline.run(source)
print(load_info)
