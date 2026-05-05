# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Ingestion
# MAGIC Téléchargement du dataset data.gouv.fr via `dlt` (dlthub) et écriture dans `qualite_eau.bronze`.

# COMMAND ----------
%pip install "dlt[databricks]>=1.4" --quiet
dbutils.library.restartPython()

# COMMAND ----------
import os
import io
import dlt  # dlthub — pas le module Databricks DLT
import requests
import pandas as pd

# URL de la ressource CSV sur data.gouv.fr
# À mettre à jour si l'identifiant de ressource change (onglet "Fichiers" du dataset)
SOURCE_URL = "https://www.data.gouv.fr/fr/datasets/r/e7834586-1f62-4d07-8b72-d77ff35c82fb"

os.environ["DATABRICKS_SERVER_HOSTNAME"] = dbutils.secrets.get("qualite-eau", "databricks_host")
os.environ["DATABRICKS_HTTP_PATH"]       = dbutils.secrets.get("qualite-eau", "databricks_http_path")
os.environ["DATABRICKS_ACCESS_TOKEN"]    = dbutils.secrets.get("qualite-eau", "databricks_token")

# COMMAND ----------

@dlt.resource(name="raw_controles_sanitaires", write_disposition="replace")
def controles_sanitaires():
    response = requests.get(SOURCE_URL, timeout=180)
    response.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(response.content),
        sep=";",
        encoding="utf-8",
        dtype=str,
        low_memory=False,
    )

    # Normalisation des noms de colonnes : minuscules, sans accents, espaces → _
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(r"[\s']+", "_", regex=True)
        .str.replace(r"[éèê]", "e", regex=True)
        .str.replace(r"[àâ]", "a", regex=True)
        .str.replace(r"[ôö]", "o", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    yield from df.to_dict(orient="records")


# COMMAND ----------
pipeline = dlt.pipeline(
    pipeline_name="qualite_eau_ingestion",
    destination="databricks",
    dataset_name="qualite_eau.bronze",
)

load_info = pipeline.run(controles_sanitaires())
print(load_info)
