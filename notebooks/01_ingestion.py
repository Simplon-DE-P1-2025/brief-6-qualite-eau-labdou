# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Ingestion
# MAGIC Trois sources ingérées dans `qualite_eau.bronze` :
# MAGIC
# MAGIC | Source | Endpoint / Fichier | Table Bronze | Mode |
# MAGIC |--------|--------------------|--------------|------|
# MAGIC | Hub'Eau | `resultats_dis` | `resultats_dis` | merge incrémental |
# MAGIC | Hub'Eau | `communes_udi` | `communes_udi` | replace |
# MAGIC | INSEE COG 2025 | `v_commune_2025.csv` | `cog_communes` | replace |
# MAGIC | INSEE COG 2025 | `v_departement_2025.csv` | `cog_departements` | replace |
# MAGIC | INSEE COG 2025 | `v_region_2025.csv` | `cog_regions` | replace |

# COMMAND ----------
%pip install "dlt[databricks]>=1.4" "protobuf>=3.20.2,<5" --quiet
dbutils.library.restartPython()

# COMMAND ----------
import io
import os

import dlt  # dlthub — pas le module Databricks DLT
import pandas as pd
import requests

os.environ["DATABRICKS_SERVER_HOSTNAME"] = dbutils.secrets.get("qualite-eau", "databricks_host")
os.environ["DATABRICKS_ACCESS_TOKEN"]    = dbutils.secrets.get("qualite-eau", "databricks_token")

HUBEAU_BASE_URL     = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
COG_BASE            = "https://www.insee.fr/fr/statistiques/fichier/8377162"
COG_COMMUNE_URL     = f"{COG_BASE}/v_commune_2025.csv"
COG_DEPARTEMENT_URL = f"{COG_BASE}/v_departement_2025.csv"
COG_REGION_URL      = f"{COG_BASE}/v_region_2025.csv"

pipeline = dlt.pipeline(
    pipeline_name="qualite_eau_ingestion",
    destination="databricks",
    dataset_name="bronze",
)

# COMMAND ----------
# MAGIC %md ## 1 — Hub'Eau : resultats_dis + communes_udi

# COMMAND ----------

def _paginate(endpoint: str, params: dict):
    """Parcourt toutes les pages Hub'Eau et yielde les enregistrements."""
    url = f"{HUBEAU_BASE_URL}{endpoint}"
    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        body = response.json()
        yield from body.get("data", [])
        url = body.get("next")
        params = {}  # les pages suivantes utilisent l'URL complète de `next`


@dlt.resource(name="resultats_dis", write_disposition="merge",
              primary_key=["code_prelevement", "code_parametre"])
def resultats_dis():
    yield from _paginate("resultats_dis", {
        "size": 10000,
        "date_min_prelevement": "2020-01-01",
    })


@dlt.resource(name="communes_udi", write_disposition="replace",
              primary_key=["code_commune", "code_reseau"])
def communes_udi():
    yield from _paginate("communes_udi", {"size": 10000})


load_info_hubeau = pipeline.run([resultats_dis(), communes_udi()])
print(load_info_hubeau)

# COMMAND ----------
# MAGIC %md ## 2 — INSEE COG 2025 : communes, départements et régions

# COMMAND ----------

def _load_csv(url: str) -> list[dict]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    df = pd.read_csv(io.BytesIO(response.content), dtype=str, encoding="utf-8")
    df.columns = df.columns.str.lower()
    return df.to_dict(orient="records")


@dlt.resource(name="cog_communes", write_disposition="replace", primary_key="com")
def cog_communes():
    yield from _load_csv(COG_COMMUNE_URL)


@dlt.resource(name="cog_departements", write_disposition="replace", primary_key="dep")
def cog_departements():
    yield from _load_csv(COG_DEPARTEMENT_URL)


@dlt.resource(name="cog_regions", write_disposition="replace", primary_key="reg")
def cog_regions():
    yield from _load_csv(COG_REGION_URL)


load_info_cog = pipeline.run([cog_communes(), cog_departements(), cog_regions()])
print(load_info_cog)
