# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 — Bronze
# MAGIC Lecture des données brutes ingérées par dlthub et ajout de métadonnées.
# MAGIC Table cible : `qualite_eau.bronze.controles_sanitaires`

# COMMAND ----------
import dlt  # Databricks DLT — injecté par le runtime DLT
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(
    name="controles_sanitaires",
    comment="Données brutes contrôle sanitaire de l'eau — data.gouv.fr",
    partition_cols=["annee_prelevement"],
    table_properties={"quality": "bronze", "delta.autoOptimize.optimizeWrite": "true"},
)
def bronze_controles_sanitaires():
    return (
        spark.table("qualite_eau.bronze.resultats_dis")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("data.gouv.fr/controle-sanitaire-eau"))
        .withColumn(
            "annee_prelevement",
            F.year(F.to_date(F.col("date_prelevement"), "yyyy-MM-dd")),
        )
    )
