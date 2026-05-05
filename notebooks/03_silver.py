# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Silver
# MAGIC Nettoyage, typage, déduplication et enrichissement (catégorie paramètre).
# MAGIC Table cible : `qualite_eau.silver.controles_sanitaires`

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

# COMMAND ----------

MICROBIOLOGIE = [
    "bacteries aerobies", "escherichia coli", "e. coli", "enterocoque",
    "coliforme", "bacterie coliforme",
]
RADIOACTIVITE = ["tritium", "dose totale indicative", "radioactivite"]


def _categorie(col_name: str) -> F.Column:
    libelle = F.lower(F.col(col_name))
    return (
        F.when(
            F.reduce(lambda a, b: a | b, [libelle.contains(k) for k in RADIOACTIVITE]),
            "radioactivite",
        )
        .when(
            F.reduce(lambda a, b: a | b, [libelle.contains(k) for k in MICROBIOLOGIE]),
            "microbiologie",
        )
        .otherwise("chimie")
    )


# COMMAND ----------

@dlt.table(
    name="controles_sanitaires",
    comment="Données nettoyées et enrichies — partitionnées par année et département",
    partition_cols=["annee_prelevement", "code_departement"],
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("code_commune non nul", "code_commune IS NOT NULL")
@dlt.expect_or_drop("date valide", "date_prelevement IS NOT NULL")
@dlt.expect("conclusion connue", "conclusion_conformite IS NOT NULL")
def silver_controles_sanitaires():
    return (
        dlt.read("controles_sanitaires")  # lit depuis bronze via le pipeline DLT
        .dropDuplicates()
        .withColumn("date_prelevement", F.to_date(F.col("date_prelevement"), "yyyy-MM-dd"))
        .withColumn("annee_prelevement", F.year("date_prelevement"))
        .withColumn("resultat_numerique", F.col("resultat_numerique").cast(DoubleType()))
        .withColumn("limite_qualite", F.col("limite_qualite").cast(DoubleType()))
        .withColumn("reference_qualite", F.col("reference_qualite").cast(DoubleType()))
        .withColumn("coordonnee_x", F.col("coordonnee_x").cast(DoubleType()))
        .withColumn("coordonnee_y", F.col("coordonnee_y").cast(DoubleType()))
        .withColumn("code_departement", F.col("code_commune").substr(1, 2))
        .withColumn("categorie_parametre", _categorie("libelle_parametre"))
        .withColumn(
            "est_conforme",
            F.lower(F.col("conclusion_conformite")).startswith("conforme"),
        )
        .select(
            "code_commune",
            "nom_commune",
            "code_departement",
            "date_prelevement",
            "annee_prelevement",
            "libelle_parametre",
            "categorie_parametre",
            "resultat_numerique",
            "unite_mesure",
            "limite_qualite",
            "reference_qualite",
            "conclusion_conformite",
            "appreciation_conformite",
            "est_conforme",
            "coordonnee_x",
            "coordonnee_y",
            "_ingested_at",
            "_source",
        )
    )
