# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Silver
# MAGIC Nettoyage, typage, enrichissement COG et calcul des indicateurs de conformité.
# MAGIC Table cible : `qualite_eau.silver.controles_sanitaires`
# MAGIC
# MAGIC **Granularité** : une ligne = un résultat d'analyse (code_prelevement × libelle_parametre).
# MAGIC Les champs de conformité sont au niveau du prélèvement et se répètent sur toutes
# MAGIC les lignes du même prélèvement — utiliser COUNT(DISTINCT code_prelevement) dans Gold.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------


def _extract_num(col_name: str) -> F.Column:
    """Extraire la première valeur numérique d'un champ texte (ex: '50 mg/L' → 50.0)."""
    return (
        F.regexp_extract(
            F.regexp_replace(F.col(col_name), ",", "."),
            r"(\d+\.?\d*)",
            1,
        )
        .cast(DoubleType())
    )


def _categorie(col_name: str) -> F.Column:
    libelle = F.lower(F.col(col_name))
    return (
        F.when(
            libelle.rlike(
                r"bact|coli|entero|ent.rocoques|coliforme|streptoc|l.gionel"
                r"|pseudo|campylo|crypto|giardia|virus|phage"
            ),
            "microbiologie",
        )
        .when(
            libelle.rlike(
                r"activit.|tritium|dose total|radioactiv|alpha global|beta global|uranium"
            ),
            "radioactivite",
        )
        .when(
            libelle.rlike(
                r"temp.rature|aspect|saveur|couleur|coloration|odeur"
                r"|turbidit.|opacit.|duret."
            ),
            "organoleptique",
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
def silver_controles_sanitaires():
    # Référentiels COG — tables Bronze brutes ingérées par dlt (dlthub)
    cog_communes     = spark.table("qualite_eau.bronze.cog_communes")
    cog_departements = spark.table("qualite_eau.bronze.cog_departements")
    cog_regions      = spark.table("qualite_eau.bronze.cog_regions")

    enriched = (
        dlt.read("controles_sanitaires")
        .withColumn("date_prelevement",
            F.to_date(F.col("date_prelevement"), "yyyy-MM-dd"))
        .withColumn("annee_prelevement", F.year("date_prelevement"))
        .withColumn("resultat_numerique",
            F.col("resultat_numerique").cast(DoubleType()))
        .withColumn("limite_qualite",    _extract_num("limite_qualite_parametre"))
        .withColumn("reference_qualite", _extract_num("reference_qualite_parametre"))
        .withColumn("categorie_parametre", _categorie("libelle_parametre"))

        # Dépassement des seuils numériques (NULL si l'une des valeurs est absente)
        .withColumn("depasse_limite_qualite",
            F.when(
                F.col("resultat_numerique").isNotNull()
                & F.col("limite_qualite").isNotNull(),
                F.col("resultat_numerique") > F.col("limite_qualite"),
            )
        )
        .withColumn("depasse_reference_qualite",
            F.when(
                F.col("resultat_numerique").isNotNull()
                & F.col("reference_qualite").isNotNull(),
                F.col("resultat_numerique") > F.col("reference_qualite"),
            )
        )

        # Conformité bactériologique (C/D/S/NULL = pas N → non pénalisé)
        .withColumn("conformite_bact_limites",
            F.col("conformite_limites_bact_prelevement"))
        .withColumn("est_conforme_bact_limites",
            F.col("conformite_limites_bact_prelevement") != F.lit("N"))

        # Conformité physico-chimique
        .withColumn("conformite_pc_limites",
            F.col("conformite_limites_pc_prelevement"))
        .withColumn("est_conforme_pc_limites",
            F.col("conformite_limites_pc_prelevement") != F.lit("N"))

        # Global : non conforme si au moins un champ = 'N'
        .withColumn("est_conforme_global",
            ~(
                (F.col("conformite_limites_bact_prelevement") == F.lit("N"))
                | (F.col("conformite_limites_pc_prelevement") == F.lit("N"))
            )
        )
    )

    # Jointures COG pour les libellés géographiques
    return (
        enriched
        .join(
            cog_communes.select(
                F.col("com"),
                F.col("libelle").alias("libelle_commune"),
                F.col("dep"),
                F.col("reg"),
            ),
            enriched["code_commune"] == cog_communes["com"],
            "left",
        )
        .join(
            cog_departements.select(
                F.col("dep"),
                F.col("libelle").alias("libelle_departement"),
            ),
            "dep",
            "left",
        )
        .join(
            cog_regions.select(
                F.col("reg"),
                F.col("libelle").alias("libelle_region"),
            ),
            "reg",
            "left",
        )
        .select(
            "code_prelevement",
            "code_commune",
            "libelle_commune",
            F.col("dep").alias("code_departement"),
            "libelle_departement",
            F.col("reg").alias("code_region"),
            "libelle_region",
            "date_prelevement",
            "annee_prelevement",
            "libelle_parametre",
            "categorie_parametre",
            "resultat_numerique",
            "unite_mesure",
            "limite_qualite",
            "reference_qualite",
            "depasse_limite_qualite",
            "depasse_reference_qualite",
            "conformite_bact_limites",
            "est_conforme_bact_limites",
            "conformite_pc_limites",
            "est_conforme_pc_limites",
            "est_conforme_global",
            "_ingested_at",
            "_source",
        )
    )
