# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC 4 tables agrégées pour les cas d'usage métier.
# MAGIC
# MAGIC **Déduplication** : les champs de conformité sont au niveau du prélèvement et se
# MAGIC répètent sur toutes les lignes du même prélèvement (une par paramètre analysé).
# MAGIC On utilise `countDistinct(when(condition, col("code_prelevement")))` pour compter
# MAGIC des prélèvements distincts, pas des lignes.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## Table 1 — Conformité par commune


@dlt.table(
    name="conformite_par_commune",
    comment="Taux de conformité annuel par commune (dédupliqué au niveau prélèvement)",
    table_properties={"quality": "gold"},
)
def gold_conformite_par_commune():
    return (
        dlt.read("controles_sanitaires")
        .groupBy(
            "code_commune", "libelle_commune",
            "code_departement", "libelle_departement",
            "annee_prelevement",
        )
        .agg(
            F.countDistinct("code_prelevement").alias("nb_prelevements"),
            F.countDistinct(
                F.when(F.col("est_conforme_bact_limites"), F.col("code_prelevement"))
            ).alias("nb_conformes_bact"),
            F.countDistinct(
                F.when(F.col("est_conforme_pc_limites"), F.col("code_prelevement"))
            ).alias("nb_conformes_pc"),
            F.countDistinct(
                F.when(F.col("est_conforme_global"), F.col("code_prelevement"))
            ).alias("nb_conformes_global"),
        )
        .withColumn("taux_bact_pct",
            F.round(F.col("nb_conformes_bact") / F.col("nb_prelevements") * 100, 2))
        .withColumn("taux_pc_pct",
            F.round(F.col("nb_conformes_pc") / F.col("nb_prelevements") * 100, 2))
        .withColumn("taux_global_pct",
            F.round(F.col("nb_conformes_global") / F.col("nb_prelevements") * 100, 2))
    )


# COMMAND ----------
# MAGIC %md ## Table 2 — Évolution temporelle des paramètres


@dlt.table(
    name="evolution_temporelle_parametres",
    comment="Évolution mensuelle des mesures par paramètre",
    table_properties={"quality": "gold"},
)
def gold_evolution_temporelle():
    return (
        dlt.read("controles_sanitaires")
        .withColumn("mois_prelevement", F.month("date_prelevement"))
        .groupBy(
            "annee_prelevement", "mois_prelevement",
            "libelle_parametre", "categorie_parametre",
        )
        .agg(
            F.countDistinct("code_prelevement").alias("nb_prelevements"),
            F.round(F.mean("resultat_numerique"), 4).alias("moyenne"),
            F.round(F.min("resultat_numerique"), 4).alias("minimum"),
            F.round(F.max("resultat_numerique"), 4).alias("maximum"),
            F.first("unite_mesure").alias("unite_mesure"),
            # Dépassement numérique par paramètre — uniquement quand la valeur limite est connue
            F.count(F.when(F.col("depasse_limite_qualite").isNotNull(), True))
                .alias("nb_mesures_avec_limite"),
            F.sum(F.col("depasse_limite_qualite").cast("int"))
                .alias("nb_depassements_limite"),
            F.round(
                F.sum(F.col("depasse_limite_qualite").cast("int"))
                / F.count(F.when(F.col("depasse_limite_qualite").isNotNull(), True))
                * 100,
                2,
            ).alias("taux_depassement_limite_pct"),
        )
    )


# COMMAND ----------
# MAGIC %md ## Table 3 — Qualité par département


@dlt.table(
    name="qualite_par_departement",
    comment="Indicateurs de conformité globale par département (niveau prélèvement)",
    table_properties={"quality": "gold"},
)
def gold_qualite_par_departement():
    return (
        dlt.read("controles_sanitaires")
        .groupBy(
            "code_departement", "libelle_departement",
            "code_region", "libelle_region",
            "annee_prelevement",
        )
        .agg(
            F.countDistinct("code_prelevement").alias("nb_prelevements"),
            F.countDistinct(
                F.when(F.col("est_conforme_global"), F.col("code_prelevement"))
            ).alias("nb_conformes"),
            F.round(
                F.countDistinct(
                    F.when(F.col("est_conforme_global"), F.col("code_prelevement"))
                ) / F.countDistinct("code_prelevement") * 100,
                2,
            ).alias("taux_conformite_pct"),
        )
    )


# COMMAND ----------
# MAGIC %md ## Table 4 — Non-conformités par département et année


@dlt.table(
    name="non_conformites_par_departement",
    comment="Prélèvements non conformes par département et année (niveau prélèvement global)",
    table_properties={"quality": "gold"},
)
def gold_non_conformites():
    return (
        dlt.read("controles_sanitaires")
        .groupBy(
            "annee_prelevement",
            "code_departement", "libelle_departement",
        )
        .agg(
            F.countDistinct("code_prelevement").alias("nb_prelevements"),
            F.countDistinct(
                F.when(~F.col("est_conforme_bact_limites"), F.col("code_prelevement"))
            ).alias("nb_non_conformes_bact"),
            F.countDistinct(
                F.when(~F.col("est_conforme_pc_limites"), F.col("code_prelevement"))
            ).alias("nb_non_conformes_pc"),
            F.countDistinct(
                F.when(~F.col("est_conforme_global"), F.col("code_prelevement"))
            ).alias("nb_non_conformes_global"),
            F.round(
                F.countDistinct(
                    F.when(~F.col("est_conforme_global"), F.col("code_prelevement"))
                ) / F.countDistinct("code_prelevement") * 100,
                2,
            ).alias("taux_non_conformite_pct"),
        )
        .orderBy("annee_prelevement", F.col("nb_non_conformes_global").desc())
    )
