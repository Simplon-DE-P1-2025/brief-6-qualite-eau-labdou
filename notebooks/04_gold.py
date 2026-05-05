# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC 5 tables agrégées pour les cas d'usage métier.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## Table 1 — Conformité par commune

@dlt.table(
    name="conformite_par_commune",
    comment="Taux de conformité annuel par commune",
    table_properties={"quality": "gold"},
)
def gold_conformite_par_commune():
    return (
        dlt.read("controles_sanitaires")
        .groupBy("code_commune", "nom_commune", "code_departement", "annee_prelevement")
        .agg(
            F.count("*").alias("nb_analyses"),
            F.sum(F.col("est_conforme").cast("int")).alias("nb_conformes"),
            F.round(
                F.mean(F.col("est_conforme").cast("int")) * 100, 2
            ).alias("taux_conformite_pct"),
        )
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
        .groupBy("annee_prelevement", "mois_prelevement", "libelle_parametre", "categorie_parametre")
        .agg(
            F.count("*").alias("nb_analyses"),
            F.round(F.mean("resultat_numerique"), 4).alias("moyenne"),
            F.round(F.min("resultat_numerique"), 4).alias("minimum"),
            F.round(F.max("resultat_numerique"), 4).alias("maximum"),
            F.first("unite_mesure").alias("unite_mesure"),
            F.round(
                F.mean(F.col("est_conforme").cast("int")) * 100, 2
            ).alias("taux_conformite_pct"),
        )
    )


# COMMAND ----------
# MAGIC %md ## Table 3 — Carte de qualité par région

@dlt.table(
    name="qualite_par_departement",
    comment="Indicateurs de qualité agrégés par département pour cartographie",
    table_properties={"quality": "gold"},
)
def gold_qualite_par_departement():
    return (
        dlt.read("controles_sanitaires")
        .groupBy("code_departement", "annee_prelevement", "categorie_parametre")
        .agg(
            F.count("*").alias("nb_analyses"),
            F.round(
                F.mean(F.col("est_conforme").cast("int")) * 100, 2
            ).alias("taux_conformite_pct"),
            F.round(F.avg("coordonnee_x"), 4).alias("centroide_x"),
            F.round(F.avg("coordonnee_y"), 4).alias("centroide_y"),
        )
    )


# COMMAND ----------
# MAGIC %md ## Table 4 — Top 10 communes les plus / moins conformes

@dlt.table(
    name="top10_communes_conformite",
    comment="10 communes les meilleures et 10 les moins bonnes par taux de conformité (dernière année)",
    table_properties={"quality": "gold"},
)
def gold_top10_communes():
    from pyspark.sql.window import Window

    derniere_annee = (
        dlt.read("conformite_par_commune")
        .agg(F.max("annee_prelevement"))
        .collect()[0][0]
    )

    window_asc = Window.orderBy(F.col("taux_conformite_pct").asc())
    window_desc = Window.orderBy(F.col("taux_conformite_pct").desc())

    base = (
        dlt.read("conformite_par_commune")
        .filter(F.col("annee_prelevement") == derniere_annee)
        .filter(F.col("nb_analyses") >= 10)  # filtre les communes avec peu de mesures
    )

    top10_meilleures = base.withColumn("rang", F.rank().over(window_desc)).filter(F.col("rang") <= 10).withColumn("classement", F.lit("meilleures"))
    top10_pires = base.withColumn("rang", F.rank().over(window_asc)).filter(F.col("rang") <= 10).withColumn("classement", F.lit("moins_bonnes"))

    return top10_meilleures.union(top10_pires)


# COMMAND ----------
# MAGIC %md ## Table 5 — Analyse des non-conformités

@dlt.table(
    name="analyse_non_conformites",
    comment="Détail des non-conformités par paramètre, département et année",
    table_properties={"quality": "gold"},
)
def gold_non_conformites():
    return (
        dlt.read("controles_sanitaires")
        .filter(~F.col("est_conforme"))
        .groupBy(
            "annee_prelevement",
            "code_departement",
            "libelle_parametre",
            "categorie_parametre",
            "unite_mesure",
        )
        .agg(
            F.count("*").alias("nb_non_conformes"),
            F.round(F.avg("resultat_numerique"), 4).alias("moyenne_resultat"),
            F.round(F.avg("limite_qualite"), 4).alias("moyenne_limite_qualite"),
            F.round(
                (F.avg("resultat_numerique") / F.avg("limite_qualite") - 1) * 100, 2
            ).alias("depassement_moyen_pct"),
        )
        .orderBy("annee_prelevement", "nb_non_conformes", ascending=[False, False])
    )
