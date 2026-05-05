# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 05 — Validation Great Expectations
# MAGIC Validation de la qualité des données sur les tables Silver et Gold.

# COMMAND ----------
%pip install great-expectations --quiet
dbutils.library.restartPython()

# COMMAND ----------
import great_expectations as gx

context = gx.get_context()

# COMMAND ----------
# MAGIC %md ## Validation Silver

silver_df = spark.table("qualite_eau.silver.controles_sanitaires").toPandas()

datasource = context.sources.add_or_update_pandas("silver_source")
asset = datasource.add_dataframe_asset("silver_controles")
batch_request = asset.build_batch_request(dataframe=silver_df)

suite_silver = context.add_or_update_expectation_suite("silver_controles_sanitaires")
validator = context.get_validator(batch_request=batch_request, expectation_suite=suite_silver)

# Colonnes clés non nulles
for col in ["code_commune", "date_prelevement", "libelle_parametre", "conclusion_conformite"]:
    validator.expect_column_values_to_not_be_null(col)

# Valeurs attendues
validator.expect_column_values_to_be_in_set(
    "categorie_parametre", ["microbiologie", "chimie", "radioactivite"]
)
validator.expect_column_values_to_be_between("resultat_numerique", min_value=0, mostly=0.95)
validator.expect_column_values_to_match_regex("code_commune", r"^\d{5}$", mostly=0.90)
validator.expect_column_values_to_match_regex("code_departement", r"^\d{2,3}$")

results_silver = validator.validate()
validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------
# MAGIC %md ## Validation Gold — Conformité par commune

gold_df = spark.table("qualite_eau.gold.conformite_par_commune").toPandas()

asset_gold = datasource.add_dataframe_asset("gold_conformite_commune")
batch_request_gold = asset_gold.build_batch_request(dataframe=gold_df)

suite_gold = context.add_or_update_expectation_suite("gold_conformite_par_commune")
validator_gold = context.get_validator(batch_request=batch_request_gold, expectation_suite=suite_gold)

validator_gold.expect_column_values_to_not_be_null("taux_conformite_pct")
validator_gold.expect_column_values_to_be_between("taux_conformite_pct", min_value=0, max_value=100)
validator_gold.expect_column_values_to_be_between("nb_analyses", min_value=1)

results_gold = validator_gold.validate()
validator_gold.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------
# MAGIC %md ## Résumé

for name, results in [("Silver", results_silver), ("Gold conformite_commune", results_gold)]:
    status = "OK" if results.success else "ECHEC"
    stats = results.statistics
    print(f"[{status}] {name} — {stats['successful_expectations']}/{stats['evaluated_expectations']} expectations validées")

# Fail the notebook (et donc le job Databricks) si une validation échoue
assert results_silver.success, "Validation Silver échouée"
assert results_gold.success, "Validation Gold échouée"
