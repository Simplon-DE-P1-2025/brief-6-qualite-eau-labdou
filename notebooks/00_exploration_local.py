# %% [markdown]
# # 00 — Exploration locale — Qualité de l'eau potable
#
# Exploration du dataset Hub'Eau avec DuckDB.
# Objectif : comprendre les champs de conformité pour construire les couches Silver et Gold.
#
# **Lancement** : ouvrir dans VS Code, sélectionner le kernel `.venv`, `Shift+Enter` par cellule.

# %%
import io

import duckdb
import pandas as pd
import requests
from IPython.display import display

con = duckdb.connect()

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
DEPT = "33"  # Gironde — bon échantillon représentatif

# %% [markdown]
# ## 1. Ingestion depuis Hub'Eau

# %%
def fetch_hubeau(endpoint: str, params: dict, max_pages: int = 2) -> pd.DataFrame:
    records, url, page = [], f"{BASE_URL}/{endpoint}", 0
    while url and page < max_pages:
        r = requests.get(url, params=params if page == 0 else {}, timeout=60)
        r.raise_for_status()
        body = r.json()
        records.extend(body.get("data", []))
        url = body.get("next")
        page += 1
    return pd.DataFrame(records)


def fetch_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), dtype=str, encoding="utf-8")
    df.columns = df.columns.str.lower()
    return df


df_resultats    = fetch_hubeau("resultats_dis",  {"code_departement": DEPT, "size": 5000})
df_communes_udi = fetch_hubeau("communes_udi",   {"code_departement": DEPT, "size": 5000}, max_pages=1)
df_cog_communes = fetch_csv("https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_2025.csv")

con.register("resultats",    df_resultats)
con.register("communes_udi", df_communes_udi)
con.register("cog_communes", df_cog_communes)

print(f"resultats     : {len(df_resultats):>7,} lignes | {len(df_resultats.columns)} colonnes")
print(f"communes_udi  : {len(df_communes_udi):>7,} lignes | {len(df_communes_udi.columns)} colonnes")
print(f"cog_communes  : {len(df_cog_communes):>7,} lignes | {len(df_cog_communes.columns)} colonnes")

# %% [markdown]
# ## 2. Structure de resultats_dis — colonnes réelles

# %%
con.execute("DESCRIBE resultats").df()

# %%
con.execute("SELECT * FROM resultats LIMIT 5").df()

# %% [markdown]
# ## 3. Champs de conformité — valeurs distinctes

# %%
# 4 champs de conformité identifiés dans les données Hub'Eau :
# - bact = bactériologique | pc = physico-chimique
# - limites = limites sanitaires réglementaires | references = références qualité indicatives
CONFORMITE_COLS = [
    "conformite_limites_bact_prelevement",
    "conformite_limites_pc_prelevement",
    "conformite_references_bact_prelevement",
    "conformite_references_pc_prelevement",
]

# %%
for col in CONFORMITE_COLS:
    print(f"\n── {col} ──")
    display(con.execute(f"""
        SELECT "{col}" AS valeur,
               COUNT(*) AS nb,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM resultats
        GROUP BY 1 ORDER BY 2 DESC
    """).df())

# %% [markdown]
# ## 4. Taux de null sur les colonnes clés

# %%
cols_cles = CONFORMITE_COLS + [
    "code_commune", "nom_commune", "date_prelevement", "libelle_parametre"
]
cols_cles = [c for c in cols_cles if c in df_resultats.columns]

null_query = "\nUNION ALL\n".join(
    f"SELECT '{col}' AS colonne, ROUND(100.0 * COUNT(*) FILTER (WHERE \"{col}\" IS NULL) / COUNT(*), 1) AS pct_null FROM resultats"
    for col in cols_cles
)
con.execute(f"SELECT * FROM ({null_query}) ORDER BY pct_null DESC").df()

# %% [markdown]
# ## 5. Paramètres non conformes les plus fréquents

# %%
# Paramètres non conformes sur les limites sanitaires (bact + pc)
con.execute("""
    SELECT
        libelle_parametre,
        conformite_limites_bact_prelevement  AS bact,
        conformite_limites_pc_prelevement    AS pc,
        COUNT(*) AS nb
    FROM resultats
    WHERE conformite_limites_bact_prelevement = 'N'
       OR conformite_limites_pc_prelevement   = 'N'
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    LIMIT 20
""").df()

# %% [markdown]
# ## 6. Jointure resultats × communes_udi × COG

# %%
print("Colonnes resultats contenant 'commune':",
      [c for c in df_resultats.columns if "commune" in c.lower()])
print("Colonnes communes_udi :", list(df_communes_udi.columns))

# %%
con.execute("""
    SELECT
        COUNT(*)                                    AS total,
        COUNT(cog.com)                              AS avec_cog,
        ROUND(100.0 * COUNT(cog.com) / COUNT(*), 1) AS pct_match_cog
    FROM resultats r
    LEFT JOIN cog_communes cog ON r.code_commune = cog.com
""").df()

# %% [markdown]
# ## 7. Prototype Silver
#
# À ajuster selon les vrais noms de colonnes observés en sections 2 et 3.

# %%
con.execute("""
CREATE OR REPLACE VIEW silver AS
SELECT
    r.code_commune,
    r.nom_commune,
    cog.dep                                          AS code_departement,
    cog.reg                                          AS code_region,
    cog.libelle                                      AS libelle_commune,
    TRY_CAST(r.date_prelevement AS DATE)             AS date_prelevement,
    YEAR(TRY_CAST(r.date_prelevement AS DATE))       AS annee_prelevement,
    r.libelle_parametre,
    CASE
        WHEN LOWER(r.libelle_parametre) SIMILAR TO '.*(bacter|coli|entero|coliforme).*'
            THEN 'microbiologie'
        WHEN LOWER(r.libelle_parametre) SIMILAR TO '.*(tritium|dose total).*'
            THEN 'radioactivite'
        ELSE 'chimie'
    END                                              AS categorie_parametre,

    -- Conformité bactériologique
    r.conformite_limites_bact_prelevement            AS conformite_bact_limites,
    r.conformite_references_bact_prelevement         AS conformite_bact_references,
    r.conformite_limites_bact_prelevement = 'C'      AS est_conforme_bact_limites,
    r.conformite_references_bact_prelevement = 'C'   AS est_conforme_bact_references,

    -- Conformité physico-chimique
    r.conformite_limites_pc_prelevement              AS conformite_pc_limites,
    r.conformite_references_pc_prelevement           AS conformite_pc_references,
    r.conformite_limites_pc_prelevement = 'C'        AS est_conforme_pc_limites,
    r.conformite_references_pc_prelevement = 'C'     AS est_conforme_pc_references,

    -- Conformité globale : conforme si bact ET pc sont conformes aux limites
    (r.conformite_limites_bact_prelevement = 'C'
     AND r.conformite_limites_pc_prelevement = 'C')  AS est_conforme_global,

    -- Cause de non-conformité
    CASE
        WHEN r.conformite_limites_bact_prelevement != 'C'
          OR r.conformite_limites_pc_prelevement   != 'C'
        THEN r.libelle_parametre
    END                                              AS cause_non_conformite,

    u.code_reseau,
    u.nom_reseau
FROM resultats r
LEFT JOIN cog_communes cog ON r.code_commune = cog.com
LEFT JOIN communes_udi u   ON r.code_commune = u.code_commune
WHERE r.code_commune    IS NOT NULL
  AND r.date_prelevement IS NOT NULL
""")

con.execute("SELECT * FROM silver LIMIT 5").df()

# %% [markdown]
# ## 8. Prototypes Gold

# %%
# Gold 1 — Conformité par commune (bact + pc différenciés)
con.execute("""
SELECT
    code_commune, libelle_commune, code_departement, annee_prelevement,
    COUNT(*)                                                AS nb_analyses,
    ROUND(100.0 * AVG(est_conforme_bact_limites::INT), 2)  AS taux_bact_limites_pct,
    ROUND(100.0 * AVG(est_conforme_pc_limites::INT), 2)    AS taux_pc_limites_pct,
    ROUND(100.0 * AVG(est_conforme_global::INT), 2)        AS taux_global_pct
FROM silver
GROUP BY 1, 2, 3, 4
ORDER BY taux_global_pct ASC
LIMIT 10
""").df()

# %%
# Gold 5 — Causes de non-conformité aux limites sanitaires
con.execute("""
SELECT
    annee_prelevement,
    code_departement,
    cause_non_conformite,
    categorie_parametre,
    SUM((NOT est_conforme_bact_limites)::INT) AS nb_non_conformes_bact,
    SUM((NOT est_conforme_pc_limites)::INT)   AS nb_non_conformes_pc
FROM silver
WHERE NOT est_conforme_global
  AND cause_non_conformite IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY (nb_non_conformes_bact + nb_non_conformes_pc) DESC
LIMIT 15
""").df()

# %% [markdown]
# ## 9. Tests à implémenter
#
# | Test | Logique |
# |------|---------|
# | `test_est_conforme_bact` | `"C"` → `True` ; `"N"` → `False` ; `NULL` → `False` |
# | `test_est_conforme_pc` | idem pour physico-chimique |
# | `test_est_conforme_global` | `True` seulement si bact ET pc limites = `"C"` |
# | `test_cause_non_conformite` | `None` si conforme global, `libelle_parametre` sinon |
# | `test_categorie_parametre` | `"E. coli"` → `microbiologie`, `"Nitrates"` → `chimie` |
# | `test_null_code_commune_dropped` | Lignes sans `code_commune` absentes de Silver |
# | `test_taux_conformite_range` | tous les taux entre `0` et `100` |
# | `test_cog_join_rate` | > 95 % des communes ont un `code_departement` non nul |
