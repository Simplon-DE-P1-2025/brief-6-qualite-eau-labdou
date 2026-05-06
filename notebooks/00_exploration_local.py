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


COG_BASE = "https://www.insee.fr/fr/statistiques/fichier/8377162"

df_resultats        = fetch_hubeau("resultats_dis", {"code_departement": DEPT, "size": 5000})
df_communes_udi     = fetch_hubeau("communes_udi",  {"code_departement": DEPT, "size": 5000}, max_pages=1)
df_cog_communes     = fetch_csv(f"{COG_BASE}/v_commune_2025.csv")
df_cog_departements = fetch_csv(f"{COG_BASE}/v_departement_2025.csv")
df_cog_regions      = fetch_csv(f"{COG_BASE}/v_region_2025.csv")

con.register("resultats",        df_resultats)
con.register("communes_udi",     df_communes_udi)
con.register("cog_communes",     df_cog_communes)
con.register("cog_departements", df_cog_departements)
con.register("cog_regions",      df_cog_regions)

print(f"resultats        : {len(df_resultats):>7,} lignes | {len(df_resultats.columns)} colonnes")
print(f"communes_udi     : {len(df_communes_udi):>7,} lignes | {len(df_communes_udi.columns)} colonnes")
print(f"cog_communes     : {len(df_cog_communes):>7,} lignes | {len(df_cog_communes.columns)} colonnes")
print(f"cog_departements : {len(df_cog_departements):>7,} lignes | {len(df_cog_departements.columns)} colonnes")
print(f"cog_regions      : {len(df_cog_regions):>7,} lignes | {len(df_cog_regions.columns)} colonnes")

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
# `code_commune` dans resultats_dis désigne la commune du point de surveillance (une seule).
# La relation UDI → communes multiples est dans `communes_udi`, pas dans ce champ.
# Granularité Silver : une ligne par résultat d'analyse (code_prelevement × libelle_parametre).

# %%
con.execute("""
CREATE OR REPLACE VIEW silver AS
WITH source AS (
    -- Extraction des valeurs numériques depuis les champs texte limite/reference
    SELECT
        r.*,
        TRY_CAST(
            regexp_extract(REPLACE(r.limite_qualite_parametre, ',', '.'), '\d+\.?\d*')
            AS DOUBLE
        ) AS _limite_num,
        TRY_CAST(
            regexp_extract(REPLACE(r.reference_qualite_parametre, ',', '.'), '\d+\.?\d*')
            AS DOUBLE
        ) AS _reference_num
    FROM resultats r
    WHERE r.code_commune    IS NOT NULL
      AND r.date_prelevement IS NOT NULL
)
SELECT
    s.code_prelevement,
    s.code_commune,
    cog.libelle                                      AS libelle_commune,
    cog.dep                                          AS code_departement,
    dep_ref.libelle                                  AS libelle_departement,
    cog.reg                                          AS code_region,
    reg_ref.libelle                                  AS libelle_region,
    TRY_CAST(s.date_prelevement AS DATE)             AS date_prelevement,
    YEAR(TRY_CAST(s.date_prelevement AS DATE))       AS annee_prelevement,
    s.libelle_parametre,
    CASE
        WHEN LOWER(s.libelle_parametre) SIMILAR TO
            '.*(bact|coli|entero|ent.rocoques|coliforme|streptoc|l.gionel|pseudo|campylo|crypto|giardia|virus|phage).*'
            THEN 'microbiologie'
        WHEN LOWER(s.libelle_parametre) SIMILAR TO
            '.*(activit.|tritium|dose total|radioactiv|alpha global|beta global|uranium).*'
            THEN 'radioactivite'
        WHEN LOWER(s.libelle_parametre) SIMILAR TO
            '.*(temp.rature|aspect|saveur|couleur|coloration|odeur|turbidit.|opacit.|duret.).*'
            THEN 'organoleptique'
        ELSE 'chimie'
    END                                              AS categorie_parametre,

    -- Mesure et seuils
    TRY_CAST(s.resultat_numerique AS DOUBLE)         AS resultat_numerique,
    s._limite_num                                    AS limite_qualite,
    s._reference_num                                 AS reference_qualite,
    TRY_CAST(s.resultat_numerique AS DOUBLE) > s._limite_num
                                                     AS depasse_limite_qualite,
    TRY_CAST(s.resultat_numerique AS DOUBLE) > s._reference_num
                                                     AS depasse_reference_qualite,

    -- Conformité bactériologique (C/D/S/NULL = pas non-conforme ; N = non-conforme)
    s.conformite_limites_bact_prelevement            AS conformite_bact_limites,
    s.conformite_references_bact_prelevement         AS conformite_bact_references,
    s.conformite_limites_bact_prelevement != 'N'     AS est_conforme_bact_limites,
    s.conformite_references_bact_prelevement != 'N'  AS est_conforme_bact_references,

    -- Conformité physico-chimique
    s.conformite_limites_pc_prelevement              AS conformite_pc_limites,
    s.conformite_references_pc_prelevement           AS conformite_pc_references,
    s.conformite_limites_pc_prelevement != 'N'       AS est_conforme_pc_limites,
    s.conformite_references_pc_prelevement != 'N'    AS est_conforme_pc_references,

    -- Global : non conforme uniquement si au moins un champ = 'N' (D/S/NULL non pénalisés)
    NOT (
        s.conformite_limites_bact_prelevement = 'N'
        OR s.conformite_limites_pc_prelevement = 'N'
    )                                                AS est_conforme_global,

    u.code_reseau,
    u.nom_reseau
FROM source s
LEFT JOIN cog_communes     cog     ON s.code_commune = cog.com
LEFT JOIN cog_departements dep_ref ON cog.dep         = dep_ref.dep
LEFT JOIN cog_regions      reg_ref ON cog.reg         = reg_ref.reg
LEFT JOIN communes_udi     u       ON s.code_commune = u.code_commune
""")

con.execute("SELECT * FROM silver LIMIT 5").df()

# %% [markdown]
# ## 8. Prototypes Gold

# %%
# Gold 1 — Conformité par commune (bact + pc différenciés)
con.execute("""
SELECT
    code_commune, libelle_commune, code_departement, annee_prelevement,
    COUNT(DISTINCT code_prelevement) AS nb_prelevements,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_bact_limites THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_bact_limites_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_pc_limites THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_pc_limites_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_global_pct
FROM silver
GROUP BY 1, 2, 3, 4
ORDER BY taux_global_pct ASC
LIMIT 10
""").df()

# %%
# Gold 5 — Non-conformités par département et année
# Les codes C/N/D/S sont au niveau du prélèvement global : on ne peut pas attribuer
# la non-conformité à un paramètre précis depuis ces données.
con.execute("""
SELECT
    annee_prelevement,
    code_departement,
    libelle_departement,
    COUNT(DISTINCT code_prelevement) AS nb_prelevements,
    COUNT(DISTINCT CASE WHEN NOT est_conforme_bact_limites THEN code_prelevement END)
        AS nb_non_conformes_bact,
    COUNT(DISTINCT CASE WHEN NOT est_conforme_pc_limites   THEN code_prelevement END)
        AS nb_non_conformes_pc,
    COUNT(DISTINCT CASE WHEN NOT est_conforme_global       THEN code_prelevement END)
        AS nb_non_conformes_global,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN NOT est_conforme_global THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_non_conformite_pct
FROM silver
GROUP BY 1, 2, 3
ORDER BY taux_non_conformite_pct DESC
LIMIT 15
""").df()

# %% [markdown]
# ## 9. Lancement de DuckDB UI
#
# Les DataFrames enregistrés via `con.register()` ne sont pas visibles par l'UI (connexion séparée).
# On les matérialise en vraies tables DuckDB avant de démarrer l'UI.

# %%
con.execute("CREATE OR REPLACE TABLE resultats        AS SELECT * FROM resultats")
con.execute("CREATE OR REPLACE TABLE communes_udi     AS SELECT * FROM communes_udi")
con.execute("CREATE OR REPLACE TABLE cog_communes     AS SELECT * FROM cog_communes")
con.execute("CREATE OR REPLACE TABLE cog_departements AS SELECT * FROM cog_departements")
con.execute("CREATE OR REPLACE TABLE cog_regions      AS SELECT * FROM cog_regions")

# %%
con.execute("INSTALL ui; LOAD ui;")
con.execute("CALL start_ui();")


# %% [markdown]
# ## 10. Tests à implémenter
#
# | Test | Logique |
# |------|---------|
# | `test_est_conforme_bact` | `"C"` → `True` ; `"N"` → `False` ; `NULL` → `False` |
# | `test_est_conforme_pc` | idem pour physico-chimique |
# | `test_est_conforme_global` | `True` seulement si bact ET pc limites = `"C"` |
# | `test_cause_non_conformite` | `None` si conforme global, `libelle_parametre` sinon |
# | `test_categorie_parametre` | `"E. coli"` → `microbiologie` |
# | | `"Nitrates"` → `nitrate` ; `"Température"` → `organoleptique` |
# | `test_null_code_commune_dropped` | Lignes sans `code_commune` absentes de Silver |
# | `test_taux_conformite_range` | tous les taux entre `0` et `100` |
# | `test_cog_join_rate` | > 95 % des communes ont un `code_departement` non nul |
