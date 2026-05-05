# %% [markdown]
# # 00 — Exploration locale — Qualité de l'eau potable
#
# Exploration du dataset Hub'Eau avec DuckDB pour préparer les transformations Silver et Gold.
#
# **Lancement** : `uv run --extra explore jupyter notebook` puis ouvrir ce fichier.

# %%
import io

import duckdb
import pandas as pd
import requests

con = duckdb.connect()  # base in-memory

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
DEPT = "33"  # Gironde — taille représentative, bon échantillon

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


df_resultats   = fetch_hubeau("resultats_dis",  {"code_departement": DEPT, "size": 5000})
df_communes_udi = fetch_hubeau("communes_udi",   {"code_departement": DEPT, "size": 5000}, max_pages=1)
df_cog_communes = fetch_csv("https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_2025.csv")
df_cog_depts    = fetch_csv("https://www.insee.fr/fr/statistiques/fichier/8377162/v_departement_2025.csv")

con.register("resultats",    df_resultats)
con.register("communes_udi", df_communes_udi)
con.register("cog_communes", df_cog_communes)
con.register("cog_depts",    df_cog_depts)

print(f"resultats     : {len(df_resultats):>7,} lignes | {len(df_resultats.columns)} colonnes")
print(f"communes_udi  : {len(df_communes_udi):>7,} lignes | {len(df_communes_udi.columns)} colonnes")
print(f"cog_communes  : {len(df_cog_communes):>7,} lignes | {len(df_cog_communes.columns)} colonnes")
print(f"cog_depts     : {len(df_cog_depts):>7,} lignes | {len(df_cog_depts.columns)} colonnes")

# %% [markdown]
# ## 2. Structure de resultats_dis

# %%
# Colonnes et types inférés par DuckDB
con.execute("DESCRIBE resultats").df()

# %%
# Échantillon de 5 lignes (toutes colonnes)
con.execute("SELECT * FROM resultats LIMIT 5").df()

# %% [markdown]
# ## 3. Qualité des données — taux de nulls

# %%
# Taux de null par colonne — identifie les colonnes inutilisables
null_query = "\nUNION ALL\n".join(
    f"SELECT '{col}' AS colonne, ROUND(100.0 * COUNT(*) FILTER (WHERE \"{col}\" IS NULL) / COUNT(*), 1) AS pct_null FROM resultats"
    for col in df_resultats.columns
)
con.execute(f"SELECT * FROM ({null_query}) ORDER BY pct_null DESC").df()

# %% [markdown]
# ## 4. Valeurs spéciales dans `valtraduite`

# %%
# Repérer les valeurs non numériques (chaînes, opérateurs, codes)
con.execute("""
    SELECT
        valtraduite,
        COUNT(*) AS nb,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM resultats
    WHERE TRY_CAST(valtraduite AS DOUBLE) IS NULL
      AND valtraduite IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 30
""").df()

# %%
# Valeurs avec préfixe < ou > (résultats censurés)
con.execute("""
    SELECT
        CASE
            WHEN valtraduite LIKE '<%'  THEN 'sous_seuil (<X)'
            WHEN valtraduite LIKE '>%'  THEN 'au_dessus (>X)'
            WHEN valtraduite = 'TRACES' THEN 'traces'
            WHEN valtraduite IN ('N.D.', 'ND', 'N.M.', 'NM') THEN 'non_detecte'
            WHEN valtraduite = 'PRESENCE' THEN 'presence'
            WHEN TRY_CAST(valtraduite AS DOUBLE) IS NOT NULL THEN 'numerique'
            ELSE 'autre'
        END AS type_valeur,
        COUNT(*) AS nb,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM resultats
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# %% [markdown]
# ## 5. Champs de conformité

# %%
# Valeurs distinctes des champs de conformité — base de `est_conforme`
for col in df_resultats.columns:
    if "conformit" in col.lower() or "conclusion" in col.lower():
        print(f"\n── {col} ──")
        display(con.execute(f"""
            SELECT "{col}", COUNT(*) AS nb
            FROM resultats
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """).df())

# %% [markdown]
# ## 6. Catégories de paramètres

# %%
# Paramètres les plus fréquents — base de la catégorisation Silver
con.execute("""
    SELECT
        libelle_parametre,
        categorie_parametre,
        COUNT(*) AS nb
    FROM resultats
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 30
""").df()

# %%
# Vérification : les mots-clés de catégorisation couvrent-ils bien les paramètres ?
con.execute("""
    SELECT libelle_parametre, COUNT(*) AS nb
    FROM resultats
    WHERE LOWER(libelle_parametre) NOT SIMILAR TO
        '.*(bacter|coli|entero|coliforme|nitrat|nitrit|pestici|alumi|cuivre|fer |plomb|fluor|chlor|ph|conduct|tritiun|dose total).*'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
""").df()

# %% [markdown]
# ## 7. Jointure resultats × communes_udi × COG

# %%
# Clé de jointure : quel champ de resultats correspond à inseecommune ?
con.execute("""
    SELECT *
    FROM resultats r
    LEFT JOIN communes_udi c USING (code_commune)
    LEFT JOIN cog_communes cog ON r.code_commune = cog.com
    LIMIT 5
""").df()

# %%
# Taux de correspondance COG
con.execute("""
    SELECT
        COUNT(*) AS total,
        COUNT(cog.com) AS avec_cog,
        ROUND(100.0 * COUNT(cog.com) / COUNT(*), 1) AS pct_match
    FROM resultats r
    LEFT JOIN cog_communes cog ON r.code_commune = cog.com
""").df()

# %% [markdown]
# ## 8. Prototype Silver

# %%
# Transformation Silver complète — à valider puis reporter dans 03_silver.py
con.execute("""
CREATE OR REPLACE VIEW silver AS
SELECT
    -- Localisation
    r.code_commune,
    r.nom_commune,
    c.dep  AS code_departement,
    c.reg  AS code_region,
    c.libelle AS libelle_commune,

    -- Temporel
    TRY_CAST(r.date_prelevement AS DATE)             AS date_prelevement,
    YEAR(TRY_CAST(r.date_prelevement AS DATE))       AS annee_prelevement,

    -- Paramètre
    r.libelle_parametre,
    CASE
        WHEN LOWER(r.libelle_parametre) SIMILAR TO '.*(bacter|coli|entero|coliforme).*' THEN 'microbiologie'
        WHEN LOWER(r.libelle_parametre) SIMILAR TO '.*(tritium|dose total).*'           THEN 'radioactivite'
        ELSE 'chimie'
    END AS categorie_parametre,

    -- Résultat numérique nettoyé
    CASE
        WHEN TRY_CAST(r.valtraduite AS DOUBLE) IS NOT NULL  THEN TRY_CAST(r.valtraduite AS DOUBLE)
        WHEN r.valtraduite LIKE '<%'                        THEN 0.0
        WHEN r.valtraduite LIKE '>%'                        THEN TRY_CAST(SUBSTR(r.valtraduite, 2) AS DOUBLE)
        WHEN r.valtraduite IN ('TRACES')                    THEN 0.0
        WHEN r.valtraduite = 'PRESENCE'                     THEN 1.0
        ELSE NULL
    END AS resultat_numerique,

    r.unite_mesure,
    TRY_CAST(r.limite_qualite AS DOUBLE) AS limite_qualite,

    -- Conformité
    r.conclusion_conformite_prelevement_parametre   AS conclusion_conformite,
    r.appreciation_conformite_prelevement           AS appreciation_conformite,
    r.conclusion_conformite_prelevement_parametre NOT IN ('N', 'NC', 'Non conforme')
                                                    AS est_conforme,

    -- Réseau
    u.code_reseau,
    u.nom_reseau

FROM resultats r
LEFT JOIN cog_communes c   ON r.code_commune = c.com
LEFT JOIN communes_udi u   ON r.code_commune = u.code_commune
WHERE r.code_commune IS NOT NULL
  AND r.date_prelevement IS NOT NULL
""")

con.execute("SELECT * FROM silver LIMIT 5").df()

# %% [markdown]
# ## 9. Prototype Gold

# %%
# Gold 1 — Conformité par commune
con.execute("""
SELECT
    code_commune,
    libelle_commune,
    code_departement,
    annee_prelevement,
    COUNT(*)                                    AS nb_analyses,
    SUM(est_conforme::INT)                      AS nb_conformes,
    ROUND(100.0 * AVG(est_conforme::INT), 2)    AS taux_conformite_pct
FROM silver
GROUP BY 1, 2, 3, 4
ORDER BY taux_conformite_pct ASC
LIMIT 10
""").df()

# %%
# Gold 2 — Évolution temporelle par paramètre
con.execute("""
SELECT
    annee_prelevement,
    libelle_parametre,
    categorie_parametre,
    COUNT(*)                                AS nb_analyses,
    ROUND(AVG(resultat_numerique), 4)       AS moyenne,
    ROUND(MIN(resultat_numerique), 4)       AS minimum,
    ROUND(MAX(resultat_numerique), 4)       AS maximum,
    ROUND(100.0 * AVG(est_conforme::INT), 2) AS taux_conformite_pct
FROM silver
WHERE resultat_numerique IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, nb_analyses DESC
LIMIT 20
""").df()

# %%
# Gold 5 — Top non-conformités
con.execute("""
SELECT
    annee_prelevement,
    code_departement,
    libelle_parametre,
    categorie_parametre,
    COUNT(*)                                AS nb_non_conformes,
    ROUND(AVG(resultat_numerique), 4)       AS moyenne_resultat,
    ROUND(AVG(limite_qualite), 4)           AS moyenne_limite
FROM silver
WHERE NOT est_conforme
GROUP BY 1, 2, 3, 4
ORDER BY nb_non_conformes DESC
LIMIT 15
""").df()

# %% [markdown]
# ## 10. Idées de tests
#
# À partir de cette exploration, les tests unitaires à implémenter dans `tests/` :
#
# | Test | Logique |
# |------|---------|
# | `test_valtraduite_cleaning` | `<0.05` → `0.0`, `TRACES` → `0.0`, `PRESENCE` → `1.0`, `>500` → `500.0`, `N.D.` → `NULL` |
# | `test_categorie_parametre` | `E. coli` → `microbiologie`, `Nitrates` → `chimie`, `Tritium` → `radioactivite` |
# | `test_est_conforme` | Valeurs `C`, `Conforme` → `True` ; `N`, `NC`, `Non conforme` → `False` |
# | `test_null_code_commune_dropped` | Lignes sans `code_commune` absentes de Silver |
# | `test_taux_conformite_range` | `taux_conformite_pct` entre `0` et `100` |
# | `test_cog_join_rate` | Plus de 95 % des communes ont un `code_departement` non nul |
