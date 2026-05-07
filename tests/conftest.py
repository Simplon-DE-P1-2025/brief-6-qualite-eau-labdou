"""
Fixtures DuckDB pour les tests du pipeline qualité de l'eau.

- `con`      : connexion in-memory avec données inline (déterministe, rapide)
- `con_real` : connexion avec ~100 lignes réelles Hub'Eau dept 33 (marquée slow)
"""

import io

import duckdb
import pandas as pd
import pytest
import requests

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
COG_BASE = "https://www.insee.fr/fr/statistiques/fichier/8377162"

# SQL de la vue Silver en DuckDB (réutilisé depuis 00_exploration_local.py section 7)
_SILVER_VIEW_SQL = r"""
CREATE OR REPLACE VIEW silver AS
WITH source AS (
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
    TRY_CAST(s.resultat_numerique AS DOUBLE)         AS resultat_numerique,
    s._limite_num                                    AS limite_qualite,
    s._reference_num                                 AS reference_qualite,
    TRY_CAST(s.resultat_numerique AS DOUBLE) > s._limite_num
                                                     AS depasse_limite_qualite,
    TRY_CAST(s.resultat_numerique AS DOUBLE) > s._reference_num
                                                     AS depasse_reference_qualite,
    s.conformite_limites_bact_prelevement            AS conformite_bact_limites,
    s.conformite_limites_bact_prelevement != 'N'     AS est_conforme_bact_limites,
    s.conformite_limites_pc_prelevement              AS conformite_pc_limites,
    s.conformite_limites_pc_prelevement != 'N'       AS est_conforme_pc_limites,
    NOT (
        s.conformite_limites_bact_prelevement = 'N'
        OR s.conformite_limites_pc_prelevement = 'N'
    )                                                AS est_conforme_global
FROM source s
LEFT JOIN cog_communes     cog     ON s.code_commune = cog.com
LEFT JOIN cog_departements dep_ref ON cog.dep        = dep_ref.dep
LEFT JOIN cog_regions      reg_ref ON cog.reg        = reg_ref.reg
"""


@pytest.fixture
def con():
    """
    Connexion DuckDB in-memory avec table `silver_inline` construite à partir
    de données synthétiques couvrant tous les cas de conformité (C, N, D, S, NULL).
    """
    conn = duckdb.connect()

    rows = [
        # (code_prelevement, code_commune, date, libelle_parametre,
        #  limite_qualite_parametre, reference_qualite_parametre,
        #  resultat_numerique, unite_mesure,
        #  conformite_limites_bact_prelevement, conformite_limites_pc_prelevement)
        ("P001", "33063", "2023-01-10", "E. coli",      "0 UFC/100mL", None,   "0",  "UFC/100mL", "C",   "C"),
        ("P001", "33063", "2023-01-10", "Nitrates",     "50 mg/L",     None,   "12", "mg/L",      "C",   "C"),
        ("P001", "33063", "2023-01-10", "Température",  None,          None,   "16", "°C",        "C",   "C"),
        ("P002", "33063", "2023-02-05", "E. coli",      "0 UFC/100mL", None,   "5",  "UFC/100mL", "N",   "C"),
        ("P003", "33063", "2023-03-12", "Nitrates",     "50 mg/L",     None,   "8",  "mg/L",      "C",   "N"),
        ("P004", "33063", "2023-04-20", "E. coli",      "0 UFC/100mL", None,   "3",  "UFC/100mL", "N",   "N"),
        ("P005", "33063", "2023-05-15", "E. coli",      "0 UFC/100mL", None,   "0",  "UFC/100mL", "D",   "C"),
        ("P006", "33063", "2023-06-01", "Coliformes",   "0 UFC/100mL", None,   "0",  "UFC/100mL", None,  "C"),
        ("P007", "33063", "2023-07-08", "Tritium",      "100 Bq/L",    None,   "5",  "Bq/L",      "C",   "C"),
        ("P008", "33063", "2023-08-22", "Arsenic",      "10 µg/L",     None,   "15", "µg/L",      "C",   "N"),
    ]

    df = pd.DataFrame(rows, columns=[
        "code_prelevement", "code_commune", "date_prelevement", "libelle_parametre",
        "limite_qualite_parametre", "reference_qualite_parametre",
        "resultat_numerique", "unite_mesure",
        "conformite_limites_bact_prelevement", "conformite_limites_pc_prelevement",
    ])

    conn.register("_raw", df)
    conn.execute(r"""
        CREATE TABLE silver_inline AS
        SELECT
            code_prelevement,
            code_commune,
            TRY_CAST(date_prelevement AS DATE) AS date_prelevement,
            YEAR(TRY_CAST(date_prelevement AS DATE)) AS annee_prelevement,
            libelle_parametre,
            CASE
                WHEN LOWER(libelle_parametre) SIMILAR TO
                    '.*(bact|coli|entero|ent.rocoques|coliforme|streptoc|l.gionel|pseudo|campylo|crypto|giardia|virus|phage).*'
                    THEN 'microbiologie'
                WHEN LOWER(libelle_parametre) SIMILAR TO
                    '.*(activit.|tritium|dose total|radioactiv|alpha global|beta global|uranium).*'
                    THEN 'radioactivite'
                WHEN LOWER(libelle_parametre) SIMILAR TO
                    '.*(temp.rature|aspect|saveur|couleur|coloration|odeur|turbidit.|opacit.|duret.).*'
                    THEN 'organoleptique'
                ELSE 'chimie'
            END AS categorie_parametre,
            TRY_CAST(resultat_numerique AS DOUBLE) AS resultat_numerique,
            TRY_CAST(
                regexp_extract(REPLACE(limite_qualite_parametre, ',', '.'), '\d+\.?\d*')
                AS DOUBLE
            ) AS limite_qualite,
            TRY_CAST(resultat_numerique AS DOUBLE) >
                TRY_CAST(
                    regexp_extract(REPLACE(limite_qualite_parametre, ',', '.'), '\d+\.?\d*')
                    AS DOUBLE
                ) AS depasse_limite_qualite,
            conformite_limites_bact_prelevement AS conformite_bact_limites,
            conformite_limites_bact_prelevement != 'N' AS est_conforme_bact_limites,
            conformite_limites_pc_prelevement   AS conformite_pc_limites,
            conformite_limites_pc_prelevement   != 'N' AS est_conforme_pc_limites,
            NOT (
                conformite_limites_bact_prelevement = 'N'
                OR conformite_limites_pc_prelevement = 'N'
            ) AS est_conforme_global
        FROM _raw
    """)
    yield conn
    conn.close()


def _fetch_hubeau(endpoint: str, params: dict, max_pages: int = 2) -> pd.DataFrame:
    records, url, page = [], f"{BASE_URL}/{endpoint}", 0
    while url and page < max_pages:
        r = requests.get(url, params=params if page == 0 else {}, timeout=60)
        r.raise_for_status()
        body = r.json()
        records.extend(body.get("data", []))
        url = body.get("next")
        page += 1
    return pd.DataFrame(records)


def _fetch_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), dtype=str, encoding="utf-8")
    df.columns = df.columns.str.lower()
    return df


@pytest.fixture(scope="session")
def con_real():
    """
    Connexion DuckDB avec ~100 lignes réelles tirées de l'API Hub'Eau (dept 33).
    Crée la vue `silver` avec la même logique que le notebook d'exploration.
    """
    df_resultats        = _fetch_hubeau("resultats_dis", {"code_departement": "33", "size": 100}, max_pages=1)
    df_communes_udi     = _fetch_hubeau("communes_udi",  {"code_departement": "33", "size": 1000}, max_pages=1)
    df_cog_communes     = _fetch_csv(f"{COG_BASE}/v_commune_2025.csv")
    df_cog_departements = _fetch_csv(f"{COG_BASE}/v_departement_2025.csv")
    df_cog_regions      = _fetch_csv(f"{COG_BASE}/v_region_2025.csv")

    conn = duckdb.connect()
    conn.register("resultats",        df_resultats)
    conn.register("communes_udi",     df_communes_udi)
    conn.register("cog_communes",     df_cog_communes)
    conn.register("cog_departements", df_cog_departements)
    conn.register("cog_regions",      df_cog_regions)
    conn.execute(_SILVER_VIEW_SQL)
    yield conn
    conn.close()
