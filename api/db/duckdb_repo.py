# api/db/duckdb_repo.py
import io

import duckdb
import pandas as pd
import requests

from api.config import settings
from api.db.base import AbstractRepo

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"
COG_BASE = "https://www.insee.fr/fr/statistiques/fichier/8377162"

# SQL Silver — identique à 00_exploration_local.py lignes 161-236
_SILVER_VIEW_SQL = r"""
CREATE OR REPLACE VIEW silver AS
WITH source AS (
    SELECT r.*,
        TRY_CAST(regexp_extract(REPLACE(r.limite_qualite_parametre, ',', '.'), '\d+\.?\d*') AS DOUBLE) AS _limite_num,
        TRY_CAST(regexp_extract(REPLACE(r.reference_qualite_parametre, ',', '.'), '\d+\.?\d*') AS DOUBLE) AS _reference_num
    FROM resultats r
    WHERE r.code_commune IS NOT NULL AND r.date_prelevement IS NOT NULL
)
SELECT
    s.code_prelevement, s.code_commune,
    cog.libelle AS libelle_commune, cog.dep AS code_departement,
    dep_ref.libelle AS libelle_departement, cog.reg AS code_region,
    reg_ref.libelle AS libelle_region,
    TRY_CAST(s.date_prelevement AS DATE) AS date_prelevement,
    YEAR(TRY_CAST(s.date_prelevement AS DATE)) AS annee_prelevement,
    s.libelle_parametre,
    CASE
        WHEN LOWER(s.libelle_parametre) SIMILAR TO '.*(bact|coli|entero|ent.rocoques|coliforme|streptoc|l.gionel|pseudo|campylo|crypto|giardia|virus|phage).*' THEN 'microbiologie'
        WHEN LOWER(s.libelle_parametre) SIMILAR TO '.*(activit.|tritium|dose total|radioactiv|alpha global|beta global|uranium).*' THEN 'radioactivite'
        WHEN LOWER(s.libelle_parametre) SIMILAR TO '.*(temp.rature|aspect|saveur|couleur|coloration|odeur|turbidit.|opacit.|duret.).*' THEN 'organoleptique'
        ELSE 'chimie'
    END AS categorie_parametre,
    TRY_CAST(s.resultat_numerique AS DOUBLE) AS resultat_numerique,
    s.unite_mesure, s._limite_num AS limite_qualite,
    TRY_CAST(s.resultat_numerique AS DOUBLE) > s._limite_num AS depasse_limite_qualite,
    s.conformite_limites_bact_prelevement AS conformite_bact_limites,
    s.conformite_limites_bact_prelevement != 'N' AS est_conforme_bact_limites,
    s.conformite_limites_pc_prelevement AS conformite_pc_limites,
    s.conformite_limites_pc_prelevement != 'N' AS est_conforme_pc_limites,
    NOT (s.conformite_limites_bact_prelevement = 'N' OR s.conformite_limites_pc_prelevement = 'N') AS est_conforme_global
FROM source s
LEFT JOIN cog_communes cog ON s.code_commune = cog.com
LEFT JOIN cog_departements dep_ref ON cog.dep = dep_ref.dep
LEFT JOIN cog_regions reg_ref ON cog.reg = reg_ref.reg
"""


class DuckDBRepo(AbstractRepo):
    def __init__(self):
        self._con = self._build_connection()

    def _build_connection(self) -> duckdb.DuckDBPyConnection:
        def fetch_hubeau(endpoint, params):
            r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=60)
            r.raise_for_status()
            return pd.DataFrame(r.json().get("data", []))

        def fetch_csv(url):
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            df = pd.read_csv(io.BytesIO(r.content), dtype=str, encoding="utf-8")
            df.columns = df.columns.str.lower()
            return df

        con = duckdb.connect()
        con.register("resultats", fetch_hubeau(
            "resultats_dis",
            {"code_departement": settings.hubeau_dept, "size": settings.hubeau_size}
        ))
        con.register("communes_udi", fetch_hubeau(
            "communes_udi",
            {"code_departement": settings.hubeau_dept, "size": 1000}
        ))
        con.register("cog_communes",     fetch_csv(f"{COG_BASE}/v_commune_2025.csv"))
        con.register("cog_departements", fetch_csv(f"{COG_BASE}/v_departement_2025.csv"))
        con.register("cog_regions",      fetch_csv(f"{COG_BASE}/v_region_2025.csv"))
        con.execute(_SILVER_VIEW_SQL)
        return con

    def _filters(self, conditions: list[str]) -> str:
        """Construit la clause WHERE à partir d'une liste de conditions non vides."""
        if not conditions:
            return ""
        return "WHERE " + " AND ".join(conditions)

    def get_conformite_communes(self, annee=None, code_departement=None):
        conds = []
        if annee:
            conds.append(f"annee_prelevement = {annee}")
        if code_departement:
            conds.append(f"code_departement = '{code_departement}'")

        sql = f"""
            SELECT
                code_commune, libelle_commune, code_departement, libelle_departement,
                annee_prelevement,
                COUNT(DISTINCT code_prelevement) AS nb_prelevements,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN est_conforme_bact_limites THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2) AS taux_bact_pct,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN est_conforme_pc_limites THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2) AS taux_pc_pct,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2) AS taux_global_pct
            FROM silver
            {self._filters(conds)}
            GROUP BY code_commune, libelle_commune, code_departement, libelle_departement, annee_prelevement
            ORDER BY taux_global_pct ASC
        """
        return self._con.execute(sql).df().to_dict(orient="records")

    def get_qualite_departements(self, annee=None, code_region=None):
        conds = []
        if annee:
            conds.append(f"annee_prelevement = {annee}")
        if code_region:
            conds.append(f"code_region = '{code_region}'")

        sql = f"""
            SELECT
                code_departement, libelle_departement, code_region, libelle_region,
                annee_prelevement,
                COUNT(DISTINCT code_prelevement) AS nb_prelevements,
                COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END) AS nb_conformes,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2) AS taux_conformite_pct
            FROM silver
            {self._filters(conds)}
            GROUP BY code_departement, libelle_departement, code_region, libelle_region, annee_prelevement
            ORDER BY taux_conformite_pct ASC
        """
        return self._con.execute(sql).df().to_dict(orient="records")

    def get_evolution_parametres(self, annee=None, categorie=None):
        conds = []
        if annee:
            conds.append(f"annee_prelevement = {annee}")
        if categorie:
            conds.append(f"categorie_parametre = '{categorie}'")

        sql = f"""
            SELECT
                annee_prelevement,
                MONTH(date_prelevement) AS mois_prelevement,
                libelle_parametre, categorie_parametre,
                COUNT(DISTINCT code_prelevement) AS nb_prelevements,
                ROUND(AVG(resultat_numerique), 4) AS moyenne,
                ROUND(MIN(resultat_numerique), 4) AS minimum,
                ROUND(MAX(resultat_numerique), 4) AS maximum,
                FIRST(unite_mesure) AS unite_mesure,
                ROUND(100.0 * SUM(depasse_limite_qualite::INT)
                    / NULLIF(COUNT(CASE WHEN depasse_limite_qualite IS NOT NULL THEN 1 END), 0), 2)
                    AS taux_depassement_limite_pct
            FROM silver
            {self._filters(conds)}
            GROUP BY annee_prelevement, mois_prelevement, libelle_parametre, categorie_parametre
            ORDER BY annee_prelevement, mois_prelevement
        """
        return self._con.execute(sql).df().to_dict(orient="records")
