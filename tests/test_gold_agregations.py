"""
Tests des agrégations Gold : déduplication, calcul des taux, non-conformités.

Les tests inline utilisent la fixture `con` (données synthétiques, déterministes).
Les tests `slow` utilisent `con_real` (API Hub'Eau, 100 lignes réelles dept 33).
"""

import pytest

# ---------------------------------------------------------------------------
# Helper : exécute une requête Gold sur silver_inline et retourne un DataFrame
# ---------------------------------------------------------------------------

_GOLD_COMMUNE_SQL = """
SELECT
    code_prelevement,
    code_commune,
    annee_prelevement,
    COUNT(DISTINCT code_prelevement) AS nb_prelevements,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_bact_limites THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_bact_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_pc_limites THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_pc_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
        / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
    ) AS taux_global_pct
FROM {table}
GROUP BY 1, 2, 3
"""

_GOLD_NON_CONFORMITES_SQL = """
SELECT
    code_commune,
    annee_prelevement,
    COUNT(DISTINCT code_prelevement)                                    AS nb_prelevements,
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
FROM {table}
GROUP BY 1, 2
"""


# ---------------------------------------------------------------------------
# Déduplication — invariant clé du pipeline
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_nb_prelevements_deduplication(self, con):
        """
        P001 a 3 lignes dans silver_inline (3 paramètres analysés).
        nb_prelevements Gold doit être 1, pas 3.
        """
        row = con.execute("""
            SELECT COUNT(DISTINCT code_prelevement) AS nb
            FROM silver_inline
            WHERE code_prelevement = 'P001'
        """).fetchone()
        assert row[0] == 1

    def test_silver_plus_de_lignes_que_gold(self, con):
        """
        Le total de lignes Silver est supérieur au COUNT(DISTINCT code_prelevement)
        car chaque prélèvement a plusieurs paramètres analysés.
        """
        nb_lignes = con.execute("SELECT COUNT(*) FROM silver_inline").fetchone()[0]
        nb_prelevements = con.execute(
            "SELECT COUNT(DISTINCT code_prelevement) FROM silver_inline"
        ).fetchone()[0]
        assert nb_lignes > nb_prelevements


# ---------------------------------------------------------------------------
# Calcul des taux de conformité
# ---------------------------------------------------------------------------

class TestTauxConformite:
    def test_taux_100_si_tous_conformes(self, con):
        """
        Parmi les prélèvements avec bact=C et pc=C : P001, P005, P007
        → taux_global_pct = 100 sur ce sous-ensemble.
        """
        row = con.execute("""
            SELECT
                ROUND(
                    100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
                ) AS taux
            FROM silver_inline
            WHERE code_prelevement IN ('P001', 'P005', 'P007')
        """).fetchone()
        assert row[0] == 100.0

    def test_taux_0_si_aucun_conforme(self, con):
        """
        P004 : bact=N et pc=N → taux_global_pct = 0.
        """
        row = con.execute("""
            SELECT
                ROUND(
                    100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
                ) AS taux
            FROM silver_inline
            WHERE code_prelevement = 'P004'
        """).fetchone()
        assert row[0] == 0.0

    def test_taux_50_si_moitie_conforme(self, con):
        """
        P001 (conforme) + P002 (non conforme bact) → taux_global_pct = 50.
        """
        row = con.execute("""
            SELECT
                ROUND(
                    100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
                ) AS taux
            FROM silver_inline
            WHERE code_prelevement IN ('P001', 'P002')
        """).fetchone()
        assert row[0] == 50.0

    def test_taux_dans_plage_0_100(self, con):
        """Tous les taux Gold calculés sur données inline sont entre 0 et 100."""
        df = con.execute(
            _GOLD_COMMUNE_SQL.format(table="silver_inline")
        ).df()
        for col in ("taux_bact_pct", "taux_pc_pct", "taux_global_pct"):
            subset = df[col].dropna()
            assert (subset >= 0).all() and (subset <= 100).all(), f"{col} hors plage"


# ---------------------------------------------------------------------------
# Non-conformités
# ---------------------------------------------------------------------------

class TestNonConformites:
    def test_nb_non_conformes_bact(self, con):
        """P002 (bact=N) et P004 (bact=N) → 2 prélèvements non conformes bact."""
        row = con.execute("""
            SELECT COUNT(DISTINCT CASE WHEN NOT est_conforme_bact_limites THEN code_prelevement END)
            FROM silver_inline
        """).fetchone()
        assert row[0] == 2

    def test_nb_non_conformes_global(self, con):
        """P002, P003, P004, P008 ont au moins un champ N → 4 non conformes global."""
        row = con.execute("""
            SELECT COUNT(DISTINCT CASE WHEN NOT est_conforme_global THEN code_prelevement END)
            FROM silver_inline
        """).fetchone()
        assert row[0] == 4

    def test_taux_non_conformite_coherent(self, con):
        """taux_non_conformite = nb_non_conformes_global / nb_prelevements × 100."""
        df = con.execute(
            _GOLD_NON_CONFORMITES_SQL.format(table="silver_inline")
        ).df()
        for _, row in df.iterrows():
            if row["nb_prelevements"] > 0:
                expected = round(
                    100.0 * row["nb_non_conformes_global"] / row["nb_prelevements"], 2
                )
                assert abs(row["taux_non_conformite_pct"] - expected) < 0.01


# ---------------------------------------------------------------------------
# Tests sur données réelles (Hub'Eau dept 33)
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestGoldReel:
    def test_taux_dans_plage(self, con_real):
        df = con_real.execute("""
            SELECT
                ROUND(
                    100.0 * COUNT(DISTINCT CASE WHEN est_conforme_global THEN code_prelevement END)
                    / NULLIF(COUNT(DISTINCT code_prelevement), 0), 2
                ) AS taux_global_pct
            FROM silver
            GROUP BY code_commune, annee_prelevement
        """).df()
        subset = df["taux_global_pct"].dropna()
        assert (subset >= 0).all() and (subset <= 100).all()

    def test_nb_prelevements_positif(self, con_real):
        row = con_real.execute("""
            SELECT MIN(nb) FROM (
                SELECT COUNT(DISTINCT code_prelevement) AS nb
                FROM silver
                GROUP BY code_commune, annee_prelevement
            )
        """).fetchone()
        assert row[0] >= 1

    def test_deduplication_gold_inferieur_silver(self, con_real):
        """
        La somme des nb_prelevements Gold doit être inférieure au COUNT(*) Silver :
        chaque prélèvement génère plusieurs lignes Silver (une par paramètre).
        """
        nb_silver = con_real.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
        nb_gold = con_real.execute("""
            SELECT SUM(nb) FROM (
                SELECT COUNT(DISTINCT code_prelevement) AS nb
                FROM silver
                GROUP BY code_commune, annee_prelevement
            )
        """).fetchone()[0]
        assert nb_gold < nb_silver
