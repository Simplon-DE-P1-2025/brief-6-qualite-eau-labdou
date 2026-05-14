"""
Tests de la logique Silver : conformité, catégorisation des paramètres, dépassement de seuil.

Toutes les assertions portent sur la table `silver_inline` de la fixture `con`
(données inline, déterministes, sans appel API).
"""

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(con, col, where):
    return con.execute(f"SELECT {col} FROM silver_inline WHERE {where}").fetchone()[0]


# ---------------------------------------------------------------------------
# Conformité bactériologique
# ---------------------------------------------------------------------------

class TestConformiteBact:
    def test_conforme_C(self, con):
        # P001 : bact = "C" → True
        val = _get(con, "est_conforme_bact_limites", "code_prelevement = 'P001' LIMIT 1")
        assert val is True

    def test_non_conforme_N(self, con):
        # P002 : bact = "N" → False
        val = _get(con, "est_conforme_bact_limites", "code_prelevement = 'P002'")
        assert val is False

    def test_derogation_D(self, con):
        # P005 : bact = "D" (dérogation) → True (pas une non-conformité)
        val = _get(con, "est_conforme_bact_limites", "code_prelevement = 'P005'")
        assert val is True

    def test_null_bact_non_penalise(self, con):
        # P006 : bact = NULL → NULL (ni conforme ni non-conforme, pas pénalisé)
        val = _get(con, "est_conforme_bact_limites", "code_prelevement = 'P006'")
        assert val is None


# ---------------------------------------------------------------------------
# Conformité physico-chimique
# ---------------------------------------------------------------------------

class TestConformitePc:
    def test_conforme_C(self, con):
        val = _get(con, "est_conforme_pc_limites", "code_prelevement = 'P001' LIMIT 1")
        assert val is True

    def test_non_conforme_N(self, con):
        # P003 : pc = "N"
        val = _get(con, "est_conforme_pc_limites", "code_prelevement = 'P003'")
        assert val is False


# ---------------------------------------------------------------------------
# Conformité globale
# ---------------------------------------------------------------------------

class TestConformiteGlobal:
    def test_global_vrai_si_bact_et_pc_conformes(self, con):
        # P001 : bact="C", pc="C" → True
        val = _get(con, "est_conforme_global", "code_prelevement = 'P001' LIMIT 1")
        assert val is True

    def test_global_faux_si_bact_N(self, con):
        # P002 : bact="N", pc="C" → False
        val = _get(con, "est_conforme_global", "code_prelevement = 'P002'")
        assert val is False

    def test_global_faux_si_pc_N(self, con):
        # P003 : bact="C", pc="N" → False
        val = _get(con, "est_conforme_global", "code_prelevement = 'P003'")
        assert val is False

    def test_global_faux_si_bact_et_pc_N(self, con):
        # P004 : bact="N", pc="N" → False
        val = _get(con, "est_conforme_global", "code_prelevement = 'P004'")
        assert val is False

    def test_global_null_si_bact_null_et_pc_conforme(self, con):
        # P006 : bact=NULL, pc="C" → NULL (aucun champ = "N", mais bact est NULL)
        # NOT (NULL = 'N' OR FALSE) = NOT (NULL OR FALSE) = NOT NULL = NULL
        val = _get(con, "est_conforme_global", "code_prelevement = 'P006'")
        assert val is None

    def test_derogation_compte_comme_conforme_global(self, con):
        # P005 : bact="D" → est_conforme_global = True (D ≠ N)
        val = _get(con, "est_conforme_global", "code_prelevement = 'P005'")
        assert val is True


# ---------------------------------------------------------------------------
# Catégorisation des paramètres
# ---------------------------------------------------------------------------

class TestCategorieParametre:
    def test_ecoli_microbiologie(self, con):
        val = _get(con, "categorie_parametre", "code_prelevement = 'P001' AND libelle_parametre = 'E. coli'")
        assert val == "microbiologie"

    def test_coliformes_microbiologie(self, con):
        val = _get(con, "categorie_parametre", "code_prelevement = 'P006'")
        assert val == "microbiologie"

    def test_nitrates_chimie(self, con):
        val = _get(con, "categorie_parametre", "code_prelevement = 'P001' AND libelle_parametre = 'Nitrates'")
        assert val == "chimie"

    def test_arsenic_chimie(self, con):
        # "Arsenic" n'appartient à aucune catégorie spéciale → chimie (défaut)
        val = _get(con, "categorie_parametre", "code_prelevement = 'P008'")
        assert val == "chimie"

    def test_temperature_organoleptique(self, con):
        val = _get(con, "categorie_parametre", "code_prelevement = 'P001' AND libelle_parametre = 'Température'")
        assert val == "organoleptique"

    def test_tritium_radioactivite(self, con):
        val = _get(con, "categorie_parametre", "code_prelevement = 'P007'")
        assert val == "radioactivite"


# ---------------------------------------------------------------------------
# Dépassement de seuil
# ---------------------------------------------------------------------------

class TestDepassementSeuil:
    def test_depasse_limite_vrai(self, con):
        # P008 : résultat=15 µg/L, limite=10 µg/L → True
        val = _get(con, "depasse_limite_qualite", "code_prelevement = 'P008'")
        assert val is True

    def test_depasse_limite_faux(self, con):
        # P007 : résultat=5 Bq/L, limite=100 Bq/L → False
        val = _get(con, "depasse_limite_qualite", "code_prelevement = 'P007'")
        assert val is False

    def test_depasse_null_si_limite_absente(self, con):
        # P001 Température : limite_qualite_parametre = NULL → depasse = NULL
        val = _get(con, "depasse_limite_qualite",
                   "code_prelevement = 'P001' AND libelle_parametre = 'Température'")
        assert val is None


# ---------------------------------------------------------------------------
# Tests sur données réelles (Hub'Eau dept 33)
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestSilverReel:
    def test_categories_valides(self, con_real):
        categories_valides = {"microbiologie", "radioactivite", "organoleptique", "chimie"}
        rows = con_real.execute(
            "SELECT DISTINCT categorie_parametre FROM silver WHERE categorie_parametre IS NOT NULL"
        ).fetchall()
        for (cat,) in rows:
            assert cat in categories_valides

    def test_cog_join_rate(self, con_real):
        # > 95 % des lignes Silver ont un code_departement non nul
        row = con_real.execute("""
            SELECT
                ROUND(100.0 * COUNT(*) FILTER (WHERE code_departement IS NOT NULL) / COUNT(*), 1)
            FROM silver
        """).fetchone()
        assert row[0] > 95.0

    def test_code_commune_non_nul(self, con_real):
        count = con_real.execute(
            "SELECT COUNT(*) FROM silver WHERE code_commune IS NULL"
        ).fetchone()[0]
        assert count == 0
