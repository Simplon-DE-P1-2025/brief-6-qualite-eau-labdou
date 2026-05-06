# brief-6-qualite-eau-labdou

Pipeline de traitement et analyse de la qualité de l'eau avec Databricks, Delta Live Tables (DLT) et orchestration CI/CD.

## Objectif

Ingérer, transformer et analyser des données de qualité de l'eau en appliquant une architecture medallion (Bronze / Silver / Gold). L'ingestion depuis l'API HTTP est gérée par [`dlt`](https://dlthub.com) (pagination, chargement incrémental, schéma automatique) ; les transformations Bronze → Silver → Gold sont définies via Databricks Delta Live Tables, avec un pipeline CI/CD automatisé.

## Structure du projet

```
.
├── notebooks/
│   ├── 00_exploration_local.py   # Exploration DuckDB locale (VS Code, hors Databricks)
│   ├── 01_ingestion.py           # Ingestion Hub'Eau + INSEE COG via dlthub
│   ├── 02_bronze.py              # Couche Bronze DLT (métadonnées + partitionnement)
│   ├── 03_silver.py              # Couche Silver DLT (nettoyage, enrichissement, conformité)
│   ├── 04_gold.py                # Couche Gold DLT (4 tables agrégées)
│   └── 05_validation_ge.py       # Validation Great Expectations (Silver + Gold)
├── tests/
│   ├── conftest.py               # Fixtures DuckDB (inline synthétique + real Hub'Eau)
│   ├── test_silver_conformite.py # Tests logique Silver (conformité, catégories, seuils)
│   └── test_gold_agregations.py  # Tests agrégations Gold (déduplication, taux)
├── config/                       # Paramètres d'environnement et de pipeline
├── databricks.yml                # Databricks Asset Bundle (DAB)
└── pyproject.toml                # Dépendances et configuration du projet
```

## Stack technique

| Couche | Outil |
|--------|-------|
| Ingestion API | [`dlt`](https://dlthub.com) — pagination, incrémental, schéma automatique |
| Transformation | Databricks Delta Live Tables |
| Stockage | Delta Lake / Hive Metastore |
| Orchestration | Databricks Workflows |
| Tests | pytest + DuckDB (logique SQL locale) |

## Prérequis

- Python >= 3.11
- Accès à un workspace Databricks
- `uv` (gestionnaire de paquets recommandé)

## Installation

```bash
# Dépendances de production
uv sync

# Avec les outils de développement (tests, linting)
uv sync --extra dev

# Avec les outils d'exploration locale (DuckDB, Jupyter)
uv sync --extra explore
```

## Tests

Les tests sont organisés en deux niveaux :

- **inline** (rapide, sans réseau) : données synthétiques in-memory via DuckDB. Couvrent tous les cas de conformité C/N/D/S/NULL.
- **slow** (réseau requis) : ~100 lignes réelles tirées de l'API Hub'Eau (dept 33). Marqués `@pytest.mark.slow`.

```bash
# Tests rapides uniquement
uv run pytest tests/ -m "not slow"

# Tous les tests (requiert accès réseau)
uv run pytest tests/
```

## Architecture

### Sources ingérées (`01_ingestion.py`)

| Source | Ressource | Table Bronze | Mode |
|--------|-----------|--------------|------|
| Hub'Eau | `resultats_dis` | `bronze.resultats_dis` | merge incrémental |
| Hub'Eau | `communes_udi` | `bronze.communes_udi` | replace |
| INSEE COG 2025 | communes | `bronze.cog_communes` | replace |
| INSEE COG 2025 | départements | `bronze.cog_departements` | replace |
| INSEE COG 2025 | régions | `bronze.cog_regions` | replace |

### Bronze (`02_bronze.py`)

Lecture de `bronze.resultats_dis`, ajout de `_ingested_at`, `_source`, et `annee_prelevement`. Partitionnée par année.

### Silver (`03_silver.py`)

Granularité : **une ligne = un résultat d'analyse** (code_prelevement × libelle_parametre).

Transformations principales :
- Typage des dates et extraction numérique des seuils (`limite_qualite`, `reference_qualite`) depuis les champs texte
- Classification `categorie_parametre` : `microbiologie`, `radioactivite`, `organoleptique`, `chimie` (fallback)
- Calcul `depasse_limite_qualite` / `depasse_reference_qualite` (NULL si seuil absent)
- Conformité bactériologique et physico-chimique : `!= 'N'` (C/D/S/NULL ne sont pas pénalisés)
- Conformité globale : `NOT (bact = 'N' OR pc = 'N')`
- Jointures COG pour libellés communes, départements, régions

Les champs de conformité (`est_conforme_bact_limites`, `est_conforme_pc_limites`, `est_conforme_global`) sont au niveau du **prélèvement** et se répètent sur toutes ses lignes.

### Gold (`04_gold.py`)

4 tables agrégées. La déduplication est systématique : `countDistinct(when(condition, code_prelevement))` pour compter des prélèvements distincts, pas des lignes.

| Table | Granularité | Description |
|-------|-------------|-------------|
| `conformite_par_commune` | commune × année | Taux bact, physico-chimique, global |
| `evolution_temporelle_parametres` | paramètre × mois × année | Moyenne, min, max, taux de dépassement numérique |
| `qualite_par_departement` | département × année | Taux de conformité globale |
| `non_conformites_par_departement` | département × année | Comptage et taux de non-conformités |

> `evolution_temporelle_parametres` utilise `depasse_limite_qualite` (niveau paramètre) et non `est_conforme_global` (niveau prélèvement) pour rester cohérent avec le groupe par paramètre.

## Exploration locale

Avant de construire les couches Silver et Gold sur Databricks, une exploration des données brutes a été réalisée en local avec DuckDB (`notebooks/00_exploration_local.py`). L'objectif était de comprendre la structure réelle de l'API Hub'Eau et de prototyper les transformations.

**Sources ingérées :**
- Hub'Eau `resultats_dis` — prélèvements et résultats d'analyses (Gironde, dept. 33)
- Hub'Eau `communes_udi` — lien commune ↔ unité de distribution (réseau)
- INSEE COG 2025 — communes, départements et régions (libellés et codes)

**Points clés découverts :**
- Granularité : une ligne = un résultat d'analyse (paramètre × prélèvement). Les champs de conformité (`conformite_limites_bact_prelevement`, `conformite_limites_pc_prelevement`, etc.) sont au niveau du **prélèvement** et se répètent sur toutes les lignes du même prélèvement.
- Non-conformité : les valeurs possibles sont `C` (conforme), `N` (non conforme), `D` (dérogation), `S` (sans objet). On considère un prélèvement non conforme uniquement si au moins un champ = `N`.
- `code_commune` dans `resultats_dis` désigne la commune du **point de surveillance** (toujours une seule valeur). La relation UDI → plusieurs communes est dans `communes_udi`.
- `COUNT(DISTINCT code_prelevement)` est nécessaire dans les agrégats Gold pour ne pas compter plusieurs fois un même prélèvement (répété par paramètre analysé).

**Pour lancer l'exploration :**
```bash
uv sync --extra explore
# Ouvrir notebooks/00_exploration_local.py dans VS Code
# Sélectionner le kernel .venv et exécuter cellule par cellule (Shift+Enter)
```
