# brief-6-qualite-eau-labdou

Pipeline de traitement et analyse de la qualité de l'eau avec Databricks, Delta Live Tables (DLT) et orchestration CI/CD.

## Objectif

Ingérer, transformer et analyser des données de qualité de l'eau en appliquant une architecture medallion (Bronze / Silver / Gold). L'ingestion depuis l'API HTTP est gérée par [`dlt`](https://dlthub.com) (pagination, chargement incrémental, schéma automatique) ; les transformations Bronze → Silver → Gold sont définies via Databricks Delta Live Tables, avec un pipeline CI/CD automatisé.

## Structure du projet

```
.
├── notebooks/          # Notebooks d'exploration et pipelines DLT
├── config/             # Paramètres d'environnement et de pipeline
├── tests/              # Tests unitaires et d'intégration
├── pyproject.toml      # Dépendances et configuration du projet
└── .gitignore
```

## Stack technique

| Couche | Outil |
|--------|-------|
| Ingestion API | [`dlt`](https://dlthub.com) — pagination, incrémental, schéma automatique |
| Transformation | Databricks Delta Live Tables |
| Stockage | Delta Lake / Unity Catalog |
| Orchestration | Databricks Workflows |

## Prérequis

- Python >= 3.11
- Accès à un workspace Databricks
- `uv` (gestionnaire de paquets recommandé)

## Installation

```bash
uv sync
```

## Tests

```bash
uv run pytest tests/
```

## Architecture

| Couche | Description |
|--------|-------------|
| Bronze | Données brutes ingérées depuis la source |
| Silver | Données nettoyées et validées |
| Gold   | Agrégats et indicateurs métier |

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
