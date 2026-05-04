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
