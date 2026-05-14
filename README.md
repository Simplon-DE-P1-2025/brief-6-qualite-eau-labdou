# Qualité de l'Eau — Pipeline Databricks & API

![Python](https://img.shields.io/badge/python-3.11+-blue)
![CI](https://github.com/Simplon-DE-P1-2025/brief-6-qualite-eau-labdou/actions/workflows/ci.yml/badge.svg)

Pipeline complète d'ingestion, transformation et exposition des données de qualité de l'eau potable en France, construite sur Databricks Delta Live Tables et exposée via une API REST FastAPI.

---

## Contexte du projet

**Cadre pédagogique** : Brief n°6 — Formation DataEng Promo 1 2025, Simplon.

**Problématique** : construire, de bout en bout, une pipeline de données sur la qualité de l'eau potable en France. Les données sont publiques et issues de deux sources officielles :

- **[Hub'Eau](https://hubeau.eaufrance.fr)** — API gouvernementale exposant les résultats des contrôles sanitaires réalisés par les ARS (prélèvements, paramètres analysés, seuils de conformité)
- **[INSEE COG 2025](https://www.insee.fr/fr/statistiques/8377162)** — référentiel géographique officiel des communes, départements et régions

L'objectif couvre l'ensemble du cycle data : ingestion, stockage, transformation (architecture Medallion), qualité des données, exposition API et CI/CD.

---

## Architecture

```
Hub'Eau API ──┐
              ├──→ [01 — Ingestion dlt] ──→ Bronze ──→ Silver ──→ Gold ──→ FastAPI
INSEE COG ────┘              │                              │
                     Databricks Workflows          DLT (Delta Live Tables)
                     (Databricks Asset Bundle)
                              │
                     [05 — Great Expectations]
```

Le pipeline suit une **architecture Medallion** à trois couches :

| Couche | Rôle |
|--------|------|
| **Bronze** | Données brutes enrichies de métadonnées d'ingestion (`_ingested_at`, `_source`, `annee_prelevement`) |
| **Silver** | Données nettoyées, typées, enrichies des libellés COG, avec logique de conformité calculée |
| **Gold** | 4 tables agrégées prêtes à consommer (conformité par commune/département, évolution temporelle, non-conformités) |

---

## Chemin de pensée — comment on a construit le projet

### Phase 1 — Exploration locale (DuckDB)

Avant d'écrire la moindre ligne de code Databricks, toutes les transformations ont été prototypées **en local** dans `notebooks/00_exploration_local.py`, un notebook Python exécutable dans VS Code sans aucune infrastructure cloud.

L'objectif était de comprendre la structure réelle des données Hub'Eau : granularité des lignes, signification des codes de conformité (`C`, `N`, `D`, `S`), présence de valeurs manquantes, formats de seuils numériques stockés en texte (`"50 µg/L"`). Les jointures COG ont également été validées à ce stade (taux de correspondance communes > 95 %).

### Phase 2 — Tests unitaires

Une fois la logique de conformité et d'agrégation fixée, des **tests pytest ont été écrits avant la migration Databricks**. Ils utilisent DuckDB en mémoire et des données synthétiques couvrant tous les cas limites : conformité `C/N/D/S/NULL`, déduplication `COUNT(DISTINCT code_prelevement)`, calcul des taux. Cette approche garantit que la logique métier est correcte indépendamment de l'infrastructure.

### Phase 3 — Migration Databricks (DLT + DAB)

Les requêtes DuckDB ont été transposées en **Delta Live Tables Python** (notebooks `02_bronze.py` à `04_gold.py`). Le pipeline est déclaré via un **Databricks Asset Bundle** (`databricks.yml`) qui versionne en git l'ensemble des ressources : notebooks, cluster, job, planification. Deux targets sont configurées : `dev` (cluster single-node, pipeline en mode développement) et `prod` (multi-nœuds, exécution quotidienne à 2h00).

### Phase 4 — API FastAPI

Les tables Gold sont exposées via une **API REST FastAPI** avec un pattern repository abstrait (`AbstractRepo`) : le backend bascule entre DuckDB (test local, sans accès cloud) et Databricks SQL (production) via la variable d'environnement `DATA_SOURCE`. Cela permet de développer et tester l'API entièrement localement.

### Phase 5 — CI/CD

Un workflow GitHub Actions valide le code à chaque PR (`ci.yml` : ruff + pytest). Le release est automatisé par `python-semantic-release` à partir des commits conventionnels, et le déploiement Databricks se déclenche automatiquement à chaque publication de release (`cd.yml`).

---

## Stack technique

| Composant | Outil | Pourquoi |
|-----------|-------|----------|
| Ingestion API | [dlt](https://dlthub.com) | Pagination automatique, schéma inféré, chargement incrémental |
| Transformation | Databricks Delta Live Tables | Pipeline déclarative, dépendances gérées, monitoring natif |
| Stockage | Delta Lake / ADLS Gen2 | Transactions ACID, time travel, partitionnement |
| Orchestration | Databricks Workflows (DAB) | Infrastructure as code, versionnable en git |
| Tests | pytest + DuckDB | SQL testable localement, rapide, zéro infrastructure |
| API | FastAPI + Pydantic | Typage fort, documentation OpenAPI auto-générée |
| Lint | Ruff | Rapide, remplace flake8 + isort en un seul outil |
| CI/CD | GitHub Actions + semantic-release | Versioning conventionnel, déploiement automatisé |

---

## Structure du projet

```
.
├── notebooks/
│   ├── 00_exploration_local.py    # Prototypage DuckDB local (VS Code, hors Databricks)
│   ├── 01_ingestion.py            # Ingestion Hub'Eau + INSEE COG via dlt (dlthub)
│   ├── 02_bronze.py               # Couche Bronze DLT (métadonnées + partitionnement)
│   ├── 03_silver.py               # Couche Silver DLT (nettoyage, conformité, enrichissement COG)
│   ├── 04_gold.py                 # Couche Gold DLT (4 tables agrégées)
│   └── 05_validation_ge.py        # Validation Great Expectations (Silver + Gold)
├── api/
│   ├── main.py                    # Application FastAPI (lifespan, routers)
│   ├── config.py                  # Paramètres (DATA_SOURCE, credentials Databricks)
│   ├── dependencies.py            # Injection de dépendances (AbstractRepo singleton)
│   ├── db/
│   │   ├── base.py                # Interface AbstractRepo
│   │   ├── duckdb_repo.py         # Backend DuckDB (local, sans cloud)
│   │   └── databricks_repo.py     # Backend Databricks SQL (production)
│   ├── models/
│   │   ├── conformite.py          # Schéma ConformiteParCommune
│   │   ├── qualite.py             # Schéma QualiteParDepartement
│   │   └── evolution.py           # Schéma EvolutionTemporelleParametres
│   └── routes/
│       ├── conformite.py          # GET /conformite/communes
│       ├── qualite.py             # GET /qualite/departements
│       └── evolution.py           # GET /evolution/parametres
├── tests/
│   ├── conftest.py                # Fixtures DuckDB (inline synthétique + real Hub'Eau)
│   ├── test_silver_conformite.py  # Tests logique Silver (conformité, catégories, seuils)
│   └── test_gold_agregations.py   # Tests agrégations Gold (déduplication, taux)
├── config/
│   └── pipeline.yml               # Paramètres du pipeline (schémas, chemins ADLS)
├── .github/
│   └── workflows/
│       ├── ci.yml                 # CI : lint + tests sur PR
│       ├── release.yml            # Release automatique (semantic-release)
│       └── cd.yml                 # Deploy Databricks sur release publiée
├── databricks.yml                 # Databricks Asset Bundle (DAB)
└── pyproject.toml                 # Dépendances et configuration (uv, ruff, pytest)
```

---

## Pipeline de données

### Sources ingérées (`01_ingestion.py`)

| Source | Ressource | Table Bronze | Mode |
|--------|-----------|--------------|------|
| Hub'Eau | `resultats_dis` | `bronze.resultats_dis` | merge incrémental |
| Hub'Eau | `communes_udi` | `bronze.communes_udi` | replace |
| INSEE COG 2025 | communes | `bronze.cog_communes` | replace |
| INSEE COG 2025 | départements | `bronze.cog_departements` | replace |
| INSEE COG 2025 | régions | `bronze.cog_regions` | replace |

### Bronze (`02_bronze.py`)

Lecture de `bronze.resultats_dis`, ajout de `_ingested_at`, `_source` et `annee_prelevement` (extrait de `date_prelevement`). Partitionnée par année.

### Silver (`03_silver.py`)

Granularité : **une ligne = un résultat d'analyse** (code_prelevement × libelle_parametre).

Transformations principales :
- Typage des dates et extraction numérique des seuils depuis les champs texte (`"50 µg/L"` → `50.0`)
- Classification `categorie_parametre` : `microbiologie`, `radioactivite`, `organoleptique`, `chimie` (fallback)
- Calcul `depasse_limite_qualite` et `depasse_reference_qualite` (NULL si seuil absent)
- Conformité bactériologique et physico-chimique : `!= 'N'` — les codes `C` (conforme), `D` (dérogation), `S` (sans objet) et `NULL` ne sont pas pénalisés
- Conformité globale : `NOT (bact = 'N' OR pc = 'N')`
- Jointures COG pour les libellés communes, départements, régions

> Les champs de conformité sont définis au niveau du **prélèvement** et se répètent sur toutes les lignes du même prélèvement. Les agrégations Gold doivent donc utiliser `COUNT(DISTINCT code_prelevement)`.

### Gold (`04_gold.py`)

4 tables agrégées. La déduplication est systématique : `COUNT(DISTINCT CASE WHEN ... THEN code_prelevement END)`.

| Table | Granularité | Description |
|-------|-------------|-------------|
| `conformite_par_commune` | commune × année | Taux bactériologique, physico-chimique et global |
| `qualite_par_departement` | département × année | Taux de conformité globale avec lien région |
| `evolution_temporelle_parametres` | paramètre × mois × année | Moyenne, min, max, taux de dépassement numérique |
| `non_conformites_par_departement` | département × année | Comptage et taux de non-conformités |

> `evolution_temporelle_parametres` utilise `depasse_limite_qualite` (niveau paramètre) et non `est_conforme_global` (niveau prélèvement) pour rester cohérent avec le groupement par paramètre.

---

## API FastAPI

L'API expose les tables Gold avec un **pattern repository abstrait** : le backend (DuckDB ou Databricks SQL) est sélectionné à l'exécution via la variable `DATA_SOURCE`. Cela permet de faire tourner l'API localement sans accès cloud.

### Lancer l'API en local (mode DuckDB)

```bash
uv sync --extra api
DATA_SOURCE=duckdb uvicorn api.main:app --reload
```

L'API se connecte automatiquement à Hub'Eau et INSEE COG au démarrage pour construire les vues Silver en mémoire (environ 30 secondes pour ~5 000 lignes).

La documentation interactive est disponible sur `http://localhost:8000/docs`.

### Endpoints

| Méthode | Route | Filtres optionnels | Description |
|---------|-------|--------------------|-------------|
| GET | `/conformite/communes` | `annee`, `code_departement` | Conformité par commune |
| GET | `/qualite/departements` | `annee`, `code_region` | Qualité par département |
| GET | `/evolution/parametres` | `annee`, `categorie` | Évolution temporelle des paramètres |
| GET | `/health` | — | Statut et source de données active |

**Exemples :**

```bash
# Conformité pour le département 33 (Gironde) en 2023
curl "http://localhost:8000/conformite/communes?annee=2023&code_departement=33"

# Qualité par département pour une région
curl "http://localhost:8000/qualite/departements?code_region=75"

# Évolution des paramètres microbiologiques
curl "http://localhost:8000/evolution/parametres?categorie=microbiologie"

# Santé de l'API
curl "http://localhost:8000/health"
# → {"status": "ok", "source": "duckdb"}
```

---

## Tests

Les tests sont organisés en deux niveaux :

- **inline** (rapide, sans réseau) : données synthétiques in-memory via DuckDB. Couvrent tous les cas de conformité C/N/D/S/NULL et les invariants de déduplication Gold.
- **slow** (réseau requis) : ~100 lignes réelles tirées de l'API Hub'Eau (dept 33). Valident le taux de jointure COG (> 95 %) et la cohérence des agrégations sur données réelles.

```bash
# Dépendances de dev
uv sync --extra dev

# Tests rapides uniquement (zéro réseau)
uv run pytest tests/ -m "not slow" -v

# Tous les tests
uv run pytest tests/

# Lint
uv run ruff check .
```

---

## CI/CD

```
push dev / PR → main
       │
       ▼
  [ci.yml] ruff + pytest (tests rapides)
       │
       ▼ (si merge dans main)
  [release.yml] test gate + semantic-release → GitHub Release
       │
       ▼ (sur release publiée)
  [cd.yml] databricks bundle deploy --target prod
```

- **`ci.yml`** : déclenché sur push vers `dev` et sur toute PR vers `main` ou `dev`. Bloque le merge si le lint ou les tests échouent.
- **`release.yml`** : déclenché sur push vers `main`. Exécute les tests en prérequis (`needs: test`), puis `python-semantic-release` crée automatiquement le tag et le CHANGELOG à partir des commits conventionnels.
- **`cd.yml`** : déclenché sur chaque release GitHub publiée. Déploie le bundle Databricks en production.

Le déploiement requiert deux secrets GitHub : `DATABRICKS_HOST` et `DATABRICKS_TOKEN`.

---

## Difficultés rencontrées

### Disponibilité des VMs Azure

La souscription Simplon présente un quota limité sur les familles de VMs les plus courantes. Plusieurs types de machines se sont révélés indisponibles en France Central, puis en West Europe, nécessitant une analyse méthodique du stock disponible via Azure CLI avant d'identifier une combinaison région + type de VM fonctionnelle (`Standard_D4ds_v4`, West Europe).

### Compatibilité de `dlt` dans le runtime Databricks

L'installation de `dlt` via `%pip install` dans un notebook Databricks génère des conflits avec des dépendances pré-installées dans le runtime du cluster (notamment `protobuf`). La résolution a nécessité d'identifier les incompatibilités, de contraindre les versions transitives, et de réécrire la logique d'ingestion pour éviter les sous-modules du SDK `dlt` non accessibles dans ce contexte (en particulier `dlt.sources`).

### Logique de conformité et déduplication Gold

Les champs de conformité dans Hub'Eau sont définis au niveau du **prélèvement**, pas du paramètre analysé. Un prélèvement génère plusieurs lignes dans Silver (une par paramètre), toutes portant la même valeur de conformité. Il a fallu identifier ce problème tôt (Phase 1, exploration locale) pour concevoir les agrégations Gold avec `COUNT(DISTINCT code_prelevement)` et distinguer rigoureusement les métriques relevant du niveau prélèvement (conformité globale) de celles relevant du niveau paramètre (dépassement de seuil numérique).

### Séquençage ingestion → pipeline DLT

Le pipeline DLT (Bronze → Silver → Gold) dépend de l'ingestion préalable des tables brutes par `dlt`. Dans les premiers essais, lancer le job DLT avant la fin de l'ingestion produisait des tables Silver vides. La solution a été de définir explicitement les dépendances de tâches dans le Databricks Asset Bundle (`depends_on`), forçant l'ordonnancement correct : ingestion → transformation DLT → validation Great Expectations.

---

## À ajouter / améliorer

1. **Tests d'intégration API** : la couche FastAPI n'a actuellement aucun test. Ajouter des tests avec `TestClient` (FastAPI) couvrant les 3 routes, les filtres et les cas d'erreur.

2. **Gate de test dans le workflow de déploiement** : `cd.yml` déploie directement sur publication d'une release, sans re-valider les tests. Ajouter un job de test en prérequis renforcerait la sécurité.

3. **Endpoint `non_conformites_par_departement` non exposé** : cette table Gold existe et contient des informations pertinentes (classement des départements par taux de non-conformité), mais n'a pas de route API.

4. **Scope géographique limité du backend DuckDB** : en mode local, l'API ne charge que la Gironde (dept 33) depuis Hub'Eau. Étendre à l'ensemble du territoire pour que le mode DuckDB soit représentatif des données de production.

5. **Cache API** : chaque appel ré-exécute la requête SQL sur les tables Gold. L'ajout d'un cache HTTP (`Cache-Control`) ou d'un cache applicatif (Redis, `functools.lru_cache`) réduirait significativement la latence pour des données qui évoluent quotidiennement.

6. **Monitoring du pipeline** : aucune alerte n'est configurée en cas d'échec du job Databricks. Intégrer les notifications Databricks (email ou webhook Slack) permettrait de réagir rapidement sans surveiller manuellement le tableau de bord.

7. **Enrichissement des validations Great Expectations** : les expectations actuelles sont structurelles (non-null, plages de valeurs). Les compléter avec des checks métier — par exemple, s'assurer que le taux de conformité national calculé reste cohérent avec les données historiques — renforcerait la confiance dans la qualité des données produites.
