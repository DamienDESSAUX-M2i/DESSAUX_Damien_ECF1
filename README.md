# DESSAUX_Damien_ECF1

ECF1 de la formation Développeur Concepteur en Science des Donnée de M2i.

## Description

Ce projet à pour objectif de concevoir et implémenter une plateforme de collecte et d'analyse de données répondant aux exigences définies dans le document `docs/statement.md`.

# Architexture

```
Quotes
┌────────────────────────┐
│Web Scrapping           │
└─────────┬──────────────┘
          │ Scraping (Python)
          ▼
┌────────────────────────┐
│Pipeline ETL            │
└─────────┬──────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────────┐
│MinIO   │  │PostgreSQL  │
│        │  │            │
│Exports │  │authors     │
│        │  │quotes      │
│        │  │quotes_tags │
│        │  │tags        │
└────────┘  └────────────┘
```

```
Librairies
┌───────────────────────┐
│Excel Extraction       │
│API Extraction         │
└─────────┬─────────────┘
          │ Extractions (Python)
          ▼
┌───────────────────────┐
│Pipeline ETL           │
└─────────┬─────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌───────────┐
│MinIO   │  │PostgreSQL │
│        │  │           │
│Exports │  │librairies │
│        │  │ca_annuel  │
└────────┘  └───────────┘
```

```
Books
┌───────────────────────┐
│Web Scraping           │
└─────────┬─────────────┘
          │ Scraping (Python)
          ▼
┌───────────────────────┐
│Pipeline ETL           │
└─────────┬─────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌───────────┐
│MinIO   │  │PostgreSQL │
│        │  │           │
│Images  │  │books      │
│Exports │  │           │
└────────┘  └───────────┘
```

## Démarrage rapide

### Prérequis

- Python 3.13+
- Docker et Docker Compose
- Git

### Installation

1. Cloner le projet depuis GitHub.

```bash
git clone https://github.com/DamienDESSAUX-M2i/DESSAUX_Damien_ECF1.git
```

2. Créer un environement virtuel et installer les dépendances.

```bash
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement
## Linux/Mac:
source venv/bin/activate
## Windows:
venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

Si vous utiliser uv, initialisez le projet avec `uv sync`.

3. Créer un créer à la racine du projet un fichier environement `./env` comprenant les variables d'environnement suivantes :

```bash
# ===
# MinIO
# ===

MINIO_ROOT_USER="admin"
MINIO_ROOT_PASSWORD="admin0000"
MINIO_ENDPOINT="localhost:9000"
# bucket names
BUCKET_IMAGES="images"
BUCKET_EXPORTS="exports"
BUCKET_BACKUPS="backups"

# ===
# PostgreSQL
# ===

POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin0000"
POSTGRES_DB="mydb"
# pgadmin
PGADMIN_DEFAULT_EMAIL="admin@admin.com"
PGADMIN_DEFAULT_PASSWORD="admin0000"
```

4. Démarrer l'infrastructure Docker. Trois services seront lancés `minio`, `pgdb` et `pgadmin` qui correspondent respectivement aux images `quay.io/minio/minio`, `postgres` et `dpage/pgadmin4`.

```bash
docker-compose up -d
```

## Utilisation

### Exécuter le pipeline

```bash
# quotes pipeline
python main.py --quotes
## uv
uv run main.py --quotes

# librairies pipeline
python main.py --librairies
## uv
uv run main.py --librairies

# books pipeline
python main.py --books
## uv
uv run main.py --books
```

### Options disponibles

| Option | Description |
|--------|-------------|
| `--books` | Lance la pipeline books |
| `--quotes` | Lance la pipeline quotes |
| `--librairies` | Lance la pipeline partenaire librairies |

## Structure du projet

```
DESSAUX_DAMIEN_ECF1/
├───config
│   ├───settings.py
│   └───__init__.py
├───data
│   └───partenaire_librairies.xlsx
├───docs
│   ├───MLD
│   │   ├───MLD_books.jpg
│   │   ├───MLD_books.loo
│   │   ├───MLD_librairies.jpg
│   │   ├───MLD_librairies.loo
│   │   ├───MLD_quotes.jpg
│   │   └───MLD_quotes.loo
│   ├───statement.md
│   ├───DAT.md
│   └───RGPD_CONFORMITE.md
├───logs
├───postgresql_initdb
│   ├───01_tables_quotes.sql
│   ├───02_tables_librairies.sql
│   └───03_tables_books.sql
├───sql
│   └───analyses.sql
├───src
│   ├───extractors
│   │   ├───api_adress_extractor.py
│   │   ├───books_scrapper.py
│   │   ├───excel_extractor.py
│   │   ├───quotes_scrapper.py
│   │   └───__init__.py
│   ├───pipelines
│   │   ├───books_pipeline.py
│   │   ├───partenaire_librairies_pipeline.py
│   │   ├───quotes_pipeline.py
│   │   └───__init__.py
│   ├───storage
│   │   ├───minio_storage.py
│   │   ├───postgresql_storage.py
│   │   └───__init__.py
│   ├───utils
│   │   ├───logger.py
│   │   └───__init__.py
│   └───__init__.py
├───.gitignore
├───docker-compose.yaml
├───pyproject.toml
├───README.md
└───requirements.txt
```