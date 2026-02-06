# ECF DataPulse Analytics - Pipeline de Données Multi-Sources

## Titre Professionnel Data Engineer - RNCP35288
### Compétences évaluées : C1.1, C1.3, C1.4

---

## Description du projet

DataPulse Analytics est une plateforme de collecte et d'analyse de données multi-sources. Ce projet implémente un pipeline ETL complet permettant de :

- **Collecter** des données depuis plusieurs sources hétérogènes (web scraping, API, fichiers Excel)
- **Transformer** et nettoyer les données conformément aux exigences RGPD
- **Charger** les données dans une architecture Lakehouse pour l'analyse

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────┐
│   SOURCES   │───▶│   BRONZE    │───▶│   SILVER    │───▶│  GOLD   │
│             │    │  (MongoDB)  │    │  (MongoDB)  │    │(Postgres)│
│ • Scrapers  │    │  Données    │    │  Données    │    │ Données │
│ • API       │    │  brutes     │    │  nettoyées  │    │ prêtes  │
│ • Excel     │    │             │    │  RGPD OK    │    │ pour BI │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────┘
```

## Structure du projet

```
ecf-datapulse/
├── docker-compose.yml          # Infrastructure Docker
├── Dockerfile                   # Image du pipeline
├── requirements.txt             # Dépendances Python
├── README.md                    # Ce fichier
│
├── docs/
│   ├── DAT.md                   # Dossier d'Architecture Technique
│   └── RGPD_CONFORMITE.md       # Documentation RGPD
│
├── src/
│   ├── scrapers/
│   │   ├── books_scraper.py     # Scraper Books to Scrape
│   │   └── quotes_scraper.py    # Scraper Quotes to Scrape
│   ├── api/
│   │   └── geocoding_client.py  # Client API Adresse
│   ├── import/
│   │   └── excel_importer.py    # Import fichier Excel
│   └── pipeline/
│       └── etl_pipeline.py      # Pipeline ETL complet
│
├── sql/
│   └── analyses.sql             # 5 requêtes analytiques
│
├── init/
│   ├── mongo-init.js            # Initialisation MongoDB
│   └── postgres-init.sql        # Initialisation PostgreSQL
│
├── data/
│   └── partenaire_librairies.xlsx  # Fichier fourni
│
└── logs/                        # Logs d'exécution
```

## Prérequis

- Docker et Docker Compose
- Python 3.11+ (pour exécution locale)

## Installation

### 1. Démarrer l'infrastructure

```bash
# Démarrer tous les services
docker-compose up -d

# Vérifier l'état des services
docker-compose ps
```

### 2. Accès aux interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Mongo Express** | http://localhost:8081 | admin / admin2026 |
| **pgAdmin** | http://localhost:5050 | admin@datapulse.local / admin2026 |
| **MinIO Console** | http://localhost:9001 | datapulse / datapulse2026 |

### 3. Exécuter le pipeline

```bash
# Pipeline complet
docker-compose exec pipeline python -m pipeline.etl_pipeline --step all --excel /app/data/partenaire_librairies.xlsx

# Mode test (données limitées)
docker-compose exec pipeline python -m pipeline.etl_pipeline --step all --test --excel /app/data/partenaire_librairies.xlsx

# Étapes séparées
docker-compose exec pipeline python -m pipeline.etl_pipeline --step extract
docker-compose exec pipeline python -m pipeline.etl_pipeline --step transform
docker-compose exec pipeline python -m pipeline.etl_pipeline --step load
```

## Sources de données

### 1. Web Scraping

| Site | URL | Données |
|------|-----|---------|
| Books to Scrape | https://books.toscrape.com | ~1000 livres |
| Quotes to Scrape | https://quotes.toscrape.com | ~100 citations |

### 2. API

| API | URL | Usage |
|-----|-----|-------|
| API Adresse | https://api-adresse.data.gouv.fr | Géocodage des librairies |

### 3. Fichier Excel

Le fichier `partenaire_librairies.xlsx` contient 20 librairies partenaires avec des données personnelles nécessitant un traitement RGPD.

## Conformité RGPD

Les données personnelles (nom, email, téléphone des contacts) sont :

1. **Pseudonymisées** : Hashage SHA-256 avec sel
2. **Minimisées** : Seul le hash est conservé en couche Gold
3. **Supprimables** : Script de suppression sur demande disponible

Voir `docs/RGPD_CONFORMITE.md` pour plus de détails.

## Requêtes analytiques

Le fichier `sql/analyses.sql` contient 5 requêtes démontrant la valeur de la plateforme :

1. **Agrégation simple** : Statistiques par catégorie de livres
2. **Jointure** : Citations avec auteurs et tags
3. **Window function** : Classement des livres par prix dans leur catégorie
4. **Top N** : Top 10 des auteurs les plus prolifiques
5. **Croisement sources** : Opportunités commerciales librairies/livres

## Technologies utilisées

| Composant | Technologie | Justification |
|-----------|-------------|---------------|
| Données brutes | MongoDB | Flexibilité schéma, JSON natif |
| Données finales | PostgreSQL | SQL standard, window functions |
| Stockage fichiers | MinIO | Compatible S3, versioning |
| Pipeline | Python | Écosystème riche, lisibilité |
| Infrastructure | Docker | Reproductibilité, isolation |

## Commandes utiles

```bash
# Logs du pipeline
docker-compose logs -f pipeline

# Accès MongoDB
docker-compose exec mongodb mongosh -u admin -p datapulse2026

# Accès PostgreSQL
docker-compose exec postgres psql -U datapulse -d datapulse

# Arrêter l'infrastructure
docker-compose down

# Supprimer les données (volumes)
docker-compose down -v
```

## Auteur

Data Engineer - DataPulse Analytics
ECF Titre Professionnel Data Engineer

---

*Projet réalisé dans le cadre du Titre Professionnel Data Engineer (RNCP35288)*
