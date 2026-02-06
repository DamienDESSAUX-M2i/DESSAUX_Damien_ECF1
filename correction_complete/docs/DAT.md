# Dossier d'Architecture Technique (DAT)
## ECF Pipeline de Données Multi-Sources - DataPulse Analytics

**Version :** 1.0
**Date :** Janvier 2026
**Auteur :** Data Engineer - DataPulse Analytics

---

## Table des matières

1. [Contexte et objectifs](#1-contexte-et-objectifs)
2. [Architecture globale](#2-architecture-globale)
3. [Choix des technologies](#3-choix-des-technologies)
4. [Organisation des données](#4-organisation-des-données)
5. [Modélisation des données](#5-modélisation-des-données)
6. [Conformité RGPD](#6-conformité-rgpd)
7. [Diagrammes](#7-diagrammes)

---

## 1. Contexte et objectifs

### 1.1 Contexte métier

DataPulse Analytics est une startup spécialisée dans l'agrégation et l'analyse de données multi-sources. L'entreprise souhaite créer une plateforme capable de :

- Collecter des données depuis des sources hétérogènes (web scraping, API, fichiers)
- Stocker, nettoyer et transformer ces données
- Les rendre disponibles pour les analystes via SQL

### 1.2 Sources de données

| Source | Type | Volume estimé | Fréquence |
|--------|------|---------------|-----------|
| Books to Scrape | Web scraping | ~1000 livres | Hebdomadaire |
| Quotes to Scrape | Web scraping | ~100 citations | Hebdomadaire |
| API Adresse (gouv.fr) | API REST | ~20 géolocalisations | À la demande |
| Fichier partenaires | Excel | 20 librairies | Mensuel |

### 1.3 Exigences fonctionnelles

- **EF1** : Collecter des données depuis au moins 2 sites web par scraping
- **EF2** : Collecter des données depuis au moins 1 API REST
- **EF3** : Importer des données depuis 1 fichier Excel fourni
- **EF4** : Stocker les données brutes de manière pérenne
- **EF5** : Nettoyer et transformer les données collectées
- **EF6** : Rendre les données interrogeables via SQL
- **EF7** : Permettre des analyses croisées entre les sources

---

## 2. Architecture globale

### 2.1 Choix d'architecture : Data Lakehouse

J'ai choisi une architecture **Data Lakehouse** combinant les avantages du Data Lake et du Data Warehouse.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ARCHITECTURE LAKEHOUSE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │
│   │   SOURCES   │    │   BRONZE    │    │   SILVER    │    ┌─────────┐ │
│   │             │    │   (Brut)    │    │  (Nettoyé)  │    │  GOLD   │ │
│   │ • Scrapers  │───▶│             │───▶│             │───▶│ (Prêt)  │ │
│   │ • API       │    │  MongoDB    │    │  MongoDB    │    │         │ │
│   │ • Excel     │    │             │    │             │    │PostgreSQL│
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────┘ │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                      MinIO (Object Storage)                      │  │
│   │              Fichiers bruts, exports, sauvegardes                │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Justification du choix

| Critere | Data Lake | Data Warehouse | **Lakehouse (choisi)** |
|---------|-----------|----------------|------------------------|
| Donnees brutes | + Excellente flexibilite | - Schema rigide | + Flexibilite |
| Requetes SQL | - Performances limitees | + Optimise | + Couche SQL dediee |
| Cout | + Faible | - Eleve | + Equilibre |
| Evolutivite | + Excellente | ~ Moyenne | + Excellente |
| Gouvernance | - Complexe | + Native | + Structuree |

**Pourquoi pas uniquement un Data Lake ?**
- Les analystes ont besoin de requêtes SQL performantes
- Un Data Lake seul ne garantit pas la qualité des données

**Pourquoi pas uniquement un Data Warehouse ?**
- Les sources sont hétérogènes (JSON, HTML, Excel)
- Besoin de conserver les données brutes pour retraitement
- Trop rigide pour notre cas d'usage évolutif

**Avantages du Lakehouse :**
- Conservation des données brutes (traçabilité)
- Transformation progressive (Bronze → Silver → Gold)
- Performances SQL sur les données finales
- Flexibilité pour ajouter de nouvelles sources

**Inconvénients :**
- Complexité accrue (3 couches à maintenir)
- Nécessite une bonne orchestration

---

## 3. Choix des technologies

### 3.1 Stockage des données brutes : MongoDB

**Choix :** MongoDB (base NoSQL documentaire)

**Justification :**
- Schéma flexible pour données hétérogènes (JSON natif)
- Adapté aux documents semi-structurés (résultats de scraping)
- Facilité d'insertion sans définition préalable du schéma
- Requêtes d'agrégation puissantes pour les transformations

**Alternative considérée : MinIO (stockage fichiers)**

| Critere | MongoDB | MinIO |
|---------|---------|-------|
| Flexibilite schema | + Excellente | + Excellente |
| Requetes sur donnees | + Native | - Necessite outil externe |
| Recherche dans documents | + Indexation | - Non natif |
| Stockage fichiers volumineux | ~ Limite 16MB/doc | + Illimite |

**Conclusion :** MongoDB pour les données structurées/semi-structurées, MinIO pour les fichiers bruts (images, Excel originaux).

### 3.2 Stockage des données transformées : PostgreSQL

**Choix :** PostgreSQL

**Justification :**
- Standard SQL complet (CTE, window functions, JSON)
- Performances excellentes avec indexation appropriée
- Écosystème riche (extensions, outils de BI)
- Open source, gratuit, largement adopté
- Types de données avancés (JSON, arrays, géométrie)

**Alternative considérée : MySQL**

| Critere | PostgreSQL | MySQL |
|---------|------------|-------|
| Conformite SQL | + Complete | ~ Partielle |
| Window functions | + Completes | ~ Limitees |
| Types JSON | + JSONB performant | ~ Moins optimise |
| Extensibilite | + Excellente | ~ Limitee |

### 3.3 Stockage fichiers : MinIO

**Choix :** MinIO (compatible S3)

**Justification :**
- Compatible API Amazon S3 (standard de l'industrie)
- Stockage objet performant et scalable
- Interface web pour exploration
- Versioning des fichiers
- Parfait pour : fichiers Excel originaux, images scrapées, exports

### 3.4 Récapitulatif des technologies

| Composant | Technologie | Port | Usage |
|-----------|-------------|------|-------|
| Données brutes | MongoDB | 27017 | Couche Bronze/Silver |
| Données finales | PostgreSQL | 5432 | Couche Gold + SQL |
| Fichiers | MinIO | 9000/9001 | Excel, images, backups |
| Admin MongoDB | Mongo Express | 8081 | Interface web MongoDB |
| Admin PostgreSQL | pgAdmin | 5050 | Interface web PostgreSQL |

---

## 4. Organisation des données

### 4.1 Architecture en couches (Medallion)

```
┌────────────────────────────────────────────────────────────────────┐
│                        COUCHE BRONZE (Brut)                        │
│                         MongoDB: db_bronze                         │
├────────────────────────────────────────────────────────────────────┤
│  • raw_books          : Livres scrapés (données brutes)            │
│  • raw_quotes         : Citations scrapées (données brutes)        │
│  • raw_librairies     : Import Excel brut                          │
│  • raw_geocoding      : Réponses API brutes                        │
│  Caractéristiques : Aucune transformation, horodatage d'ingestion  │
└────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────┐
│                       COUCHE SILVER (Nettoyé)                      │
│                         MongoDB: db_silver                         │
├────────────────────────────────────────────────────────────────────┤
│  • clean_books        : Prix convertis €, notes normalisées        │
│  • clean_quotes       : Textes nettoyés, tags normalisés           │
│  • clean_librairies   : Données RGPD conformes, géocodées          │
│  Caractéristiques : Nettoyage, normalisation, conformité RGPD      │
└────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────┐
│                        COUCHE GOLD (Final)                         │
│                       PostgreSQL: datapulse                        │
├────────────────────────────────────────────────────────────────────┤
│  Tables relationnelles optimisées pour l'analyse :                 │
│  • dim_categories     : Dimension catégories de livres             │
│  • dim_authors        : Dimension auteurs de citations             │
│  • dim_librairies     : Dimension librairies partenaires           │
│  • dim_tags           : Dimension tags de citations                │
│  • fact_books         : Table de faits livres                      │
│  • fact_quotes        : Table de faits citations                   │
│  Caractéristiques : Modèle en étoile, indexé, prêt pour BI         │
└────────────────────────────────────────────────────────────────────┘
```

### 4.2 Convention de nommage

| Élément | Convention | Exemple |
|---------|------------|---------|
| Collections Bronze | `raw_{source}` | `raw_books`, `raw_quotes` |
| Collections Silver | `clean_{source}` | `clean_books`, `clean_quotes` |
| Tables dimensions | `dim_{entité}` | `dim_categories`, `dim_authors` |
| Tables de faits | `fact_{entité}` | `fact_books`, `fact_quotes` |
| Colonnes | `snake_case` | `prix_euros`, `date_scraping` |
| Timestamps | `{action}_at` | `created_at`, `scraped_at` |

### 4.3 Métadonnées techniques

Chaque document/enregistrement contient :

```json
{
  "_metadata": {
    "source": "books.toscrape.com",
    "scraped_at": "2026-01-22T10:30:00Z",
    "pipeline_version": "1.0",
    "batch_id": "batch_20260122_001"
  }
}
```

---

## 5. Modélisation des données

### 5.1 Modèle conceptuel

Le modèle suit une approche **en étoile (star schema)** pour optimiser les requêtes analytiques.

### 5.2 Schéma Entité-Relation

```
                              ┌─────────────────────┐
                              │   dim_categories    │
                              ├─────────────────────┤
                              │ PK category_id      │
                              │    category_name    │
                              │    category_slug    │
                              └──────────┬──────────┘
                                         │
                                         │ 1:N
                                         ▼
┌─────────────────────┐        ┌─────────────────────┐
│    dim_authors      │        │     fact_books      │
├─────────────────────┤        ├─────────────────────┤
│ PK author_id        │        │ PK book_id          │
│    author_name      │        │ FK category_id      │
│    author_slug      │        │    title            │
└──────────┬──────────┘        │    price_gbp        │
           │                   │    price_eur        │
           │ 1:N               │    rating           │
           ▼                   │    availability     │
┌─────────────────────┐        │    url              │
│    fact_quotes      │        │    scraped_at       │
├─────────────────────┤        └─────────────────────┘
│ PK quote_id         │
│ FK author_id        │        ┌─────────────────────┐
│    quote_text       │        │   dim_librairies    │
│    scraped_at       │        ├─────────────────────┤
└──────────┬──────────┘        │ PK librairie_id     │
           │                   │    nom              │
           │ N:M               │    adresse          │
           ▼                   │    code_postal      │
┌─────────────────────┐        │    ville            │
│   quote_tags        │        │    latitude         │
├─────────────────────┤        │    longitude        │
│ FK quote_id         │        │    specialite       │
│ FK tag_id           │        │    date_partenariat │
└─────────────────────┘        │    contact_hash     │
           │                   └─────────────────────┘
           │ N:1
           ▼
┌─────────────────────┐
│      dim_tags       │
├─────────────────────┤
│ PK tag_id           │
│    tag_name         │
│    tag_slug         │
└─────────────────────┘
```

### 5.3 Description des tables

#### Table `dim_categories`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| category_id | SERIAL | PK | Identifiant unique |
| category_name | VARCHAR(100) | NOT NULL, UNIQUE | Nom de la catégorie |
| category_slug | VARCHAR(100) | NOT NULL | Slug URL |

#### Table `fact_books`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| book_id | SERIAL | PK | Identifiant unique |
| category_id | INTEGER | FK → dim_categories | Catégorie du livre |
| title | VARCHAR(255) | NOT NULL | Titre du livre |
| price_gbp | DECIMAL(10,2) | | Prix en livres sterling |
| price_eur | DECIMAL(10,2) | | Prix converti en euros |
| rating | INTEGER | CHECK 1-5 | Note (étoiles) |
| availability | INTEGER | | Stock disponible |
| url | VARCHAR(500) | | URL de la page |
| scraped_at | TIMESTAMP | NOT NULL | Date de scraping |

#### Table `dim_authors`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| author_id | SERIAL | PK | Identifiant unique |
| author_name | VARCHAR(200) | NOT NULL, UNIQUE | Nom de l'auteur |
| author_slug | VARCHAR(200) | | Slug URL |

#### Table `fact_quotes`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| quote_id | SERIAL | PK | Identifiant unique |
| author_id | INTEGER | FK → dim_authors | Auteur de la citation |
| quote_text | TEXT | NOT NULL | Texte de la citation |
| scraped_at | TIMESTAMP | NOT NULL | Date de scraping |

#### Table `dim_tags`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| tag_id | SERIAL | PK | Identifiant unique |
| tag_name | VARCHAR(100) | NOT NULL, UNIQUE | Nom du tag |
| tag_slug | VARCHAR(100) | | Slug URL |

#### Table `quote_tags` (association N:M)
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| quote_id | INTEGER | FK → fact_quotes | Citation |
| tag_id | INTEGER | FK → dim_tags | Tag |
| | | PK (quote_id, tag_id) | Clé composite |

#### Table `dim_librairies`
| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| librairie_id | SERIAL | PK | Identifiant unique |
| nom | VARCHAR(200) | NOT NULL | Nom de la librairie |
| adresse | VARCHAR(300) | | Adresse postale |
| code_postal | VARCHAR(10) | | Code postal |
| ville | VARCHAR(100) | | Ville |
| latitude | DECIMAL(10,8) | | Latitude (géocodage) |
| longitude | DECIMAL(11,8) | | Longitude (géocodage) |
| specialite | VARCHAR(100) | | Spécialité littéraire |
| date_partenariat | DATE | | Date début partenariat |
| contact_hash | VARCHAR(64) | | Hash SHA-256 du contact (RGPD) |

### 5.4 Justification des choix de modélisation

1. **Modèle en étoile** : Optimise les requêtes analytiques avec des jointures simples entre faits et dimensions.

2. **Séparation dimensions/faits** :
   - Évite la redondance (un auteur n'est stocké qu'une fois)
   - Facilite les mises à jour des dimensions
   - Permet des analyses par dimension (par catégorie, par auteur, etc.)

3. **Table d'association `quote_tags`** : Gère la relation N:M entre citations et tags sans duplication.

4. **Colonnes de prix (GBP et EUR)** : Conservation de la valeur originale pour traçabilité + valeur convertie pour analyse.

5. **Contact hashé** : Conformité RGPD tout en gardant une capacité de vérification/réconciliation.

---

## 6. Conformité RGPD

### 6.1 Données personnelles identifiées

| Source | Donnée | Catégorie RGPD | Traitement |
|--------|--------|----------------|------------|
| Excel partenaires | contact_nom | Donnée personnelle | Hashage SHA-256 |
| Excel partenaires | contact_email | Donnée personnelle | Hashage SHA-256 |
| Excel partenaires | contact_telephone | Donnée personnelle | Hashage SHA-256 |
| Quotes to Scrape | author_name | Donnée publique | Conservation (données publiques) |

### 6.2 Mesures de protection

1. **Pseudonymisation** : Les données personnelles (nom, email, téléphone) sont hashées avec SHA-256 avant stockage.

2. **Minimisation** : Seules les données nécessaires à l'analyse sont conservées dans la couche Gold.

3. **Séparation** : Les données brutes avec informations personnelles sont dans la couche Bronze avec accès restreint.

4. **Chiffrement** : Les bases de données sont accessibles uniquement via le réseau Docker interne.

### 6.3 Droit à l'effacement

Procédure documentée dans `RGPD_CONFORMITE.md` permettant de supprimer toutes les données d'un contact sur demande.

---

## 7. Diagrammes

### 7.1 Flux de données

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           FLUX DE DONNÉES                                │
└──────────────────────────────────────────────────────────────────────────┘

  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │   SOURCES   │     │   BRONZE    │     │   SILVER    │     ┌─────────┐
  │             │     │             │     │             │     │  GOLD   │
  │ ┌─────────┐ │     │ ┌─────────┐ │     │ ┌─────────┐ │     │         │
  │ │ Books   │─┼────▶│ │raw_books│─┼────▶│ │clean_   │─┼────▶│fact_    │
  │ │ Scraper │ │     │ │         │ │     │ │books    │ │     │books    │
  │ └─────────┘ │     │ └─────────┘ │     │ └─────────┘ │     │         │
  │             │     │             │     │             │     │dim_     │
  │ ┌─────────┐ │     │ ┌─────────┐ │     │ ┌─────────┐ │     │categories│
  │ │ Quotes  │─┼────▶│ │raw_     │─┼────▶│ │clean_   │─┼────▶│         │
  │ │ Scraper │ │     │ │quotes   │ │     │ │quotes   │ │     │fact_    │
  │ └─────────┘ │     │ └─────────┘ │     │ └─────────┘ │     │quotes   │
  │             │     │             │     │             │     │         │
  │ ┌─────────┐ │     │ ┌─────────┐ │     │ ┌─────────┐ │     │dim_     │
  │ │ API     │─┼────▶│ │raw_     │─┼────▶│ │clean_   │─┼────▶│authors  │
  │ │ Adresse │ │     │ │geocoding│ │     │ │librairies│     │         │
  │ └─────────┘ │     │ └─────────┘ │     │ └─────────┘ │     │dim_     │
  │             │     │             │     │             │     │librairies│
  │ ┌─────────┐ │     │ ┌─────────┐ │     │             │     │         │
  │ │ Excel   │─┼────▶│ │raw_     │─┼─────┘             │     │dim_tags │
  │ │ Import  │ │     │ │librairies│                    │     │         │
  │ └─────────┘ │     │ └─────────┘ │                   │     │quote_   │
  │             │     │             │                   │     │tags     │
  └─────────────┘     └─────────────┘                   │     └─────────┘
                                                        │
                      MongoDB                           │     PostgreSQL
                                                        │
                      ┌─────────────────────────────────┘
                      │
                      ▼
                ┌───────────┐
                │   MinIO   │
                │ ┌───────┐ │
                │ │.xlsx  │ │  Fichiers originaux
                │ │.json  │ │  Exports
                │ │.csv   │ │  Backups
                │ └───────┘ │
                └───────────┘
```

### 7.2 Architecture technique Docker

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE NETWORK                           │
│                            datapulse_network                             │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐             │
│  │    MongoDB     │  │   PostgreSQL   │  │     MinIO      │             │
│  │   Port 27017   │  │   Port 5432    │  │  Port 9000/01  │             │
│  │                │  │                │  │                │             │
│  │  db_bronze     │  │  datapulse     │  │  bucket:raw    │             │
│  │  db_silver     │  │                │  │  bucket:export │             │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘             │
│          │                   │                   │                       │
│          └───────────────────┼───────────────────┘                       │
│                              │                                           │
│  ┌────────────────┐  ┌───────┴────────┐  ┌────────────────┐             │
│  │ Mongo Express  │  │    Pipeline    │  │    pgAdmin     │             │
│  │   Port 8081    │  │   (Python)     │  │   Port 5050    │             │
│  │                │  │                │  │                │             │
│  │  Admin MongoDB │  │  ETL Process   │  │ Admin Postgres │             │
│  └────────────────┘  └────────────────┘  └────────────────┘             │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Exposed Ports
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              HOST                                        │
│  localhost:27017 (MongoDB)   localhost:9001 (MinIO Console)              │
│  localhost:5432  (Postgres)  localhost:8081 (Mongo Express)              │
│  localhost:9000  (MinIO API) localhost:5050 (pgAdmin)                    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Annexes

### A. Glossaire

| Terme | Définition |
|-------|------------|
| Bronze | Couche de données brutes, non transformées |
| Silver | Couche de données nettoyées et validées |
| Gold | Couche de données prêtes pour l'analyse |
| ETL | Extract, Transform, Load |
| Lakehouse | Architecture combinant Data Lake et Data Warehouse |

### B. Références

- [MongoDB Documentation](https://docs.mongodb.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [MinIO Documentation](https://min.io/docs/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
