#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline ETL complet pour DataPulse Analytics
ECF Data Engineer

Ce pipeline orchestre :
1. Collecte (Extract) : Scrapers web + API + Import Excel
2. Transformation : Nettoyage, conversion, enrichissement
3. Chargement (Load) : MongoDB (Bronze/Silver) → PostgreSQL (Gold)
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import uuid

# Ajouter le répertoire parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/pipeline_{datetime.utcnow().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger('ETLPipeline')

# =============================================================================
# Configuration
# =============================================================================

# MongoDB
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'datapulse2026')

# PostgreSQL
PG_HOST = os.getenv('POSTGRES_HOST', 'localhost')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_USER = os.getenv('POSTGRES_USER', 'datapulse')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'datapulse2026')
PG_DATABASE = os.getenv('POSTGRES_DB', 'datapulse')

# Taux de conversion
GBP_TO_EUR = 1.17  # Livre sterling vers Euro


# =============================================================================
# Classe principale du pipeline
# =============================================================================

class ETLPipeline:
    """
    Pipeline ETL pour DataPulse Analytics.
    Gère le flux de données : Sources → Bronze → Silver → Gold
    """

    def __init__(self):
        """Initialise les connexions aux bases de données."""
        self.batch_id = f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.mongo_client = None
        self.pg_conn = None

        self.stats = {
            'batch_id': self.batch_id,
            'start_time': None,
            'end_time': None,
            'books': {'extracted': 0, 'transformed': 0, 'loaded': 0},
            'quotes': {'extracted': 0, 'transformed': 0, 'loaded': 0},
            'librairies': {'extracted': 0, 'transformed': 0, 'loaded': 0},
            'errors': []
        }

        logger.info(f"Pipeline initialisé - Batch ID: {self.batch_id}")

    def connect(self):
        """Établit les connexions aux bases de données."""
        # MongoDB
        mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        self.mongo_client = MongoClient(mongo_uri)
        self.db_bronze = self.mongo_client['db_bronze']
        self.db_silver = self.mongo_client['db_silver']
        logger.info("[OK] Connexion MongoDB etablie")

        # PostgreSQL
        self.pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        logger.info("[OK] Connexion PostgreSQL etablie")

    def disconnect(self):
        """Ferme les connexions."""
        if self.mongo_client:
            self.mongo_client.close()
        if self.pg_conn:
            self.pg_conn.close()
        logger.info("Connexions fermées")

    # =========================================================================
    # EXTRACT - Collecte des données
    # =========================================================================

    def extract_books(self, limit_categories: int = None) -> List[Dict]:
        """
        Extrait les livres depuis Books to Scrape.

        Args:
            limit_categories: Limiter le nombre de catégories (pour tests)

        Returns:
            Liste des livres scrapés
        """
        from scrapers.books_scraper import BooksScraper

        logger.info("[BOOKS] Extraction des livres...")
        scraper = BooksScraper()
        books = scraper.scrape_all(limit_categories=limit_categories)

        self.stats['books']['extracted'] = len(books)
        logger.info(f"  → {len(books)} livres extraits")

        # Sauvegarder en Bronze
        if books:
            # Convertir les dates pour MongoDB
            for book in books:
                if isinstance(book['_metadata']['scraped_at'], str):
                    book['_metadata']['scraped_at'] = datetime.fromisoformat(book['_metadata']['scraped_at'])

            self.db_bronze['raw_books'].insert_many(books)
            logger.info(f"  → Sauvegardé dans Bronze (raw_books)")

        return books

    def extract_quotes(self, max_pages: int = None) -> List[Dict]:
        """
        Extrait les citations depuis Quotes to Scrape.

        Args:
            max_pages: Limiter le nombre de pages (pour tests)

        Returns:
            Liste des citations scrapées
        """
        from scrapers.quotes_scraper import QuotesScraper

        logger.info("[QUOTES] Extraction des citations...")
        scraper = QuotesScraper()
        quotes = scraper.scrape_all(max_pages=max_pages)

        self.stats['quotes']['extracted'] = len(quotes)
        logger.info(f"  → {len(quotes)} citations extraites")

        # Sauvegarder en Bronze
        if quotes:
            for quote in quotes:
                if isinstance(quote['_metadata']['scraped_at'], str):
                    quote['_metadata']['scraped_at'] = datetime.fromisoformat(quote['_metadata']['scraped_at'])

            self.db_bronze['raw_quotes'].insert_many(quotes)
            logger.info(f"  → Sauvegardé dans Bronze (raw_quotes)")

        return quotes

    def extract_librairies(self, excel_path: str) -> List[Dict]:
        """
        Extrait les librairies depuis le fichier Excel.

        Args:
            excel_path: Chemin vers le fichier Excel

        Returns:
            Liste des enregistrements importés
        """
        from importers.excel_importer import ExcelImporter
        from api.geocoding_client import GeocodingClient

        logger.info("[LIBRAIRIES] Extraction des librairies...")
        importer = ExcelImporter()
        silver_records, bronze_records = importer.import_file(excel_path)

        self.stats['librairies']['extracted'] = len(bronze_records)
        logger.info(f"  → {len(bronze_records)} librairies extraites")

        # Sauvegarder les données brutes en Bronze
        if bronze_records:
            for record in bronze_records:
                if isinstance(record['_metadata']['imported_at'], datetime):
                    pass  # Déjà datetime
                else:
                    record['_metadata']['imported_at'] = datetime.fromisoformat(record['_metadata']['imported_at'])

            self.db_bronze['raw_librairies'].insert_many(bronze_records)
            logger.info(f"  → Sauvegardé dans Bronze (raw_librairies)")

        # Géocodage des adresses
        logger.info("[GEO] Geocodage des adresses...")
        geocoder = GeocodingClient()
        addresses = importer.get_addresses_for_geocoding(silver_records)
        geo_results = geocoder.geocode_batch(addresses)

        # Enrichir les données avec les coordonnées
        for i, result in enumerate(geo_results):
            if result.get('latitude'):
                silver_records[i]['latitude'] = result['latitude']
                silver_records[i]['longitude'] = result['longitude']

        # Sauvegarder les réponses géocodage en Bronze
        geo_bronze = [{
            'result': result,
            '_metadata': {
                'source': 'api-adresse.data.gouv.fr',
                'queried_at': datetime.utcnow(),
                'batch_id': self.batch_id
            }
        } for result in geo_results if result.get('latitude')]

        if geo_bronze:
            self.db_bronze['raw_geocoding'].insert_many(geo_bronze)

        return silver_records

    # =========================================================================
    # TRANSFORM - Transformation des données
    # =========================================================================

    def transform_books(self, books: List[Dict]) -> List[Dict]:
        """
        Transforme les données des livres.

        Transformations appliquées :
        - Conversion prix GBP → EUR
        - Normalisation des notes (texte → int)
        - Nettoyage des textes
        """
        logger.info("[TRANSFORM] Transformation des livres...")
        transformed = []

        rating_map = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5}

        for book in books:
            try:
                # Extraction du prix
                price_str = book.get('price', '£0.00')
                price_gbp = float(price_str.replace('£', '').replace(',', '.').strip())
                price_eur = round(price_gbp * GBP_TO_EUR, 2)

                # Conversion de la note
                rating_str = book.get('rating', '0').lower()
                rating = rating_map.get(rating_str, int(rating_str) if rating_str.isdigit() else 0)

                # Disponibilité
                availability_str = book.get('availability', '0')
                availability = int(availability_str) if availability_str.isdigit() else 0

                transformed_book = {
                    'title': book.get('title', '').strip(),
                    'category': book.get('category', 'Uncategorized').strip(),
                    'price_gbp': price_gbp,
                    'price_eur': price_eur,
                    'rating': rating,
                    'availability': availability,
                    'url': book.get('url', ''),
                    'image_url': book.get('image_url', ''),
                    'scraped_at': book['_metadata']['scraped_at'],
                    '_metadata': {
                        'source': book['_metadata']['source'],
                        'transformed_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'transformations': ['price_gbp_to_eur', 'rating_normalized', 'text_cleaned']
                    }
                }

                transformed.append(transformed_book)

            except Exception as e:
                logger.warning(f"Erreur transformation livre '{book.get('title', '?')}': {e}")
                self.stats['errors'].append(f"Book transform: {e}")

        self.stats['books']['transformed'] = len(transformed)
        logger.info(f"  → {len(transformed)} livres transformés")

        # Sauvegarder en Silver
        if transformed:
            self.db_silver['clean_books'].insert_many(transformed)
            logger.info(f"  → Sauvegardé dans Silver (clean_books)")

        return transformed

    def transform_quotes(self, quotes: List[Dict]) -> List[Dict]:
        """
        Transforme les données des citations.

        Transformations appliquées :
        - Nettoyage des textes (guillemets, espaces)
        - Normalisation des tags (lowercase, slugify)
        - Déduplication par hash du texte
        """
        import hashlib

        logger.info("[TRANSFORM] Transformation des citations...")
        transformed = []
        seen_hashes = set()

        for quote in quotes:
            try:
                text = quote.get('text', '').strip()

                # Créer un hash pour déduplication
                text_hash = hashlib.md5(text.lower().encode()).hexdigest()

                if text_hash in seen_hashes:
                    continue  # Doublon

                seen_hashes.add(text_hash)

                # Normaliser les tags
                tags = [tag.lower().strip() for tag in quote.get('tags', [])]

                transformed_quote = {
                    'text': text,
                    'text_hash': text_hash,
                    'author': quote.get('author', 'Unknown').strip(),
                    'author_slug': quote.get('author', '').lower().replace(' ', '-'),
                    'tags': tags,
                    'tag_count': len(tags),
                    'text_length': len(text),
                    'scraped_at': quote['_metadata']['scraped_at'],
                    '_metadata': {
                        'source': quote['_metadata']['source'],
                        'transformed_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'transformations': ['text_cleaned', 'tags_normalized', 'deduplicated']
                    }
                }

                transformed.append(transformed_quote)

            except Exception as e:
                logger.warning(f"Erreur transformation citation: {e}")
                self.stats['errors'].append(f"Quote transform: {e}")

        self.stats['quotes']['transformed'] = len(transformed)
        logger.info(f"  → {len(transformed)} citations transformées ({len(quotes) - len(transformed)} doublons supprimés)")

        # Sauvegarder en Silver
        if transformed:
            self.db_silver['clean_quotes'].insert_many(transformed)
            logger.info(f"  → Sauvegardé dans Silver (clean_quotes)")

        return transformed

    def transform_librairies(self, librairies: List[Dict]) -> List[Dict]:
        """
        Transforme les données des librairies (déjà anonymisées).

        Les données sont déjà traitées par l'importeur Excel (RGPD).
        Cette étape ajoute des métadonnées de transformation.
        """
        logger.info("[TRANSFORM] Transformation des librairies...")

        transformed = []
        for lib in librairies:
            try:
                transformed_lib = {
                    'nom': lib.get('nom', '').strip(),
                    'adresse': lib.get('adresse', '').strip(),
                    'code_postal': lib.get('code_postal', '').strip(),
                    'ville': lib.get('ville', '').strip().title(),  # Capitalisation
                    'specialite': lib.get('specialite', '').strip() if lib.get('specialite') else None,
                    'date_partenariat': lib.get('date_partenariat'),
                    'ca_annuel_range': lib.get('ca_annuel_range'),
                    'contact_hash': lib.get('contact_hash'),
                    'latitude': lib.get('latitude'),
                    'longitude': lib.get('longitude'),
                    '_metadata': {
                        'source': 'partenaire_librairies.xlsx',
                        'transformed_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'rgpd_compliant': True
                    }
                }
                transformed.append(transformed_lib)

            except Exception as e:
                logger.warning(f"Erreur transformation librairie: {e}")
                self.stats['errors'].append(f"Librairie transform: {e}")

        self.stats['librairies']['transformed'] = len(transformed)
        logger.info(f"  → {len(transformed)} librairies transformées")

        # Sauvegarder en Silver
        if transformed:
            for lib in transformed:
                lib['_metadata']['transformed_at'] = lib['_metadata']['transformed_at']
            self.db_silver['clean_librairies'].insert_many(transformed)
            logger.info(f"  → Sauvegardé dans Silver (clean_librairies)")

        return transformed

    # =========================================================================
    # LOAD - Chargement vers PostgreSQL (Gold)
    # =========================================================================

    def load_books_to_gold(self, books: List[Dict]):
        """
        Charge les livres dans PostgreSQL (couche Gold).
        """
        logger.info("[LOAD] Chargement des livres vers Gold...")

        cursor = self.pg_conn.cursor()

        # 1. Charger les catégories (dimensions)
        categories = list(set(book['category'] for book in books))

        for category in categories:
            cursor.execute("""
                INSERT INTO dim_categories (category_name, category_slug)
                VALUES (%s, %s)
                ON CONFLICT (category_name) DO NOTHING
            """, (category, category.lower().replace(' ', '-')))

        self.pg_conn.commit()
        logger.info(f"  → {len(categories)} catégories chargées")

        # Récupérer les IDs des catégories
        cursor.execute("SELECT category_id, category_name FROM dim_categories")
        category_map = {name: id for id, name in cursor.fetchall()}

        # 2. Charger les livres (faits)
        book_values = [
            (
                category_map.get(book['category']),
                book['title'],
                book['price_gbp'],
                book['price_eur'],
                book['rating'],
                book['availability'],
                book['url'],
                book.get('image_url'),
                book['scraped_at']
            )
            for book in books
        ]

        execute_values(cursor, """
            INSERT INTO fact_books (category_id, title, price_gbp, price_eur, rating, availability, url, image_url, scraped_at)
            VALUES %s
        """, book_values)

        self.pg_conn.commit()
        self.stats['books']['loaded'] = len(book_values)
        logger.info(f"  → {len(book_values)} livres chargés dans fact_books")

    def load_quotes_to_gold(self, quotes: List[Dict]):
        """
        Charge les citations dans PostgreSQL (couche Gold).
        """
        logger.info("[LOAD] Chargement des citations vers Gold...")

        cursor = self.pg_conn.cursor()

        # 1. Charger les auteurs (dimensions)
        authors = list(set(quote['author'] for quote in quotes))

        for author in authors:
            cursor.execute("""
                INSERT INTO dim_authors (author_name, author_slug)
                VALUES (%s, %s)
                ON CONFLICT (author_name) DO NOTHING
            """, (author, author.lower().replace(' ', '-')))

        self.pg_conn.commit()
        logger.info(f"  → {len(authors)} auteurs chargés")

        # Récupérer les IDs des auteurs
        cursor.execute("SELECT author_id, author_name FROM dim_authors")
        author_map = {name: id for id, name in cursor.fetchall()}

        # 2. Charger les tags (dimensions)
        all_tags = set()
        for quote in quotes:
            all_tags.update(quote.get('tags', []))

        for tag in all_tags:
            cursor.execute("""
                INSERT INTO dim_tags (tag_name, tag_slug)
                VALUES (%s, %s)
                ON CONFLICT (tag_name) DO NOTHING
            """, (tag, tag.lower().replace(' ', '-')))

        self.pg_conn.commit()
        logger.info(f"  → {len(all_tags)} tags chargés")

        # Récupérer les IDs des tags
        cursor.execute("SELECT tag_id, tag_name FROM dim_tags")
        tag_map = {name: id for id, name in cursor.fetchall()}

        # 3. Charger les citations (faits)
        for quote in quotes:
            cursor.execute("""
                INSERT INTO fact_quotes (author_id, quote_text, quote_hash, scraped_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (quote_hash) DO NOTHING
                RETURNING quote_id
            """, (
                author_map.get(quote['author']),
                quote['text'],
                quote['text_hash'],
                quote['scraped_at']
            ))

            result = cursor.fetchone()
            if result:
                quote_id = result[0]

                # 4. Charger les associations quote-tags
                for tag in quote.get('tags', []):
                    tag_id = tag_map.get(tag)
                    if tag_id:
                        cursor.execute("""
                            INSERT INTO quote_tags (quote_id, tag_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (quote_id, tag_id))

        self.pg_conn.commit()
        self.stats['quotes']['loaded'] = len(quotes)
        logger.info(f"  → {len(quotes)} citations chargées dans fact_quotes")

    def load_librairies_to_gold(self, librairies: List[Dict]):
        """
        Charge les librairies dans PostgreSQL (couche Gold).
        """
        logger.info("[LOAD] Chargement des librairies vers Gold...")

        cursor = self.pg_conn.cursor()

        lib_values = [
            (
                lib['nom'],
                lib['adresse'],
                lib['code_postal'],
                lib['ville'],
                lib['latitude'],
                lib['longitude'],
                lib['specialite'],
                lib['date_partenariat'],
                lib['contact_hash'],
                lib['ca_annuel_range'],
                datetime.utcnow()
            )
            for lib in librairies
        ]

        execute_values(cursor, """
            INSERT INTO dim_librairies (nom, adresse, code_postal, ville, latitude, longitude, specialite, date_partenariat, contact_hash, ca_annuel_range, imported_at)
            VALUES %s
        """, lib_values)

        self.pg_conn.commit()
        self.stats['librairies']['loaded'] = len(lib_values)
        logger.info(f"  → {len(lib_values)} librairies chargées dans dim_librairies")

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    def run_full_pipeline(
        self,
        excel_path: str,
        limit_categories: int = None,
        max_quote_pages: int = None
    ):
        """
        Exécute le pipeline complet.

        Args:
            excel_path: Chemin vers le fichier Excel des librairies
            limit_categories: Limiter les catégories de livres (tests)
            max_quote_pages: Limiter les pages de citations (tests)
        """
        self.stats['start_time'] = datetime.utcnow()

        logger.info("=" * 70)
        logger.info("DEMARRAGE DU PIPELINE ETL COMPLET")
        logger.info(f"   Batch ID: {self.batch_id}")
        logger.info("=" * 70)

        try:
            self.connect()

            # EXTRACT
            logger.info("\n" + "=" * 70)
            logger.info("PHASE 1: EXTRACTION (Extract)")
            logger.info("=" * 70)

            books = self.extract_books(limit_categories=limit_categories)
            quotes = self.extract_quotes(max_pages=max_quote_pages)
            librairies = self.extract_librairies(excel_path)

            # TRANSFORM
            logger.info("\n" + "=" * 70)
            logger.info("PHASE 2: TRANSFORMATION (Transform)")
            logger.info("=" * 70)

            transformed_books = self.transform_books(books)
            transformed_quotes = self.transform_quotes(quotes)
            transformed_librairies = self.transform_librairies(librairies)

            # LOAD
            logger.info("\n" + "=" * 70)
            logger.info("PHASE 3: CHARGEMENT (Load)")
            logger.info("=" * 70)

            self.load_books_to_gold(transformed_books)
            self.load_quotes_to_gold(transformed_quotes)
            self.load_librairies_to_gold(transformed_librairies)

        except Exception as e:
            logger.error(f"[ERREUR] Erreur pipeline: {e}")
            self.stats['errors'].append(str(e))
            raise

        finally:
            self.disconnect()
            self.stats['end_time'] = datetime.utcnow()

        # Résumé final
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("[OK] PIPELINE TERMINE AVEC SUCCES")
        logger.info("=" * 70)
        logger.info(f"  Batch ID: {self.batch_id}")
        logger.info(f"  Durée totale: {duration:.1f} secondes")
        logger.info("")
        logger.info("  Livres:")
        logger.info(f"     Extraits: {self.stats['books']['extracted']}")
        logger.info(f"     Transformés: {self.stats['books']['transformed']}")
        logger.info(f"     Chargés: {self.stats['books']['loaded']}")
        logger.info("")
        logger.info("  Citations:")
        logger.info(f"     Extraites: {self.stats['quotes']['extracted']}")
        logger.info(f"     Transformées: {self.stats['quotes']['transformed']}")
        logger.info(f"     Chargées: {self.stats['quotes']['loaded']}")
        logger.info("")
        logger.info("  Librairies:")
        logger.info(f"     Extraites: {self.stats['librairies']['extracted']}")
        logger.info(f"     Transformées: {self.stats['librairies']['transformed']}")
        logger.info(f"     Chargées: {self.stats['librairies']['loaded']}")

        if self.stats['errors']:
            logger.warning(f"\n  [ATTENTION] Erreurs: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:
                logger.warning(f"     - {error}")

        logger.info("=" * 70)

        return self.stats

    def run_extract_only(self, excel_path: str, **kwargs):
        """Exécute uniquement la phase d'extraction."""
        self.stats['start_time'] = datetime.utcnow()
        self.connect()

        books = self.extract_books(**kwargs)
        quotes = self.extract_quotes(**kwargs)
        librairies = self.extract_librairies(excel_path)

        self.disconnect()
        self.stats['end_time'] = datetime.utcnow()

        return {'books': books, 'quotes': quotes, 'librairies': librairies}

    def run_transform_only(self):
        """Exécute uniquement la phase de transformation (depuis Bronze)."""
        self.stats['start_time'] = datetime.utcnow()
        self.connect()

        # Récupérer depuis Bronze
        books = list(self.db_bronze['raw_books'].find())
        quotes = list(self.db_bronze['raw_quotes'].find())
        librairies = list(self.db_silver['clean_librairies'].find())

        transformed_books = self.transform_books(books)
        transformed_quotes = self.transform_quotes(quotes)
        transformed_librairies = self.transform_librairies(librairies)

        self.disconnect()
        self.stats['end_time'] = datetime.utcnow()

        return {
            'books': transformed_books,
            'quotes': transformed_quotes,
            'librairies': transformed_librairies
        }

    def run_load_only(self):
        """Exécute uniquement la phase de chargement (depuis Silver)."""
        self.stats['start_time'] = datetime.utcnow()
        self.connect()

        # Récupérer depuis Silver
        books = list(self.db_silver['clean_books'].find())
        quotes = list(self.db_silver['clean_quotes'].find())
        librairies = list(self.db_silver['clean_librairies'].find())

        self.load_books_to_gold(books)
        self.load_quotes_to_gold(quotes)
        self.load_librairies_to_gold(librairies)

        self.disconnect()
        self.stats['end_time'] = datetime.utcnow()

        return self.stats

    def run_analytics_queries(self):
        """
        Exécute les requêtes analytiques de démonstration.
        Lit les requêtes depuis sql/analyses.sql et les exécute.
        """
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 4: REQUETES ANALYTIQUES (Validation)")
        logger.info("=" * 70)

        # Connexion PostgreSQL
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        cursor = pg_conn.cursor()

        # Requête 1: Statistiques par catégorie
        logger.info("\n[QUERY 1] Statistiques par categorie de livres")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT
                c.category_name AS categorie,
                COUNT(b.book_id) AS nb_livres,
                ROUND(AVG(b.price_eur), 2) AS prix_moyen,
                ROUND(AVG(b.rating), 2) AS note_moyenne,
                SUM(b.availability) AS stock_total
            FROM dim_categories c
            LEFT JOIN fact_books b ON c.category_id = b.category_id
            GROUP BY c.category_id, c.category_name
            HAVING COUNT(b.book_id) > 0
            ORDER BY nb_livres DESC
        """)
        results = cursor.fetchall()
        for row in results:
            logger.info(f"  {row[0]}: {row[1]} livres, {row[2]} EUR, note {row[3]}")

        # Requête 2: Top auteurs
        logger.info("\n[QUERY 2] Top 5 auteurs les plus cites")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT
                a.author_name,
                a.quote_count
            FROM dim_authors a
            ORDER BY a.quote_count DESC
            LIMIT 5
        """)
        results = cursor.fetchall()
        for row in results:
            logger.info(f"  {row[0]}: {row[1]} citations")

        # Requête 3: Window function - Rang des livres par prix
        logger.info("\n[QUERY 3] Top 3 livres les plus chers par categorie (Window Function)")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT categorie, titre, prix, rang FROM (
                SELECT
                    c.category_name AS categorie,
                    b.title AS titre,
                    b.price_eur AS prix,
                    RANK() OVER (PARTITION BY c.category_id ORDER BY b.price_eur DESC) AS rang
                FROM fact_books b
                JOIN dim_categories c ON b.category_id = c.category_id
            ) ranked
            WHERE rang <= 3
            ORDER BY categorie, rang
        """)
        results = cursor.fetchall()
        current_cat = None
        for row in results:
            if row[0] != current_cat:
                current_cat = row[0]
                logger.info(f"  [{current_cat}]")
            logger.info(f"    #{row[3]}: {row[1][:40]}... - {row[2]} EUR")

        # Requête 4: Librairies géolocalisées
        logger.info("\n[QUERY 4] Librairies avec coordonnees GPS")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT
                nom,
                ville,
                specialite,
                ROUND(latitude::numeric, 4) AS lat,
                ROUND(longitude::numeric, 4) AS lon
            FROM dim_librairies
            WHERE latitude IS NOT NULL
        """)
        results = cursor.fetchall()
        for row in results:
            logger.info(f"  {row[0]} ({row[1]}): {row[3]}, {row[4]} - {row[2]}")

        # Requête 5: Rapport qualité des données
        logger.info("\n[QUERY 5] Rapport qualite des donnees")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT 'Livres sans categorie' AS controle, COUNT(*) AS nombre,
                   CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ATTENTION' END AS statut
            FROM fact_books WHERE category_id IS NULL
            UNION ALL
            SELECT 'Citations sans auteur', COUNT(*),
                   CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ATTENTION' END
            FROM fact_quotes WHERE author_id IS NULL
            UNION ALL
            SELECT 'Librairies sans GPS', COUNT(*),
                   CASE WHEN COUNT(*) < 3 THEN 'OK' ELSE 'ATTENTION' END
            FROM dim_librairies WHERE latitude IS NULL
        """)
        results = cursor.fetchall()
        for row in results:
            logger.info(f"  [{row[2]}] {row[0]}: {row[1]}")

        # Dashboard global
        logger.info("\n[DASHBOARD] Statistiques globales")
        logger.info("-" * 50)
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM fact_books) AS total_livres,
                (SELECT COUNT(*) FROM fact_quotes) AS total_citations,
                (SELECT COUNT(*) FROM dim_librairies) AS total_librairies,
                (SELECT COUNT(DISTINCT category_id) FROM fact_books) AS nb_categories,
                (SELECT COUNT(DISTINCT author_id) FROM fact_quotes) AS nb_auteurs
        """)
        row = cursor.fetchone()
        logger.info(f"  Livres: {row[0]}")
        logger.info(f"  Citations: {row[1]}")
        logger.info(f"  Librairies: {row[2]}")
        logger.info(f"  Categories: {row[3]}")
        logger.info(f"  Auteurs: {row[4]}")

        pg_conn.close()
        logger.info("\n[OK] Requetes analytiques executees avec succes")
        logger.info("=" * 70)


# =============================================================================
# Point d'entrée CLI
# =============================================================================

def main():
    """Point d'entrée principal avec arguments CLI."""
    parser = argparse.ArgumentParser(description='Pipeline ETL DataPulse Analytics')

    parser.add_argument(
        '--step',
        choices=['all', 'extract', 'transform', 'load'],
        default='all',
        help='Étape à exécuter (default: all)'
    )

    parser.add_argument(
        '--excel',
        default='data/partenaire_librairies.xlsx',
        help='Chemin vers le fichier Excel (default: data/partenaire_librairies.xlsx)'
    )

    parser.add_argument(
        '--limit-categories',
        type=int,
        default=None,
        help='Limiter le nombre de catégories de livres (pour tests)'
    )

    parser.add_argument(
        '--max-quote-pages',
        type=int,
        default=None,
        help='Limiter le nombre de pages de citations (pour tests)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Mode test : limite les données (2 catégories, 2 pages)'
    )

    args = parser.parse_args()

    # Mode test
    if args.test:
        args.limit_categories = 2
        args.max_quote_pages = 2
        logger.info("[TEST] Mode test active (donnees limitees)")

    # Créer le dossier de logs
    os.makedirs('logs', exist_ok=True)

    pipeline = ETLPipeline()

    if args.step == 'all':
        stats = pipeline.run_full_pipeline(
            excel_path=args.excel,
            limit_categories=args.limit_categories,
            max_quote_pages=args.max_quote_pages
        )
        # En mode test, exécuter aussi les requêtes analytiques
        if args.test:
            pipeline.run_analytics_queries()
    elif args.step == 'extract':
        result = pipeline.run_extract_only(
            excel_path=args.excel,
            limit_categories=args.limit_categories,
            max_pages=args.max_quote_pages
        )
        print(f"Extraction terminée: {len(result['books'])} livres, {len(result['quotes'])} citations, {len(result['librairies'])} librairies")
    elif args.step == 'transform':
        result = pipeline.run_transform_only()
        print(f"Transformation terminée")
    elif args.step == 'load':
        stats = pipeline.run_load_only()
        print(f"Chargement terminé")


if __name__ == "__main__":
    main()
