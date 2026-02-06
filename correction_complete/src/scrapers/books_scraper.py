#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper pour Books to Scrape (https://books.toscrape.com)
ECF DataPulse Analytics - Collecte de données

Ce scraper respecte les bonnes pratiques :
- Délai de politesse entre les requêtes (1 seconde)
- User-Agent identifiable
- Gestion des erreurs HTTP
- Pagination complète
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime
from typing import Optional
import re
import uuid

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BooksScraper')

# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://books.toscrape.com"
DELAY_BETWEEN_REQUESTS = 1.0  # Délai de politesse en secondes
MAX_RETRIES = 3
TIMEOUT = 30

HEADERS = {
    'User-Agent': 'DataPulse-ECF-Bot/1.0 (Educational Project; contact@datapulse.example.com)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
}

# Mapping des notes textuelles vers numériques
RATING_MAP = {
    'one': 1,
    'two': 2,
    'three': 3,
    'four': 4,
    'five': 5
}


# =============================================================================
# Classe principale du scraper
# =============================================================================

class BooksScraper:
    """
    Scraper pour le site Books to Scrape.
    Extrait : titre, prix, note, disponibilité, catégorie, URL
    """

    def __init__(self, delay: float = DELAY_BETWEEN_REQUESTS):
        """
        Initialise le scraper.

        Args:
            delay: Délai entre les requêtes (secondes)
        """
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = delay
        self.batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.stats = {
            'pages_scraped': 0,
            'books_scraped': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        logger.info(f"Scraper initialisé - Batch ID: {self.batch_id}")

    def _make_request(self, url: str) -> Optional[requests.Response]:
        """
        Effectue une requête HTTP avec gestion des erreurs et retry.

        Args:
            url: URL à requêter

        Returns:
            Response object ou None si échec
        """
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()

                # Délai de politesse
                time.sleep(self.delay)

                return response

            except requests.exceptions.HTTPError as e:
                logger.warning(f"Erreur HTTP {e.response.status_code} pour {url} (tentative {attempt + 1}/{MAX_RETRIES})")
                if e.response.status_code == 404:
                    return None  # Page non trouvée, pas de retry
                time.sleep(self.delay * (attempt + 1))  # Backoff exponentiel

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur de requête pour {url}: {e} (tentative {attempt + 1}/{MAX_RETRIES})")
                time.sleep(self.delay * (attempt + 1))

        self.stats['errors'] += 1
        return None

    def _parse_rating(self, rating_class: str) -> int:
        """
        Convertit la classe CSS de rating en valeur numérique.

        Args:
            rating_class: Classe CSS (ex: "star-rating Three")

        Returns:
            Note de 1 à 5
        """
        for word, value in RATING_MAP.items():
            if word in rating_class.lower():
                return value
        return 0

    def _parse_availability(self, availability_text: str) -> int:
        """
        Extrait le nombre d'exemplaires disponibles.

        Args:
            availability_text: Texte de disponibilité (ex: "In stock (22 available)")

        Returns:
            Nombre d'exemplaires disponibles
        """
        match = re.search(r'\((\d+) available\)', availability_text)
        if match:
            return int(match.group(1))
        elif 'in stock' in availability_text.lower():
            return 1
        return 0

    def _parse_price(self, price_text: str) -> str:
        """
        Nettoie le prix (garde le format original avec £).

        Args:
            price_text: Texte du prix (ex: "£51.77")

        Returns:
            Prix nettoyé
        """
        return price_text.strip().replace('Â', '')

    def get_categories(self) -> list:
        """
        Récupère la liste de toutes les catégories.

        Returns:
            Liste de dictionnaires {name, url, slug}
        """
        logger.info("Récupération des catégories...")
        response = self._make_request(BASE_URL)

        if not response:
            logger.error("Impossible de récupérer la page d'accueil")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')

        # Les catégories sont dans le menu latéral
        category_list = soup.select('div.side_categories ul.nav-list > li > ul > li > a')

        categories = []
        for link in category_list:
            name = link.text.strip()
            href = link.get('href', '')
            slug = href.split('/')[-2] if '/' in href else name.lower().replace(' ', '-')

            categories.append({
                'name': name,
                'url': f"{BASE_URL}/{href}",
                'slug': slug
            })

        logger.info(f"Trouvé {len(categories)} catégories")
        return categories

    def scrape_book_list_page(self, url: str, category: str) -> tuple:
        """
        Scrape une page de liste de livres.

        Args:
            url: URL de la page
            category: Nom de la catégorie

        Returns:
            Tuple (liste de livres, URL page suivante ou None)
        """
        response = self._make_request(url)

        if not response:
            return [], None

        soup = BeautifulSoup(response.text, 'html.parser')
        books = []

        # Chaque livre est dans un article
        for article in soup.select('article.product_pod'):
            try:
                # Titre et URL
                title_link = article.select_one('h3 > a')
                title = title_link.get('title', title_link.text.strip())
                book_url = title_link.get('href', '')

                # Construire l'URL complète
                if book_url.startswith('../'):
                    book_url = f"{BASE_URL}/catalogue/{book_url.replace('../', '')}"
                elif not book_url.startswith('http'):
                    book_url = f"{BASE_URL}/catalogue/{book_url}"

                # Prix
                price_elem = article.select_one('p.price_color')
                price = self._parse_price(price_elem.text) if price_elem else "N/A"

                # Note (étoiles)
                rating_elem = article.select_one('p.star-rating')
                rating_class = ' '.join(rating_elem.get('class', [])) if rating_elem else ''
                rating = self._parse_rating(rating_class)

                # Disponibilité
                availability_elem = article.select_one('p.availability')
                availability_text = availability_elem.text.strip() if availability_elem else ''
                availability = self._parse_availability(availability_text)

                # Image
                image_elem = article.select_one('img.thumbnail')
                image_url = image_elem.get('src', '') if image_elem else ''
                if image_url and not image_url.startswith('http'):
                    image_url = f"{BASE_URL}/{image_url.replace('../', '')}"

                book = {
                    'title': title,
                    'price': price,
                    'rating': str(rating),
                    'availability': str(availability),
                    'category': category,
                    'url': book_url,
                    'image_url': image_url,
                    '_metadata': {
                        'source': 'books.toscrape.com',
                        'scraped_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'pipeline_version': '1.0'
                    }
                }

                books.append(book)

            except Exception as e:
                logger.warning(f"Erreur parsing livre: {e}")
                self.stats['errors'] += 1
                continue

        # Chercher la page suivante
        next_link = soup.select_one('li.next > a')
        next_url = None

        if next_link:
            next_href = next_link.get('href', '')
            # Construire l'URL de la page suivante
            base_path = '/'.join(url.rsplit('/', 1)[0].split('/'))
            next_url = f"{base_path}/{next_href}"

        self.stats['pages_scraped'] += 1
        self.stats['books_scraped'] += len(books)

        return books, next_url

    def scrape_category(self, category: dict) -> list:
        """
        Scrape tous les livres d'une catégorie.

        Args:
            category: Dictionnaire de catégorie {name, url, slug}

        Returns:
            Liste de tous les livres de la catégorie
        """
        logger.info(f"Scraping catégorie: {category['name']}")
        all_books = []
        current_url = category['url']
        page_num = 1

        while current_url:
            logger.debug(f"  Page {page_num}...")
            books, next_url = self.scrape_book_list_page(current_url, category['name'])
            all_books.extend(books)
            current_url = next_url
            page_num += 1

        logger.info(f"  → {len(all_books)} livres trouvés dans {category['name']}")
        return all_books

    def scrape_all(self, limit_categories: int = None) -> list:
        """
        Scrape tous les livres de toutes les catégories.

        Args:
            limit_categories: Nombre max de catégories (pour tests)

        Returns:
            Liste de tous les livres
        """
        self.stats['start_time'] = datetime.utcnow()
        logger.info("=" * 60)
        logger.info("Démarrage du scraping complet de Books to Scrape")
        logger.info("=" * 60)

        categories = self.get_categories()

        if limit_categories:
            categories = categories[:limit_categories]
            logger.info(f"Limitation à {limit_categories} catégories (mode test)")

        all_books = []

        for i, category in enumerate(categories, 1):
            logger.info(f"[{i}/{len(categories)}] {category['name']}")
            books = self.scrape_category(category)
            all_books.extend(books)

        self.stats['end_time'] = datetime.utcnow()

        # Résumé
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        logger.info("=" * 60)
        logger.info("SCRAPING TERMINÉ")
        logger.info(f"  • Livres scrapés: {self.stats['books_scraped']}")
        logger.info(f"  • Pages parcourues: {self.stats['pages_scraped']}")
        logger.info(f"  • Erreurs: {self.stats['errors']}")
        logger.info(f"  • Durée: {duration:.1f} secondes")
        logger.info(f"  • Batch ID: {self.batch_id}")
        logger.info("=" * 60)

        return all_books

    def get_stats(self) -> dict:
        """Retourne les statistiques du scraping."""
        return self.stats.copy()


# =============================================================================
# Point d'entrée
# =============================================================================

def main():
    """Fonction principale pour exécution directe."""
    import json

    scraper = BooksScraper()

    # Scraping complet (ou limité pour tests)
    # books = scraper.scrape_all(limit_categories=2)  # Mode test
    books = scraper.scrape_all()  # Mode complet

    # Sauvegarde locale (pour debug/vérification)
    output_file = f"books_{scraper.batch_id}.json"

    # Convertir les dates en string pour JSON
    for book in books:
        book['_metadata']['scraped_at'] = book['_metadata']['scraped_at'].isoformat()

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(books, f, ensure_ascii=False, indent=2)

    logger.info(f"Données sauvegardées dans {output_file}")

    return books


if __name__ == "__main__":
    main()
