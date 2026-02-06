#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper pour Quotes to Scrape (https://quotes.toscrape.com)
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
from typing import Optional, List, Dict
import uuid

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('QuotesScraper')

# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://quotes.toscrape.com"
DELAY_BETWEEN_REQUESTS = 1.0  # Délai de politesse en secondes
MAX_RETRIES = 3
TIMEOUT = 30

HEADERS = {
    'User-Agent': 'DataPulse-ECF-Bot/1.0 (Educational Project; contact@datapulse.example.com)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
}


# =============================================================================
# Classe principale du scraper
# =============================================================================

class QuotesScraper:
    """
    Scraper pour le site Quotes to Scrape.
    Extrait : texte de la citation, auteur, tags
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
            'quotes_scraped': 0,
            'unique_authors': set(),
            'unique_tags': set(),
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
                    return None
                time.sleep(self.delay * (attempt + 1))

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur de requête pour {url}: {e} (tentative {attempt + 1}/{MAX_RETRIES})")
                time.sleep(self.delay * (attempt + 1))

        self.stats['errors'] += 1
        return None

    def _clean_text(self, text: str) -> str:
        """
        Nettoie le texte d'une citation.

        Args:
            text: Texte brut

        Returns:
            Texte nettoyé
        """
        # Supprimer les guillemets décoratifs
        text = text.strip()
        if text.startswith('"') or text.startswith('"') or text.startswith('«'):
            text = text[1:]
        if text.endswith('"') or text.endswith('"') or text.endswith('»'):
            text = text[:-1]
        return text.strip()

    def scrape_page(self, url: str) -> tuple:
        """
        Scrape une page de citations.

        Args:
            url: URL de la page

        Returns:
            Tuple (liste de citations, URL page suivante ou None)
        """
        response = self._make_request(url)

        if not response:
            return [], None

        soup = BeautifulSoup(response.text, 'html.parser')
        quotes = []

        # Chaque citation est dans une div.quote
        for quote_div in soup.select('div.quote'):
            try:
                # Texte de la citation
                text_elem = quote_div.select_one('span.text')
                text = self._clean_text(text_elem.text) if text_elem else ''

                # Auteur
                author_elem = quote_div.select_one('small.author')
                author = author_elem.text.strip() if author_elem else 'Unknown'

                # Lien vers la page auteur (pour enrichissement futur)
                author_link = quote_div.select_one('a[href*="/author/"]')
                author_url = f"{BASE_URL}{author_link.get('href', '')}" if author_link else None

                # Tags
                tags = []
                for tag_elem in quote_div.select('a.tag'):
                    tag = tag_elem.text.strip()
                    tags.append(tag)
                    self.stats['unique_tags'].add(tag)

                # Mise à jour des statistiques
                self.stats['unique_authors'].add(author)

                quote = {
                    'text': text,
                    'author': author,
                    'author_url': author_url,
                    'tags': tags,
                    '_metadata': {
                        'source': 'quotes.toscrape.com',
                        'scraped_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'pipeline_version': '1.0'
                    }
                }

                quotes.append(quote)

            except Exception as e:
                logger.warning(f"Erreur parsing citation: {e}")
                self.stats['errors'] += 1
                continue

        # Chercher la page suivante
        next_link = soup.select_one('li.next > a')
        next_url = None

        if next_link:
            next_href = next_link.get('href', '')
            next_url = f"{BASE_URL}{next_href}"

        self.stats['pages_scraped'] += 1
        self.stats['quotes_scraped'] += len(quotes)

        return quotes, next_url

    def scrape_all(self, max_pages: int = None) -> List[Dict]:
        """
        Scrape toutes les citations du site.

        Args:
            max_pages: Nombre max de pages (pour tests)

        Returns:
            Liste de toutes les citations
        """
        self.stats['start_time'] = datetime.utcnow()
        logger.info("=" * 60)
        logger.info("Démarrage du scraping de Quotes to Scrape")
        logger.info("=" * 60)

        all_quotes = []
        current_url = BASE_URL
        page_num = 1

        while current_url:
            if max_pages and page_num > max_pages:
                logger.info(f"Arrêt à la page {max_pages} (mode test)")
                break

            logger.info(f"Scraping page {page_num}...")
            quotes, next_url = self.scrape_page(current_url)
            all_quotes.extend(quotes)

            logger.info(f"  → {len(quotes)} citations extraites")

            current_url = next_url
            page_num += 1

        self.stats['end_time'] = datetime.utcnow()

        # Résumé
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        logger.info("=" * 60)
        logger.info("SCRAPING TERMINÉ")
        logger.info(f"  • Citations scrapées: {self.stats['quotes_scraped']}")
        logger.info(f"  • Pages parcourues: {self.stats['pages_scraped']}")
        logger.info(f"  • Auteurs uniques: {len(self.stats['unique_authors'])}")
        logger.info(f"  • Tags uniques: {len(self.stats['unique_tags'])}")
        logger.info(f"  • Erreurs: {self.stats['errors']}")
        logger.info(f"  • Durée: {duration:.1f} secondes")
        logger.info(f"  • Batch ID: {self.batch_id}")
        logger.info("=" * 60)

        return all_quotes

    def scrape_by_tag(self, tag: str) -> List[Dict]:
        """
        Scrape les citations d'un tag spécifique.

        Args:
            tag: Nom du tag

        Returns:
            Liste des citations avec ce tag
        """
        logger.info(f"Scraping citations avec le tag: {tag}")

        all_quotes = []
        current_url = f"{BASE_URL}/tag/{tag}/"
        page_num = 1

        while current_url:
            quotes, next_url = self.scrape_page(current_url)
            all_quotes.extend(quotes)
            current_url = next_url
            page_num += 1

        logger.info(f"  → {len(all_quotes)} citations trouvées pour le tag '{tag}'")
        return all_quotes

    def get_all_tags(self) -> List[str]:
        """
        Récupère la liste de tous les tags disponibles.

        Returns:
            Liste des noms de tags
        """
        logger.info("Récupération de la liste des tags...")
        response = self._make_request(BASE_URL)

        if not response:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')

        # Les tags populaires sont dans la sidebar
        tags = []
        for tag_elem in soup.select('div.tags-box a.tag'):
            tags.append(tag_elem.text.strip())

        logger.info(f"  → {len(tags)} tags trouvés")
        return tags

    def get_stats(self) -> dict:
        """Retourne les statistiques du scraping."""
        stats = self.stats.copy()
        stats['unique_authors'] = list(stats['unique_authors'])
        stats['unique_tags'] = list(stats['unique_tags'])
        return stats


# =============================================================================
# Point d'entrée
# =============================================================================

def main():
    """Fonction principale pour exécution directe."""
    import json

    scraper = QuotesScraper()

    # Scraping complet
    quotes = scraper.scrape_all()

    # Sauvegarde locale
    output_file = f"quotes_{scraper.batch_id}.json"

    # Convertir les dates en string pour JSON
    for quote in quotes:
        quote['_metadata']['scraped_at'] = quote['_metadata']['scraped_at'].isoformat()

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(quotes, f, ensure_ascii=False, indent=2)

    logger.info(f"Données sauvegardées dans {output_file}")

    # Afficher quelques statistiques
    stats = scraper.get_stats()
    print(f"\nTop 5 tags: {stats['unique_tags'][:5]}")
    print(f"Auteurs: {stats['unique_authors'][:5]}...")

    return quotes


if __name__ == "__main__":
    main()
