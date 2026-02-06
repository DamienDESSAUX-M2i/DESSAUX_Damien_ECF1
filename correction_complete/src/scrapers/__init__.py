"""
Scrapers pour la collecte de données
ECF DataPulse Analytics
"""

from .books_scraper import BooksScraper
from .quotes_scraper import QuotesScraper

__all__ = ['BooksScraper', 'QuotesScraper']
