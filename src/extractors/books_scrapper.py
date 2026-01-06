import logging
import re
import time
from dataclasses import dataclass
from typing import Generator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import books_scraper_config

logger = logging.getLogger("app")

MAPPING_RATING = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


@dataclass
class Book:
    title: str
    price: float
    rating: int
    availability: bool
    category: str
    url_image: str
    image: bytes

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "price": self.price,
            "rating": self.rating,
            "availability": self.availability,
            "category": self.category,
            "url_image": self.url_image,
        }


class BooksScraper:
    def __init__(self):
        """Initialise le scraper avec une session HTTP configurée."""
        self.base_url = books_scraper_config.base_url
        self.delay = books_scraper_config.delay
        self.session = requests.Session()
        self.ua = UserAgent()
        self._setup_session()

    def _setup_session(self) -> None:
        self.session.headers.update({"User-Agent": self.ua.random})

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch(self, url: str) -> BeautifulSoup | None:
        try:
            logger.debug(f"fetching_url: {url}")

            response = self.session.get(url, timeout=books_scraper_config.timeout)
            response.raise_for_status()

            # Délai de politesse
            time.sleep(self.delay)

            return BeautifulSoup(response.content, "lxml")

        except requests.RequestException as e:
            logger.error(f"fetch_failed: {e}")
            raise

    def scrape_books(self, max_pages: int = None) -> Generator[Book, None, None]:
        max_pages = max_pages or books_scraper_config.max_pages
        page = 1

        categories = self.scrape_categories()
        for category in categories:
            current_url = category["category_url"]

            while current_url and page <= max_pages:
                logger.info(f"scraping_page: {current_url}")

                try:
                    soup = self._fetch(current_url)
                    if not soup:
                        break

                    # Parser les quotes de la page
                    books = soup.find_all("article", class_="product_pod")

                    if not books:
                        logger.warning(f"no_books_found: {current_url}")
                        break

                    for book_elem in books:
                        book = self._parse_book(
                            book_elem, category=category["category_name"]
                        )
                        if book:
                            yield book

                    # Trouver la page suivante
                    current_url = self._get_next_page(soup, current_url)
                    page += 1

                except Exception as e:
                    logger.error(f"book_scraping_failed: {e}")
                    break

    def scrape_categories(self):
        try:
            categories: list[dict] = []
            soup = self._fetch(books_scraper_config.base_url)
            ul_categories = soup.find("ul", class_="nav").find("ul")
            li_categories = ul_categories.find_all("li")
            for li_category in li_categories:
                a_category = li_category.find("a")
                categories.append(
                    {
                        "category_name": a_category.text,
                        "category_url": urljoin(
                            books_scraper_config.base_url, a_category.get("href")
                        ),
                    }
                )
            logger.info(f"scrape_categories: {len(categories)} categories scraped")
            return categories
        except Exception as e:
            logger.error(f"scrape_categories_failed: {e}")

    def _parse_price(self, text: str) -> float:
        """
        Extrait le prix d'une chaîne de texte.

        Gère les formats : $999.99, $1,299.00, etc.

        Args:
            text: Texte contenant le prix

        Returns:
            Prix en float ou 0.0 si non trouvé
        """
        # Supprimer les virgules et extraire les chiffres
        match = re.search(r"[\d.]+", text)
        return float(match.group()) if match else 0.0

    def _parse_book(self, element: Tag, category: str) -> Book | None:
        try:
            title = element.find("h3").find("a").get("title")
            price = self._parse_price(
                element.find("div", class_="product_price").find("p").text
            )
            rating = MAPPING_RATING[
                element.find("p", class_="star-rating").get("class")[1]
            ]
            availability = "in stock" in element.find("p", class_="availability").text
            category = category.strip()
            url_image = element.find("img").get("src")
            return Book(
                title=title,
                price=price,
                rating=rating,
                availability=availability,
                category=category,
                url_image=urljoin(books_scraper_config.base_url, url_image),
                image=None,
            )

        except Exception as e:
            logger.error(f"product_parse_failed: {e}")
            return None

    def _get_next_page(self, soup: BeautifulSoup, base_url: str) -> str | None:
        next_link = soup.find(class_="next")

        if next_link:
            href = next_link.find("a")["href"]
            return urljoin(base_url, href)

        return None

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5)
    )
    def _fetch_image(self, url: str) -> bytes | None:
        try:
            response = self.session.get(url, timeout=books_scraper_config.timeout)
            response.raise_for_status()
            time.sleep(self.delay / 2)  # Délai réduit pour les images
            return response.content
        except requests.RequestException as e:
            logger.error(f"image_fetch_failed: {e}")
            return None

    def download_image(self, book: Book) -> Book:
        """
        Télécharge l'image d'un produit.

        Args:
            product: Produit avec image_url

        Returns:
            Produit avec image_data rempli
        """
        if book.url_image:
            book.image = self._fetch_image(book.url_image)
            if book.image:
                logger.debug(f"image_downloaded: {book.url_image}")
        return book

    def close(self) -> None:
        """Ferme la session HTTP."""
        self.session.close()
        logger.info("scraper_session_closed")


# Test du module
if __name__ == "__main__":
    print("Test du scraper quotes...")

    books_scraper = BooksScraper()

    # Test
    books = []
    for book in books_scraper.scrape_books():
        books.append(book)
        print(
            f"{book.title} - {book.price} - {book.rating} - {book.availability} - {book.category} - {book.url_image}"
        )

    print(f"\nTotal: {len(books)} quotes scrapés")

    books_scraper.close()
    print("Test terminé!")
