import logging
import time
from dataclasses import dataclass, field
from typing import Generator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import quotes_scraper_config

logger = logging.getLogger("app")


@dataclass
class Quote:
    author: str
    text: str
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "author": self.author,
            "text": self.text,
            "tags": self.tags,
        }


class QuotesScraper:
    def __init__(self):
        """Initialise le scraper avec une session HTTP configurée."""
        self.base_url = quotes_scraper_config.base_url
        self.delay = quotes_scraper_config.delay
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

            response = self.session.get(url, timeout=quotes_scraper_config.timeout)
            response.raise_for_status()

            # Délai de politesse
            time.sleep(self.delay)

            return BeautifulSoup(response.content, "lxml")

        except requests.RequestException as e:
            logger.error(f"fetch_failed: {e}")
            raise

    def scrape_quotes(
        self, url: str, max_pages: int = None
    ) -> Generator[Quote, None, None]:
        max_pages = max_pages or quotes_scraper_config.max_pages
        page = 1
        current_url = url

        while current_url and page <= max_pages:
            logger.info(f"scraping_page: {current_url}")

            try:
                soup = self._fetch(current_url)
                if not soup:
                    break

                # Parser les quotes de la page
                quotes = soup.find_all("div", class_="quote")

                if not quotes:
                    logger.warning(f"no_quotess_found: {current_url}")
                    break

                for quote_elem in quotes:
                    quote = self._parse_quote(quote_elem)
                    if quote:
                        yield quote

                # Trouver la page suivante
                current_url = self._get_next_page(soup, url)
                page += 1

            except Exception as e:
                logger.error(f"quote_scraping_failed: {e}")
                break

    def _parse_quote(self, element: Tag) -> Quote | None:
        try:
            text = element.find(class_="text").text
            author = element.find(class_="author").text
            tags = [tag.text for tag in element.find_all(class_="tag")]
            return Quote(author=author, text=text, tags=tags)

        except Exception as e:
            logger.error(f"product_parse_failed: {e}")
            return None

    def _get_next_page(self, soup: BeautifulSoup, base_url: str) -> str | None:
        next_link = soup.find(class_="next")

        if next_link:
            href = next_link.find("a")["href"]
            return urljoin(base_url, href)

        return None

    def close(self) -> None:
        """Ferme la session HTTP."""
        self.session.close()
        logger.info("scraper_session_closed")


# Test du module
if __name__ == "__main__":
    print("Test du scraper quotes...")

    quotes_scraper = QuotesScraper()

    # Test sur 2 pages
    quotes = []
    for quote in quotes_scraper.scrape_quotes(
        url=quotes_scraper_config.base_url,
        max_pages=2,
    ):
        quotes.append(quote)
        print(f"{quote.text} - {quote.author} - {quote.tags}")

    print(f"\nTotal: {len(quotes)} quotes scrapés")

    quotes_scraper.close()
    print("Test terminé!")
