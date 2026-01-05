import logging

from config import quotes_scraper_config
from src.extractors import Quote, QuotesScraper
from src.storage import MinIOStorage, PostgreSQLStorage

logger = logging.getLogger("app")


class QuotesPipeline:
    def __init__(self):
        self.quotes_scraper = QuotesScraper()
        self.postgresql_storage = PostgreSQLStorage()
        self.minio = MinIOStorage()

    def run(self):
        try:
            logger.info("=" * 3)
            logger.info("QUOTES PIPELINE STARTED")
            logger.info("=" * 3)

            logger.info("[1/3] EXTRACTION")
            quotes = self._extract()

            logger.info("[2/3] TRANSFORMATION")
            dict_authors, list_quotes, dict_tags, list_quotes_tags = self._transform(
                quotes
            )

            logger.info("[3/3] LOADING")
            self._load(dict_authors, list_quotes, dict_tags, list_quotes_tags)

            logger.info("=" * 3)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 3)

        except Exception as e:
            logger.error(f"PIPELINE ERROR : {e}")

    def _extract(self) -> list[Quote]:
        quotes = []

        for quote in self.quotes_scraper.scrape_quotes(
            url=quotes_scraper_config.base_url,
            max_pages=20,
        ):
            quotes.append(quote)

        logger.info(f"Total: {len(quotes)} quotes scrapés")

        self.quotes_scraper.close()

        return quotes

    def _transform(self, quotes: list[Quote]) -> list[dict]:
        dict_authors = {}
        list_quotes = []
        dict_tags = {}
        list_quotes_tags = []

        for quote in quotes:
            # authors
            if quote.author not in dict_authors:
                id_author = len(dict_authors) + 1
                dict_authors[quote.author] = id_author
            else:
                id_author = dict_authors[quote.author]

            # quotes
            id_quote = len(list_quotes) + 1
            list_quotes.append(
                {"id_quote": id_quote, "text": quote.text, "id_author": id_author}
            )

            # tags
            for tag in quote.tags:
                if tag not in dict_tags.keys():
                    id_tag = len(dict_tags) + 1
                    dict_authors[tag] = id_tag
                else:
                    id_tag = dict_tags[tag]

                # quotes_tags
                list_quotes_tags.append({"id_quote": id_quote, "id_tag": id_tag})

        return dict_authors, list_quotes, dict_tags, list_quotes_tags

    def _load(
        self,
        dict_authors: dict,
        list_quotes: list,
        dict_tags: dict,
        list_quotes_tags: list,
    ) -> None:
        # authors
        for author_name, id_author in dict_authors.items():
            self.postgresql_storage.insert_into_authors(
                id_author=id_author, author_name=author_name
            )

        # quotes
        for quote in list_quotes:
            self.postgresql_storage.insert_into_quotes(
                id_quote=quote["id_quote"],
                text=quote["text"],
                id_author=quote["id_author"],
            )

        # tags
        for tag_name, id_tag in dict_tags.items():
            self.postgresql_storage.insert_into_tags(id_tag=id_tag, tag_name=tag_name)

        # quotes_tags
        for quote_tag in list_quotes_tags:
            self.postgresql_storage.insert_into_quotes_tags(
                id_quote=quote_tag["id_quote"], id_tag=quote_tag["id_tag"]
            )

        self.postgresql_storage.close()
