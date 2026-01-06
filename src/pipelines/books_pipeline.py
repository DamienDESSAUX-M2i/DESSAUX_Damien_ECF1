import json
import logging
from datetime import datetime, timezone

from src.extractors import Book, BooksScraper
from src.storage import MinIOStorage, PostgreSQLStorage

logger = logging.getLogger("app")


class BooksPipeline:
    def __init__(self, download_image: bool):
        self.download_image = download_image
        self.books_scraper = BooksScraper()
        self.postgresql_storage = PostgreSQLStorage()
        self.minio_storage = MinIOStorage()

    def run(self):
        try:
            logger.info("=" * 3)
            logger.info("BOOKS PIPELINE STARTED")
            logger.info("=" * 3)

            logger.info("[1/3] EXTRACTION")
            books = self._extract()

            logger.info("[2/3] TRANSFORM")
            books_transformed = self._transform(books)

            logger.info("[3/3] LOADING")
            self._load(books_transformed)

            logger.info("=" * 3)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 3)

        except Exception as e:
            logger.error(f"PIPELINE ERROR : {e}")

    def _extract(self) -> list[Book]:
        books = []

        for book in self.books_scraper.scrape_books():
            if self.download_image:
                book = self.books_scraper.download_image(book)
            books.append(book)

        logger.info(f"Number of books scrapped: {len(books)}")

        self.books_scraper.close()

        return books

    def _transform(self, books: list[Book]) -> list[Book]:
        # Conversion £ en € (1£ = 1.15€)
        for book in books:
            book.price = 1.15 * book.price

        return books

    def _load(self, books: list[Book]) -> None:
        # Export JSON
        self.minio_storage.upload_json(
            data=json.dumps([book.to_dict() for book in books]),
            filename=f"quotes_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json",
        )

        nb_books_inserted = 0
        nb_image_uploaded = 0

        for book in books:
            # upload image
            if self.download_image:
                try:
                    self.minio_storage.upload_image(
                        image_data=book.image,
                        filename=f"{book.title.replace(' ', '_').replace('/', '_').lower()}.jpeg",
                    )
                    nb_image_uploaded += 1
                except Exception as e:
                    logger.error(f"image_insertion_failed: {e}")

            # uplod book
            result = self.postgresql_storage.insert_into_books(
                title=book.title,
                price=book.price,
                rating=book.rating,
                availability=book.availability,
                category=book.category,
                url_image=book.url_image,
            )

            if result:
                nb_books_inserted += 1

        logger.info(f"Number of images uploaded: {nb_image_uploaded}")
        logger.info(f"Number of books inserted: {nb_books_inserted}")

        self.postgresql_storage.close()
