import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT")
    access_key: str = os.getenv("MINIO_ROOT_USER")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD")
    secure: bool = False
    bucket_images: str = os.getenv("BUCKET_IMAGES", "images")
    bucket_exports: str = os.getenv("BUCKET_EXPORTS", "exports")
    bucket_backups: str = os.getenv("BUCKET_BACKUPS", "backups")


@dataclass
class PostgreSQLConfig:
    host: str = "localhost"
    port: int = 5432
    user: str = os.getenv("POSTGRES_USER")
    password: str = os.getenv("POSTGRES_PASSWORD")
    dbname: str = os.getenv("POSTGRES_DB")

    @property
    def dsn(self) -> str:
        return f"dbname={self.dbname} user={self.user} password={self.password} host={self.host} port={self.port}"

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


@dataclass
class QuotesScraperConfig:
    base_url: str = "https://quotes.toscrape.com/"
    delay: float = 1.0
    timeout: int = 30
    max_retries: int = 3
    max_pages: int = 10


@dataclass
class BooksScraperConfig:
    base_url: str = "https://books.toscrape.com/"
    delay: float = 1.0
    timeout: int = 30
    max_retries: int = 3
    max_pages: int = 20


@dataclass
class APIAdressConfig:
    base_url: str = "https://api-adresse.data.gouv.fr/"
    endpoint: str = "search"
    delay: float = 1.0
    timeout: int = 30
    max_retries: int = 3


minio_config = MinIOConfig()
postgresql_config = PostgreSQLConfig()
quotes_scraper_config = QuotesScraperConfig()
books_scraper_config = BooksScraperConfig()
api_adress_config = APIAdressConfig()
