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


# @dataclass
# class MongoDBConfig:
#     host: str = os.getenv("MONGO_HOST", "localhost")
#     port: int = int(os.getenv("MONGO_PORT", "27017"))
#     username: str = os.getenv("MONGO_USER", "admin")
#     password: str = os.getenv("MONGO_PASSWORD", "admin123")
#     database: str = os.getenv("MONGO_DB", "scraping_db")

#     @property
#     def connection_string(self) -> str:
#         return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"


@dataclass
class QuotesScraperConfig:
    base_url: str = "https://quotes.toscrape.com/"
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
# mongo_config = MongoDBConfig()
quotes_scraper_config = QuotesScraperConfig()
api_adress_config = APIAdressConfig()
