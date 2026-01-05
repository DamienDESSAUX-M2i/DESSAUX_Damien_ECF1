import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from config import api_adress_config

logger = logging.getLogger("app")


class APIAdressExtractor:
    """Loader API REST."""

    def __init__(self):
        self.base_url = api_adress_config.base_url
        self.endpoint = api_adress_config.endpoint
        self.timeout = api_adress_config.timeout
        self.retry_strategy = Retry(
            total=api_adress_config.max_retries, backoff_factor=api_adress_config.delay
        )

    def get(self, params: dict[str, Any] = {}) -> requests.Response:
        try:
            logger.info(f"Attempting request {self.base_url}/{self.endpoint}.")

            headers = {}
            with requests.Session() as session:
                adapter = HTTPAdapter(max_retries=self.retry_strategy)
                session.mount("http://", adapter)
                session.mount("https://", adapter)

                response = session.get(
                    url=f"{self.base_url}/{self.endpoint}",
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )

            response.raise_for_status()
            data = response.json()

            logger.info(f"Extraction completed : {len(data)} entries.")
            return data
        except Exception as e:
            logger.error(f"API Extractor Error : {e}")
            raise


if __name__ == "__main__":
    # Documentation : https://data.geopf.fr/geocodage/openapi
    print("Test de l'extracteur API Adress...")

    api_adress_extractor = APIAdressExtractor()

    params = {"q": "20 avenue de Segur Paris", "limit": 1}
    data = api_adress_extractor.get(params=params)

    print(data)
