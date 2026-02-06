#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Client API pour le géocodage des adresses
Utilise l'API Adresse du gouvernement français (api-adresse.data.gouv.fr)
ECF DataPulse Analytics

Fonctionnalités :
- Géocodage d'adresses en coordonnées GPS
- Respect du rate limit (50 req/s)
- Gestion des erreurs et adresses non trouvées
- Cache des résultats pour éviter les requêtes redondantes
"""

import requests
import time
import logging
from datetime import datetime
from typing import Optional, Dict, List
from urllib.parse import quote
import uuid

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('GeocodingClient')

# =============================================================================
# Configuration
# =============================================================================

API_BASE_URL = "https://api-adresse.data.gouv.fr/search/"
RATE_LIMIT_DELAY = 0.02  # 50 req/s max → 20ms minimum entre requêtes
MAX_RETRIES = 3
TIMEOUT = 10

HEADERS = {
    'User-Agent': 'DataPulse-ECF-Bot/1.0 (Educational Project)',
    'Accept': 'application/json',
}


# =============================================================================
# Classe principale du client
# =============================================================================

class GeocodingClient:
    """
    Client pour l'API Adresse (géocodage).
    Convertit des adresses postales en coordonnées GPS.
    """

    def __init__(self, delay: float = RATE_LIMIT_DELAY):
        """
        Initialise le client.

        Args:
            delay: Délai minimum entre les requêtes (secondes)
        """
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = delay
        self.batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        # Cache pour éviter les requêtes redondantes
        self._cache: Dict[str, Dict] = {}

        self.stats = {
            'requests_made': 0,
            'cache_hits': 0,
            'successful': 0,
            'not_found': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

        logger.info(f"Client géocodage initialisé - Batch ID: {self.batch_id}")

    def _make_request(self, url: str) -> Optional[Dict]:
        """
        Effectue une requête HTTP avec gestion des erreurs.

        Args:
            url: URL complète à requêter

        Returns:
            Réponse JSON ou None si échec
        """
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()

                # Respect du rate limit
                time.sleep(self.delay)

                self.stats['requests_made'] += 1
                return response.json()

            except requests.exceptions.HTTPError as e:
                logger.warning(f"Erreur HTTP {e.response.status_code} (tentative {attempt + 1}/{MAX_RETRIES})")
                time.sleep(self.delay * (attempt + 1))

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur de requête: {e} (tentative {attempt + 1}/{MAX_RETRIES})")
                time.sleep(self.delay * (attempt + 1))

            except ValueError as e:
                logger.error(f"Erreur parsing JSON: {e}")
                break

        self.stats['errors'] += 1
        return None

    def _build_cache_key(self, address: str, city: str = None, postcode: str = None) -> str:
        """Construit une clé de cache normalisée."""
        parts = [address.lower().strip()]
        if city:
            parts.append(city.lower().strip())
        if postcode:
            parts.append(postcode.strip())
        return '|'.join(parts)

    def geocode(
        self,
        address: str,
        city: str = None,
        postcode: str = None,
        limit: int = 1
    ) -> Optional[Dict]:
        """
        Géocode une adresse.

        Args:
            address: Adresse postale (ex: "15 rue des Francs-Bourgeois")
            city: Ville (optionnel, améliore la précision)
            postcode: Code postal (optionnel, améliore la précision)
            limit: Nombre max de résultats

        Returns:
            Dictionnaire avec les coordonnées ou None si non trouvé
            {
                'latitude': float,
                'longitude': float,
                'label': str,
                'score': float,
                'city': str,
                'postcode': str,
                'raw_response': dict
            }
        """
        # Vérifier le cache
        cache_key = self._build_cache_key(address, city, postcode)
        if cache_key in self._cache:
            self.stats['cache_hits'] += 1
            logger.debug(f"Cache hit pour: {address}")
            return self._cache[cache_key]

        # Construire la requête
        query_parts = [address]
        if city:
            query_parts.append(city)
        if postcode:
            query_parts.append(postcode)

        query = ' '.join(query_parts)
        encoded_query = quote(query)

        url = f"{API_BASE_URL}?q={encoded_query}&limit={limit}"

        # Ajouter le code postal comme filtre si disponible
        if postcode:
            url += f"&postcode={postcode}"

        logger.debug(f"Géocodage: {query}")

        # Effectuer la requête
        response = self._make_request(url)

        if not response:
            return None

        # Parser la réponse
        features = response.get('features', [])

        if not features:
            logger.warning(f"Adresse non trouvée: {query}")
            self.stats['not_found'] += 1

            # Mettre en cache le résultat négatif
            self._cache[cache_key] = None
            return None

        # Extraire le premier résultat
        feature = features[0]
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})

        coords = geometry.get('coordinates', [])

        if len(coords) < 2:
            self.stats['not_found'] += 1
            return None

        result = {
            'latitude': coords[1],  # API retourne [lon, lat]
            'longitude': coords[0],
            'label': properties.get('label', ''),
            'score': properties.get('score', 0),
            'city': properties.get('city', ''),
            'postcode': properties.get('postcode', ''),
            'context': properties.get('context', ''),
            'type': properties.get('type', ''),  # housenumber, street, municipality...
            'raw_response': feature,
            '_metadata': {
                'source': 'api-adresse.data.gouv.fr',
                'queried_at': datetime.utcnow().isoformat(),
                'original_query': query,
                'batch_id': self.batch_id
            }
        }

        # Mettre en cache
        self._cache[cache_key] = result

        self.stats['successful'] += 1
        logger.debug(f"  → Trouvé: {result['label']} (score: {result['score']:.2f})")

        return result

    def geocode_batch(self, addresses: List[Dict]) -> List[Dict]:
        """
        Géocode une liste d'adresses.

        Args:
            addresses: Liste de dictionnaires avec 'address', 'city', 'postcode'
                      [{'address': '...', 'city': '...', 'postcode': '...', 'id': ...}, ...]

        Returns:
            Liste de résultats avec l'ID original
        """
        self.stats['start_time'] = datetime.utcnow()

        logger.info("=" * 60)
        logger.info(f"Géocodage de {len(addresses)} adresses")
        logger.info("=" * 60)

        results = []

        for i, addr in enumerate(addresses, 1):
            address = addr.get('address', '')
            city = addr.get('city')
            postcode = addr.get('postcode')
            original_id = addr.get('id')

            logger.info(f"[{i}/{len(addresses)}] {address}, {city}")

            result = self.geocode(address, city, postcode)

            if result:
                result['original_id'] = original_id
                result['original_address'] = addr
            else:
                result = {
                    'original_id': original_id,
                    'original_address': addr,
                    'latitude': None,
                    'longitude': None,
                    'error': 'not_found'
                }

            results.append(result)

        self.stats['end_time'] = datetime.utcnow()

        # Résumé
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        logger.info("=" * 60)
        logger.info("GÉOCODAGE TERMINÉ")
        logger.info(f"  • Adresses traitées: {len(addresses)}")
        logger.info(f"  • Trouvées: {self.stats['successful']}")
        logger.info(f"  • Non trouvées: {self.stats['not_found']}")
        logger.info(f"  • Cache hits: {self.stats['cache_hits']}")
        logger.info(f"  • Requêtes API: {self.stats['requests_made']}")
        logger.info(f"  • Erreurs: {self.stats['errors']}")
        logger.info(f"  • Durée: {duration:.1f} secondes")
        logger.info("=" * 60)

        return results

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict]:
        """
        Géocodage inverse : coordonnées → adresse.

        Args:
            latitude: Latitude
            longitude: Longitude

        Returns:
            Dictionnaire avec l'adresse ou None si non trouvé
        """
        url = f"https://api-adresse.data.gouv.fr/reverse/?lon={longitude}&lat={latitude}"

        response = self._make_request(url)

        if not response:
            return None

        features = response.get('features', [])

        if not features:
            return None

        feature = features[0]
        properties = feature.get('properties', {})

        return {
            'label': properties.get('label', ''),
            'housenumber': properties.get('housenumber', ''),
            'street': properties.get('street', ''),
            'city': properties.get('city', ''),
            'postcode': properties.get('postcode', ''),
            'context': properties.get('context', '')
        }

    def get_stats(self) -> dict:
        """Retourne les statistiques du client."""
        return self.stats.copy()

    def clear_cache(self):
        """Vide le cache."""
        self._cache.clear()
        logger.info("Cache vidé")


# =============================================================================
# Point d'entrée
# =============================================================================

def main():
    """Fonction principale pour tests."""
    import json

    client = GeocodingClient()

    # Test avec quelques adresses
    test_addresses = [
        {
            'id': 1,
            'address': '20 avenue de Ségur',
            'city': 'Paris',
            'postcode': '75007'
        },
        {
            'id': 2,
            'address': '1 place de la Comédie',
            'city': 'Montpellier',
            'postcode': '34000'
        },
        {
            'id': 3,
            'address': '15 rue des Francs-Bourgeois',
            'city': 'Paris',
            'postcode': '75004'
        },
        {
            'id': 4,
            'address': 'Adresse inexistante 999',
            'city': 'VilleInexistante',
            'postcode': '00000'
        }
    ]

    results = client.geocode_batch(test_addresses)

    # Afficher les résultats
    for result in results:
        addr = result.get('original_address', {})
        if result.get('latitude'):
            print(f"[OK] {addr.get('address')}: ({result['latitude']:.6f}, {result['longitude']:.6f})")
        else:
            print(f"[ERREUR] {addr.get('address')}: Non trouve")

    # Stats
    print(f"\nStatistiques: {client.get_stats()}")

    return results


if __name__ == "__main__":
    main()
