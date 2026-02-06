#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module d'import du fichier Excel partenaires
ECF DataPulse Analytics

Fonctionnalités :
- Validation du format Excel
- Traitement des données personnelles conforme RGPD
- Pseudonymisation avec SHA-256
- Validation des données
"""

import pandas as pd
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import uuid
import re

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ExcelImporter')

# =============================================================================
# Configuration
# =============================================================================

# Colonnes attendues dans le fichier Excel
EXPECTED_COLUMNS = [
    'nom_librairie',
    'adresse',
    'code_postal',
    'ville',
    'contact_nom',
    'contact_email',
    'contact_telephone',
    'ca_annuel',
    'date_partenariat',
    'specialite'
]

# Colonnes contenant des données personnelles (RGPD)
PERSONAL_DATA_COLUMNS = ['contact_nom', 'contact_email', 'contact_telephone']

# Colonnes confidentielles
CONFIDENTIAL_COLUMNS = ['ca_annuel']

# Sel pour le hashage (à stocker en variable d'environnement en production)
HASH_SALT = "datapulse_2026"


# =============================================================================
# Classe principale
# =============================================================================

class ExcelImporter:
    """
    Importeur de fichier Excel avec conformité RGPD.
    Gère la validation, l'anonymisation et la transformation des données.
    """

    def __init__(self, salt: str = HASH_SALT):
        """
        Initialise l'importeur.

        Args:
            salt: Sel pour le hashage SHA-256
        """
        self.salt = salt
        self.batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        self.stats = {
            'rows_read': 0,
            'rows_valid': 0,
            'rows_invalid': 0,
            'validation_errors': [],
            'personal_data_hashed': 0,
            'start_time': None,
            'end_time': None
        }

        logger.info(f"Importeur initialisé - Batch ID: {self.batch_id}")

    def _hash_personal_data(self, value: str) -> str:
        """
        Pseudonymise une donnée personnelle avec SHA-256.

        Args:
            value: Valeur à hasher

        Returns:
            Hash SHA-256 de la valeur
        """
        if pd.isna(value) or not value:
            return None

        salted = f"{self.salt}:{str(value).strip()}"
        return hashlib.sha256(salted.encode()).hexdigest()

    def _anonymize_ca(self, ca: float) -> str:
        """
        Anonymise le CA annuel en tranche.

        Args:
            ca: Chiffre d'affaires

        Returns:
            Tranche de CA
        """
        if pd.isna(ca):
            return "Non renseigné"

        ca = float(ca)

        if ca < 100000:
            return "< 100k€"
        elif ca < 250000:
            return "100k€ - 250k€"
        elif ca < 500000:
            return "250k€ - 500k€"
        elif ca < 1000000:
            return "500k€ - 1M€"
        else:
            return "> 1M€"

    def _validate_email(self, email: str) -> bool:
        """Valide le format d'un email."""
        if pd.isna(email) or not email:
            return True  # Optionnel

        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email).strip()))

    def _validate_phone(self, phone: str) -> bool:
        """Valide le format d'un numéro de téléphone français."""
        if pd.isna(phone) or not phone:
            return True  # Optionnel

        # Nettoyer le numéro
        cleaned = re.sub(r'[\s\.\-]', '', str(phone))

        # Format français : 10 chiffres commençant par 0
        # OU 9 chiffres (Excel supprime souvent le 0 initial)
        return bool(re.match(r'^0[1-9]\d{8}$', cleaned) or re.match(r'^[1-9]\d{8}$', cleaned))

    def _validate_postcode(self, postcode: str) -> bool:
        """Valide le format d'un code postal français."""
        if pd.isna(postcode) or not postcode:
            return False  # Obligatoire

        cleaned = str(postcode).strip()
        return bool(re.match(r'^\d{5}$', cleaned))

    def _validate_row(self, row: pd.Series, index: int) -> Tuple[bool, List[str]]:
        """
        Valide une ligne de données.

        Args:
            row: Ligne pandas
            index: Index de la ligne

        Returns:
            Tuple (est_valide, liste_erreurs)
        """
        errors = []

        # Nom de la librairie obligatoire
        if pd.isna(row.get('nom_librairie')) or not row.get('nom_librairie'):
            errors.append(f"Ligne {index}: nom_librairie manquant")

        # Adresse obligatoire
        if pd.isna(row.get('adresse')) or not row.get('adresse'):
            errors.append(f"Ligne {index}: adresse manquante")

        # Code postal valide
        if not self._validate_postcode(row.get('code_postal')):
            errors.append(f"Ligne {index}: code_postal invalide ({row.get('code_postal')})")

        # Ville obligatoire
        if pd.isna(row.get('ville')) or not row.get('ville'):
            errors.append(f"Ligne {index}: ville manquante")

        # Email valide (si renseigné)
        if not self._validate_email(row.get('contact_email')):
            errors.append(f"Ligne {index}: email invalide ({row.get('contact_email')})")

        # Téléphone valide (si renseigné)
        if not self._validate_phone(row.get('contact_telephone')):
            errors.append(f"Ligne {index}: téléphone invalide ({row.get('contact_telephone')})")

        return len(errors) == 0, errors

    def validate_file(self, file_path: str) -> Tuple[bool, List[str]]:
        """
        Valide le format du fichier Excel.

        Args:
            file_path: Chemin vers le fichier Excel

        Returns:
            Tuple (est_valide, liste_erreurs)
        """
        errors = []

        # Vérifier que le fichier existe
        if not Path(file_path).exists():
            return False, [f"Fichier non trouvé: {file_path}"]

        # Vérifier l'extension
        if not file_path.lower().endswith(('.xlsx', '.xls')):
            return False, [f"Extension invalide. Attendu: .xlsx ou .xls"]

        try:
            df = pd.read_excel(file_path)

            # Vérifier les colonnes attendues
            missing_columns = set(EXPECTED_COLUMNS) - set(df.columns)
            if missing_columns:
                errors.append(f"Colonnes manquantes: {missing_columns}")

            extra_columns = set(df.columns) - set(EXPECTED_COLUMNS)
            if extra_columns:
                logger.warning(f"Colonnes supplémentaires ignorées: {extra_columns}")

            # Vérifier qu'il y a des données
            if df.empty:
                errors.append("Le fichier est vide")

        except Exception as e:
            errors.append(f"Erreur lecture fichier: {e}")

        return len(errors) == 0, errors

    def import_file(
        self,
        file_path: str,
        anonymize: bool = True,
        validate: bool = True
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Importe le fichier Excel avec traitement RGPD.

        Args:
            file_path: Chemin vers le fichier Excel
            anonymize: Anonymiser les données personnelles
            validate: Valider les données

        Returns:
            Tuple (données_valides, données_brutes_pour_bronze)
        """
        self.stats['start_time'] = datetime.utcnow()

        logger.info("=" * 60)
        logger.info(f"Import du fichier: {file_path}")
        logger.info("=" * 60)

        # Validation du fichier
        is_valid, errors = self.validate_file(file_path)
        if not is_valid:
            logger.error(f"Fichier invalide: {errors}")
            raise ValueError(f"Fichier invalide: {errors}")

        # Lecture du fichier
        df = pd.read_excel(file_path)
        self.stats['rows_read'] = len(df)
        logger.info(f"Lignes lues: {self.stats['rows_read']}")

        # Données brutes pour la couche Bronze
        bronze_records = []

        # Données transformées pour la couche Silver
        silver_records = []

        for index, row in df.iterrows():
            # Validation de la ligne
            if validate:
                is_row_valid, row_errors = self._validate_row(row, index + 2)  # +2 car Excel commence à 1 + header
                if not is_row_valid:
                    self.stats['rows_invalid'] += 1
                    self.stats['validation_errors'].extend(row_errors)
                    logger.warning(f"Ligne {index + 2} invalide: {row_errors}")
                    continue

            # Données brutes (Bronze) - avec données personnelles pour audit temporaire
            bronze_record = {
                'nom_librairie': str(row.get('nom_librairie', '')).strip(),
                'adresse': str(row.get('adresse', '')).strip(),
                'code_postal': str(row.get('code_postal', '')).strip(),
                'ville': str(row.get('ville', '')).strip(),
                'contact_nom': str(row.get('contact_nom', '')).strip() if not pd.isna(row.get('contact_nom')) else None,
                'contact_email': str(row.get('contact_email', '')).strip() if not pd.isna(row.get('contact_email')) else None,
                'contact_telephone': str(row.get('contact_telephone', '')).strip() if not pd.isna(row.get('contact_telephone')) else None,
                'ca_annuel': float(row.get('ca_annuel')) if not pd.isna(row.get('ca_annuel')) else None,
                'date_partenariat': row.get('date_partenariat').isoformat() if not pd.isna(row.get('date_partenariat')) else None,
                'specialite': str(row.get('specialite', '')).strip() if not pd.isna(row.get('specialite')) else None,
                '_metadata': {
                    'source': 'partenaire_librairies.xlsx',
                    'imported_at': datetime.utcnow(),
                    'batch_id': self.batch_id,
                    'row_number': index + 2,
                    'contains_personal_data': True  # Marqueur pour purge RGPD
                }
            }
            bronze_records.append(bronze_record)

            # Données anonymisées (Silver)
            if anonymize:
                # Créer un hash combiné des données personnelles
                personal_data = '|'.join(filter(None, [
                    bronze_record['contact_nom'],
                    bronze_record['contact_email'],
                    bronze_record['contact_telephone']
                ]))
                contact_hash = self._hash_personal_data(personal_data) if personal_data else None

                silver_record = {
                    'nom': bronze_record['nom_librairie'],
                    'adresse': bronze_record['adresse'],
                    'code_postal': bronze_record['code_postal'],
                    'ville': bronze_record['ville'],
                    'specialite': bronze_record['specialite'],
                    'date_partenariat': bronze_record['date_partenariat'],
                    'ca_annuel_range': self._anonymize_ca(bronze_record['ca_annuel']),
                    'contact_hash': contact_hash,  # Hash des données personnelles
                    # Coordonnées GPS à remplir par le géocodage
                    'latitude': None,
                    'longitude': None,
                    '_metadata': {
                        'source': 'partenaire_librairies.xlsx',
                        'imported_at': datetime.utcnow(),
                        'batch_id': self.batch_id,
                        'anonymized': True,
                        'rgpd_compliant': True
                    }
                }

                if contact_hash:
                    self.stats['personal_data_hashed'] += 1

            else:
                silver_record = bronze_record.copy()

            silver_records.append(silver_record)
            self.stats['rows_valid'] += 1

        self.stats['end_time'] = datetime.utcnow()

        # Résumé
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        logger.info("=" * 60)
        logger.info("IMPORT TERMINÉ")
        logger.info(f"  • Lignes lues: {self.stats['rows_read']}")
        logger.info(f"  • Lignes valides: {self.stats['rows_valid']}")
        logger.info(f"  • Lignes invalides: {self.stats['rows_invalid']}")
        logger.info(f"  • Données personnelles hashées: {self.stats['personal_data_hashed']}")
        logger.info(f"  • Durée: {duration:.2f} secondes")
        logger.info(f"  • Batch ID: {self.batch_id}")

        if self.stats['validation_errors']:
            logger.warning(f"  • Erreurs de validation: {len(self.stats['validation_errors'])}")
            for error in self.stats['validation_errors'][:5]:
                logger.warning(f"    - {error}")
            if len(self.stats['validation_errors']) > 5:
                logger.warning(f"    ... et {len(self.stats['validation_errors']) - 5} autres")

        logger.info("=" * 60)

        return silver_records, bronze_records

    def get_addresses_for_geocoding(self, records: List[Dict]) -> List[Dict]:
        """
        Prépare les adresses pour le géocodage.

        Args:
            records: Liste des enregistrements importés

        Returns:
            Liste formatée pour le client de géocodage
        """
        addresses = []

        for i, record in enumerate(records):
            addresses.append({
                'id': i,
                'address': record.get('adresse', ''),
                'city': record.get('ville', ''),
                'postcode': record.get('code_postal', '')
            })

        return addresses

    def get_stats(self) -> dict:
        """Retourne les statistiques de l'import."""
        return self.stats.copy()


# =============================================================================
# Point d'entrée
# =============================================================================

def main():
    """Fonction principale pour tests."""
    import json

    # Créer un fichier Excel de test si nécessaire
    test_file = "data/partenaire_librairies.xlsx"

    if not Path(test_file).exists():
        logger.info("Création d'un fichier de test...")
        create_test_file(test_file)

    importer = ExcelImporter()

    try:
        silver_records, bronze_records = importer.import_file(test_file)

        print(f"\n[OK] Import reussi: {len(silver_records)} enregistrements")

        # Afficher un exemple
        if silver_records:
            print("\nExemple d'enregistrement (anonymisé):")
            example = silver_records[0].copy()
            example['_metadata']['imported_at'] = example['_metadata']['imported_at'].isoformat()
            print(json.dumps(example, indent=2, ensure_ascii=False))

    except ValueError as e:
        print(f"[ERREUR] Erreur: {e}")


def create_test_file(file_path: str):
    """Crée un fichier Excel de test."""
    from datetime import date

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    data = {
        'nom_librairie': ['Librairie du Marais', 'Le Comptoir des Lettres', 'Bibliothèque Vivante'],
        'adresse': ['15 rue des Francs-Bourgeois', '42 avenue Jean Jaurès', '8 place Bellecour'],
        'code_postal': ['75004', '69007', '69002'],
        'ville': ['Paris', 'Lyon', 'Lyon'],
        'contact_nom': ['Marie Dubois', 'Jean Martin', 'Sophie Bernard'],
        'contact_email': ['m.dubois@librairie.fr', 'j.martin@comptoir.fr', 's.bernard@biblio.fr'],
        'contact_telephone': ['0142789012', '0478123456', '0472345678'],
        'ca_annuel': [385000, 520000, 180000],
        'date_partenariat': [date(2021, 3, 15), date(2020, 6, 1), date(2022, 1, 10)],
        'specialite': ['Littérature', 'Sciences humaines', 'Jeunesse']
    }

    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False)
    logger.info(f"Fichier de test créé: {file_path}")


if __name__ == "__main__":
    main()
