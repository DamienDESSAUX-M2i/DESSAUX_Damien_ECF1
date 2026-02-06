# Document de Conformité RGPD
## ECF Pipeline de Données Multi-Sources - DataPulse Analytics

**Version :** 1.0
**Date :** Janvier 2026
**Responsable du traitement :** DataPulse Analytics
**DPO :** [À compléter]

---

## Table des matières

1. [Inventaire des données personnelles](#1-inventaire-des-données-personnelles)
2. [Base légale des traitements](#2-base-légale-des-traitements)
3. [Mesures de protection](#3-mesures-de-protection)
4. [Durée de conservation](#4-durée-de-conservation)
5. [Procédure de suppression](#5-procédure-de-suppression)
6. [Registre des traitements](#6-registre-des-traitements)

---

## 1. Inventaire des données personnelles

### 1.1 Sources de données analysées

| Source | Type de source | Contient des donnees personnelles |
|--------|---------------|-----------------------------------|
| Books to Scrape | Web scraping | Non |
| Quotes to Scrape | Web scraping | Noms d'auteurs (donnees publiques) |
| API Adresse gouv.fr | API REST | Non |
| partenaire_librairies.xlsx | Fichier Excel | Oui |

### 1.2 Détail des données personnelles collectées

#### Fichier `partenaire_librairies.xlsx`

| Champ | Type de donnée | Catégorie RGPD | Sensibilité |
|-------|---------------|----------------|-------------|
| `contact_nom` | Nom et prénom | Donnée personnelle directe | Élevée |
| `contact_email` | Adresse email professionnelle | Donnée personnelle directe | Élevée |
| `contact_telephone` | Numéro de téléphone | Donnée personnelle directe | Élevée |

#### Site Quotes to Scrape

| Champ | Type de donnée | Catégorie RGPD | Sensibilité |
|-------|---------------|----------------|-------------|
| `author_name` | Nom d'auteur | Donnée personnelle publique | Faible |

**Note :** Les noms d'auteurs sur Quotes to Scrape sont des données publiques (auteurs célèbres, décédés pour la plupart) et ne nécessitent pas de protection particulière au titre du RGPD.

### 1.3 Données NON personnelles

Les données suivantes ne sont **pas** considérées comme personnelles :

- Informations sur les librairies (nom, adresse, spécialité) → Données d'entreprise
- Livres (titre, prix, catégorie) → Données publiques commerciales
- Citations (texte, tags) → Contenu littéraire public
- Coordonnées GPS → Données de localisation d'entreprise (pas de personnes)

---

## 2. Base légale des traitements

### 2.1 Analyse par finalité

| Finalité | Base légale (Art. 6 RGPD) | Justification |
|----------|---------------------------|---------------|
| Gestion des partenaires librairies | **Intérêt légitime** (Art. 6.1.f) | Relation commerciale B2B existante |
| Contact commercial | **Exécution contractuelle** (Art. 6.1.b) | Contrat de partenariat signé |
| Analyse statistique | **Intérêt légitime** (Art. 6.1.f) | Amélioration des services, données agrégées |

### 2.2 Intérêt légitime - Balance des intérêts

**Intérêt de DataPulse Analytics :**
- Maintenir une relation commerciale efficace avec les librairies partenaires
- Contacter les responsables pour les opérations courantes

**Impact sur les personnes concernées :**
- Impact limité : données professionnelles dans un contexte B2B
- Données utilisées uniquement dans le cadre de la relation commerciale
- Pas de profilage ni de prise de décision automatisée

**Conclusion :** L'intérêt légitime est retenu car l'impact sur les droits des personnes est minimal et proportionné à l'objectif poursuivi.

### 2.3 Information des personnes

Les contacts des librairies partenaires ont été informés de la collecte de leurs données lors de la signature du contrat de partenariat, conformément aux articles 13 et 14 du RGPD.

---

## 3. Mesures de protection

### 3.1 Pseudonymisation des données personnelles

#### Méthode appliquée : Hashage SHA-256

Les données personnelles sont pseudonymisées avant stockage dans les couches Silver et Gold :

```python
import hashlib

def pseudonymize(value: str, salt: str = "datapulse_2026") -> str:
    """
    Pseudonymise une valeur avec SHA-256 + sel
    """
    if not value:
        return None
    salted = f"{salt}:{value}"
    return hashlib.sha256(salted.encode()).hexdigest()

# Exemple d'application
contact_nom = "Marie Dubois"
contact_hash = pseudonymize(contact_nom)
# Résultat : "a7b9c3d4e5f6..."
```

#### Données hashées

| Donnée originale | Donnée stockée (Silver/Gold) |
|------------------|------------------------------|
| `contact_nom` | `contact_hash` (SHA-256) |
| `contact_email` | Non stocké |
| `contact_telephone` | Non stocké |

### 3.2 Minimisation des données

**Principe appliqué :** Seules les données strictement nécessaires sont conservées dans chaque couche.

| Couche | Données personnelles conservées |
|--------|--------------------------------|
| Bronze (brut) | Toutes (pour audit limité dans le temps) |
| Silver (nettoyé) | Hash du contact uniquement |
| Gold (final) | Hash du contact uniquement |

### 3.3 Sécurité technique

#### Accès aux données

| Mesure | Description |
|--------|-------------|
| Réseau isolé | Bases de données accessibles uniquement via réseau Docker interne |
| Authentification | Mots de passe requis pour MongoDB, PostgreSQL, MinIO |
| Pas d'exposition publique | Aucun port exposé sur Internet |

#### Configuration sécurisée

```yaml
# Variables d'environnement (non versionnées)
MONGO_INITDB_ROOT_USERNAME: admin
MONGO_INITDB_ROOT_PASSWORD: ${MONGO_PASSWORD}  # Variable d'environnement
POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}        # Variable d'environnement
```

### 3.4 Séparation des environnements

| Environnement | Accès | Données personnelles |
|---------------|-------|---------------------|
| Développement | Équipe technique | Données anonymisées / fictives |
| Production | Restreint | Données réelles pseudonymisées |
| Backup | Administrateurs uniquement | Chiffrées |

---

## 4. Durée de conservation

### 4.1 Politique de rétention

| Type de donnée | Couche | Durée de conservation | Justification |
|----------------|--------|----------------------|---------------|
| Données brutes avec infos personnelles | Bronze | **30 jours** | Audit et retraitement |
| Données pseudonymisées | Silver/Gold | **Durée du partenariat + 3 ans** | Obligations légales |
| Logs d'accès | Système | **1 an** | Sécurité |

### 4.2 Purge automatique

Un script de purge s'exécute mensuellement pour supprimer les données Bronze de plus de 30 jours :

```python
from datetime import datetime, timedelta

def purge_old_bronze_data(db, days=30):
    """Supprime les données Bronze de plus de X jours"""
    cutoff = datetime.utcnow() - timedelta(days=days)

    collections = ['raw_librairies']  # Collections avec données personnelles

    for collection in collections:
        result = db[collection].delete_many({
            "_metadata.imported_at": {"$lt": cutoff}
        })
        print(f"Purged {result.deleted_count} documents from {collection}")
```

---

## 5. Procédure de suppression

### 5.1 Droit à l'effacement (Art. 17 RGPD)

Toute personne concernée peut demander la suppression de ses données personnelles.

### 5.2 Procédure de traitement d'une demande

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PROCÉDURE DE SUPPRESSION                         │
└─────────────────────────────────────────────────────────────────────┘

1. RÉCEPTION DE LA DEMANDE
   │
   ▼
2. VÉRIFICATION DE L'IDENTITÉ (48h max)
   │  • Demande de justificatif d'identité
   │  • Confirmation de la relation avec la librairie
   │
   ▼
3. RECHERCHE DES DONNÉES (72h max)
   │  • Recherche dans toutes les couches (Bronze, Silver, Gold)
   │  • Identification par nom, email ou téléphone
   │
   ▼
4. SUPPRESSION (24h après validation)
   │  • Exécution du script de suppression
   │  • Suppression dans toutes les bases
   │  • Suppression des backups concernés
   │
   ▼
5. CONFIRMATION (sous 30 jours)
   │  • Envoi d'une attestation de suppression
   │  • Mise à jour du registre des demandes
   │
   ▼
6. DOCUMENTATION
      • Archivage de la demande (sans données personnelles)
      • Mise à jour du registre des traitements
```

### 5.3 Script de suppression

```python
#!/usr/bin/env python3
"""
Script de suppression des données personnelles sur demande RGPD
Usage: python rgpd_delete.py --email "contact@example.com"
"""

import argparse
import hashlib
from pymongo import MongoClient
import psycopg2

def delete_person_data(identifier: str, identifier_type: str = "email"):
    """
    Supprime toutes les données d'une personne identifiée

    Args:
        identifier: email, nom ou téléphone
        identifier_type: type d'identifiant ("email", "nom", "telephone")
    """

    # Connexion aux bases
    mongo_client = MongoClient("mongodb://localhost:27017")
    pg_conn = psycopg2.connect("postgresql://localhost:5432/datapulse")

    # 1. Suppression Bronze (données brutes)
    db_bronze = mongo_client["db_bronze"]
    query = {f"contact_{identifier_type}": identifier}
    result_bronze = db_bronze["raw_librairies"].delete_many(query)
    print(f"Bronze: {result_bronze.deleted_count} documents supprimés")

    # 2. Suppression Silver (données nettoyées)
    db_silver = mongo_client["db_silver"]
    # Recherche par hash
    salt = "datapulse_2026"
    hash_value = hashlib.sha256(f"{salt}:{identifier}".encode()).hexdigest()
    result_silver = db_silver["clean_librairies"].delete_many({
        "contact_hash": hash_value
    })
    print(f"Silver: {result_silver.deleted_count} documents supprimés")

    # 3. Suppression Gold (PostgreSQL)
    cursor = pg_conn.cursor()
    cursor.execute(
        "DELETE FROM dim_librairies WHERE contact_hash = %s",
        (hash_value,)
    )
    pg_conn.commit()
    print(f"Gold: {cursor.rowcount} enregistrements supprimés")

    # 4. Log de la suppression (sans données personnelles)
    log_deletion(identifier_type, hash_value)

    print("\n[OK] Suppression terminee avec succes")

    return {
        "bronze_deleted": result_bronze.deleted_count,
        "silver_deleted": result_silver.deleted_count,
        "gold_deleted": cursor.rowcount
    }

def log_deletion(identifier_type: str, hash_value: str):
    """Enregistre la suppression pour audit"""
    from datetime import datetime

    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "action": "RGPD_DELETION",
        "identifier_type": identifier_type,
        "identifier_hash": hash_value[:16] + "...",  # Hash tronqué
        "status": "completed"
    }

    # Écriture dans le fichier de log
    with open("rgpd_deletions.log", "a") as f:
        f.write(f"{log_entry}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Suppression RGPD")
    parser.add_argument("--email", help="Email du contact à supprimer")
    parser.add_argument("--nom", help="Nom du contact à supprimer")
    parser.add_argument("--telephone", help="Téléphone du contact à supprimer")

    args = parser.parse_args()

    if args.email:
        delete_person_data(args.email, "email")
    elif args.nom:
        delete_person_data(args.nom, "nom")
    elif args.telephone:
        delete_person_data(args.telephone, "telephone")
    else:
        print("Erreur: Spécifiez --email, --nom ou --telephone")
```

---

## 6. Registre des traitements

### 6.1 Fiche de traitement : Gestion des partenaires librairies

| Élément | Description |
|---------|-------------|
| **Nom du traitement** | Gestion des librairies partenaires |
| **Responsable** | DataPulse Analytics |
| **Finalité** | Gestion de la relation commerciale B2B |
| **Base légale** | Intérêt légitime / Exécution contractuelle |
| **Catégories de personnes** | Contacts professionnels des librairies partenaires |
| **Catégories de données** | Nom, email professionnel, téléphone professionnel |
| **Destinataires** | Équipe commerciale DataPulse (interne uniquement) |
| **Transferts hors UE** | Aucun |
| **Durée de conservation** | Durée du partenariat + 3 ans |
| **Mesures de sécurité** | Pseudonymisation, accès restreint, réseau isolé |

### 6.2 Historique des mises à jour

| Date | Version | Modification | Auteur |
|------|---------|--------------|--------|
| 2026-01-22 | 1.0 | Création initiale | Data Engineer |

---

## Annexes

### A. Contacts

- **DPO DataPulse Analytics :** [dpo@datapulse.example.com]
- **Demandes RGPD :** [rgpd@datapulse.example.com]

### B. Références légales

- [Règlement (UE) 2016/679 - RGPD](https://eur-lex.europa.eu/eli/reg/2016/679/oj)
- [CNIL - Guide de la sécurité des données personnelles](https://www.cnil.fr/fr/guide-de-la-securite-des-donnees-personnelles)
- [CNIL - Durées de conservation](https://www.cnil.fr/fr/les-durees-de-conservation-des-donnees)
