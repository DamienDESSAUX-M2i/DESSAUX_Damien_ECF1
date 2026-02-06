// =============================================================================
// MongoDB - Script d'initialisation
// Création des bases Bronze et Silver avec leurs collections
// =============================================================================

// Connexion en tant qu'admin
db = db.getSiblingDB('admin');

// -----------------------------------------------------------------------------
// Base de données BRONZE (données brutes)
// -----------------------------------------------------------------------------
db = db.getSiblingDB('db_bronze');

// Création des collections avec validation de schéma optionnelle
db.createCollection('raw_books', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['title', '_metadata'],
            properties: {
                title: { bsonType: 'string' },
                price: { bsonType: 'string' },
                rating: { bsonType: 'string' },
                availability: { bsonType: 'string' },
                category: { bsonType: 'string' },
                url: { bsonType: 'string' },
                _metadata: {
                    bsonType: 'object',
                    required: ['source', 'scraped_at'],
                    properties: {
                        source: { bsonType: 'string' },
                        scraped_at: { bsonType: 'date' },
                        batch_id: { bsonType: 'string' }
                    }
                }
            }
        }
    },
    validationLevel: 'moderate'
});

db.createCollection('raw_quotes', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['text', 'author', '_metadata'],
            properties: {
                text: { bsonType: 'string' },
                author: { bsonType: 'string' },
                tags: { bsonType: 'array' },
                _metadata: {
                    bsonType: 'object',
                    required: ['source', 'scraped_at']
                }
            }
        }
    },
    validationLevel: 'moderate'
});

db.createCollection('raw_librairies');
db.createCollection('raw_geocoding');

// Index pour les performances
db.raw_books.createIndex({ "title": 1 });
db.raw_books.createIndex({ "_metadata.scraped_at": 1 });
db.raw_quotes.createIndex({ "author": 1 });
db.raw_quotes.createIndex({ "_metadata.scraped_at": 1 });
db.raw_librairies.createIndex({ "_metadata.imported_at": 1 });

print('[OK] Base db_bronze creee avec succes');

// -----------------------------------------------------------------------------
// Base de données SILVER (données nettoyées)
// -----------------------------------------------------------------------------
db = db.getSiblingDB('db_silver');

db.createCollection('clean_books');
db.createCollection('clean_quotes');
db.createCollection('clean_librairies');

// Index pour les performances
db.clean_books.createIndex({ "title": 1 });
db.clean_books.createIndex({ "category": 1 });
db.clean_books.createIndex({ "price_eur": 1 });
db.clean_quotes.createIndex({ "author": 1 });
db.clean_quotes.createIndex({ "tags": 1 });
db.clean_librairies.createIndex({ "ville": 1 });
db.clean_librairies.createIndex({ "contact_hash": 1 });

print('[OK] Base db_silver creee avec succes');

// -----------------------------------------------------------------------------
// Utilisateur applicatif (lecture/écriture)
// -----------------------------------------------------------------------------
db = db.getSiblingDB('admin');

db.createUser({
    user: 'datapulse_app',
    pwd: 'datapulse_app_2026',
    roles: [
        { role: 'readWrite', db: 'db_bronze' },
        { role: 'readWrite', db: 'db_silver' }
    ]
});

print('[OK] Utilisateur datapulse_app cree');
print('[OK] Initialisation MongoDB terminee');
