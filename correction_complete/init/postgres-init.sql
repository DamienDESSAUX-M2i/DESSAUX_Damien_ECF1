-- =============================================================================
-- PostgreSQL - Script d'initialisation
-- Création du schéma Gold pour l'analyse
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Extension pour les fonctions avancées
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- Table des catégories de livres (Dimension)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    category_slug VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_categories_name ON dim_categories(category_name);

COMMENT ON TABLE dim_categories IS 'Dimension des catégories de livres (Books to Scrape)';

-- -----------------------------------------------------------------------------
-- Table des livres (Fait)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_books (
    book_id SERIAL PRIMARY KEY,
    category_id INTEGER REFERENCES dim_categories(category_id),
    title VARCHAR(255) NOT NULL,
    price_gbp DECIMAL(10, 2),
    price_eur DECIMAL(10, 2),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    availability INTEGER DEFAULT 0,
    url VARCHAR(500),
    image_url VARCHAR(500),
    scraped_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_books_category ON fact_books(category_id);
CREATE INDEX idx_books_rating ON fact_books(rating);
CREATE INDEX idx_books_price ON fact_books(price_eur);
CREATE INDEX idx_books_scraped ON fact_books(scraped_at);

COMMENT ON TABLE fact_books IS 'Table de faits des livres scrapés depuis Books to Scrape';

-- -----------------------------------------------------------------------------
-- Table des auteurs (Dimension)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_authors (
    author_id SERIAL PRIMARY KEY,
    author_name VARCHAR(200) NOT NULL UNIQUE,
    author_slug VARCHAR(200),
    quote_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_authors_name ON dim_authors(author_name);

COMMENT ON TABLE dim_authors IS 'Dimension des auteurs de citations (Quotes to Scrape)';

-- -----------------------------------------------------------------------------
-- Table des tags (Dimension)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_tags (
    tag_id SERIAL PRIMARY KEY,
    tag_name VARCHAR(100) NOT NULL UNIQUE,
    tag_slug VARCHAR(100),
    usage_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tags_name ON dim_tags(tag_name);

COMMENT ON TABLE dim_tags IS 'Dimension des tags associés aux citations';

-- -----------------------------------------------------------------------------
-- Table des citations (Fait)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_quotes (
    quote_id SERIAL PRIMARY KEY,
    author_id INTEGER REFERENCES dim_authors(author_id),
    quote_text TEXT NOT NULL,
    quote_hash VARCHAR(64) UNIQUE, -- Pour éviter les doublons
    scraped_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quotes_author ON fact_quotes(author_id);
CREATE INDEX idx_quotes_scraped ON fact_quotes(scraped_at);

COMMENT ON TABLE fact_quotes IS 'Table de faits des citations scrapées depuis Quotes to Scrape';

-- -----------------------------------------------------------------------------
-- Table d'association citations-tags (N:M)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quote_tags (
    quote_id INTEGER REFERENCES fact_quotes(quote_id) ON DELETE CASCADE,
    tag_id INTEGER REFERENCES dim_tags(tag_id) ON DELETE CASCADE,
    PRIMARY KEY (quote_id, tag_id)
);

CREATE INDEX idx_quote_tags_quote ON quote_tags(quote_id);
CREATE INDEX idx_quote_tags_tag ON quote_tags(tag_id);

COMMENT ON TABLE quote_tags IS 'Association N:M entre citations et tags';

-- -----------------------------------------------------------------------------
-- Table des librairies partenaires (Dimension)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_librairies (
    librairie_id SERIAL PRIMARY KEY,
    nom VARCHAR(200) NOT NULL,
    adresse VARCHAR(300),
    code_postal VARCHAR(10),
    ville VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    specialite VARCHAR(100),
    date_partenariat DATE,
    contact_hash VARCHAR(64), -- Hash SHA-256 pour RGPD
    ca_annuel_range VARCHAR(50), -- Tranche de CA (anonymisé)
    imported_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_librairies_ville ON dim_librairies(ville);
CREATE INDEX idx_librairies_specialite ON dim_librairies(specialite);
CREATE INDEX idx_librairies_geo ON dim_librairies(latitude, longitude);

COMMENT ON TABLE dim_librairies IS 'Dimension des librairies partenaires (import Excel + géocodage)';

-- -----------------------------------------------------------------------------
-- Vue analytique : Statistiques par catégorie
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_stats_categories AS
SELECT
    c.category_name,
    COUNT(b.book_id) AS nb_books,
    ROUND(AVG(b.price_eur), 2) AS avg_price_eur,
    ROUND(AVG(b.rating), 2) AS avg_rating,
    MIN(b.price_eur) AS min_price,
    MAX(b.price_eur) AS max_price
FROM dim_categories c
LEFT JOIN fact_books b ON c.category_id = b.category_id
GROUP BY c.category_id, c.category_name
ORDER BY nb_books DESC;

COMMENT ON VIEW v_stats_categories IS 'Statistiques agrégées par catégorie de livres';

-- -----------------------------------------------------------------------------
-- Vue analytique : Top auteurs par nombre de citations
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_top_authors AS
SELECT
    a.author_name,
    COUNT(q.quote_id) AS nb_quotes,
    ARRAY_AGG(DISTINCT t.tag_name) AS tags_used
FROM dim_authors a
JOIN fact_quotes q ON a.author_id = q.author_id
LEFT JOIN quote_tags qt ON q.quote_id = qt.quote_id
LEFT JOIN dim_tags t ON qt.tag_id = t.tag_id
GROUP BY a.author_id, a.author_name
ORDER BY nb_quotes DESC;

COMMENT ON VIEW v_top_authors IS 'Classement des auteurs par nombre de citations';

-- -----------------------------------------------------------------------------
-- Vue analytique : Répartition géographique des librairies
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_librairies_geo AS
SELECT
    ville,
    COUNT(*) AS nb_librairies,
    ARRAY_AGG(DISTINCT specialite) AS specialites,
    MIN(date_partenariat) AS premier_partenariat
FROM dim_librairies
GROUP BY ville
ORDER BY nb_librairies DESC;

COMMENT ON VIEW v_librairies_geo IS 'Répartition géographique des librairies partenaires';

-- -----------------------------------------------------------------------------
-- Fonction : Mise à jour des compteurs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_author_quote_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE dim_authors SET quote_count = quote_count + 1
        WHERE author_id = NEW.author_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE dim_authors SET quote_count = quote_count - 1
        WHERE author_id = OLD.author_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_author_count
AFTER INSERT OR DELETE ON fact_quotes
FOR EACH ROW EXECUTE FUNCTION update_author_quote_count();

-- -----------------------------------------------------------------------------
-- Fonction : Mise à jour des compteurs de tags
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_tag_usage_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE dim_tags SET usage_count = usage_count + 1
        WHERE tag_id = NEW.tag_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE dim_tags SET usage_count = usage_count - 1
        WHERE tag_id = OLD.tag_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_tag_count
AFTER INSERT OR DELETE ON quote_tags
FOR EACH ROW EXECUTE FUNCTION update_tag_usage_count();

-- -----------------------------------------------------------------------------
-- Message de confirmation
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '✅ Schéma PostgreSQL Gold initialisé avec succès';
    RAISE NOTICE '📊 Tables créées : dim_categories, fact_books, dim_authors, fact_quotes, quote_tags, dim_librairies';
    RAISE NOTICE '👁️ Vues créées : v_stats_categories, v_top_authors, v_librairies_geo';
END $$;
