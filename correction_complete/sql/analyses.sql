-- =============================================================================
-- ECF DataPulse Analytics - Requêtes Analytiques
-- 5 requêtes démontrant la valeur de la plateforme
-- =============================================================================

-- -----------------------------------------------------------------------------
-- REQUÊTE 1 : Agrégation simple
-- Statistiques par catégorie de livres
-- -----------------------------------------------------------------------------
-- Cette requête calcule les métriques clés pour chaque catégorie de livres :
-- nombre de livres, prix moyen, note moyenne, écart des prix.

SELECT
    c.category_name AS "Catégorie",
    COUNT(b.book_id) AS "Nombre de livres",
    ROUND(AVG(b.price_eur), 2) AS "Prix moyen (€)",
    ROUND(AVG(b.rating), 2) AS "Note moyenne",
    MIN(b.price_eur) AS "Prix min (€)",
    MAX(b.price_eur) AS "Prix max (€)",
    SUM(b.availability) AS "Stock total"
FROM dim_categories c
LEFT JOIN fact_books b ON c.category_id = b.category_id
GROUP BY c.category_id, c.category_name
HAVING COUNT(b.book_id) > 0
ORDER BY "Nombre de livres" DESC;

-- Résultat attendu : Liste des catégories avec leurs statistiques agrégées
-- Exemple :
-- | Catégorie        | Nombre de livres | Prix moyen (€) | Note moyenne |
-- |------------------|------------------|----------------|--------------|
-- | Fiction          | 65               | 35.50          | 3.2          |
-- | Mystery          | 32               | 42.10          | 3.8          |


-- -----------------------------------------------------------------------------
-- REQUÊTE 2 : Jointure entre tables
-- Citations avec leurs auteurs et nombre de tags
-- -----------------------------------------------------------------------------
-- Cette requête joint les citations avec leurs auteurs et compte les tags
-- associés à chaque citation.

SELECT
    a.author_name AS "Auteur",
    LEFT(q.quote_text, 80) || '...' AS "Citation (extrait)",
    q.text_length AS "Longueur",
    COUNT(qt.tag_id) AS "Nombre de tags",
    STRING_AGG(t.tag_name, ', ' ORDER BY t.tag_name) AS "Tags"
FROM fact_quotes q
INNER JOIN dim_authors a ON q.author_id = a.author_id
LEFT JOIN quote_tags qt ON q.quote_id = qt.quote_id
LEFT JOIN dim_tags t ON qt.tag_id = t.tag_id
GROUP BY q.quote_id, a.author_name, q.quote_text, q.text_length
ORDER BY "Nombre de tags" DESC, a.author_name
LIMIT 15;

-- Résultat attendu : Citations enrichies avec informations auteur et tags
-- Exemple :
-- | Auteur           | Citation (extrait)              | Longueur | Nombre de tags | Tags              |
-- |------------------|--------------------------------|----------|----------------|-------------------|
-- | Albert Einstein  | The world as we have created...| 156      | 4              | change, deep, ...  |


-- -----------------------------------------------------------------------------
-- REQUÊTE 3 : Fonction de fenêtrage (Window Function)
-- Classement des livres par prix dans leur catégorie
-- -----------------------------------------------------------------------------
-- Cette requête utilise des window functions pour :
-- - Classer les livres par prix au sein de chaque catégorie (RANK)
-- - Calculer l'écart au prix moyen de la catégorie
-- - Afficher le prix cumulé

SELECT
    c.category_name AS "Catégorie",
    b.title AS "Titre",
    b.price_eur AS "Prix (€)",
    b.rating AS "Note",

    -- Rang dans la catégorie (par prix décroissant)
    RANK() OVER (
        PARTITION BY c.category_id
        ORDER BY b.price_eur DESC
    ) AS "Rang prix (catégorie)",

    -- Écart au prix moyen de la catégorie
    ROUND(
        b.price_eur - AVG(b.price_eur) OVER (PARTITION BY c.category_id),
        2
    ) AS "Écart prix moyen (€)",

    -- Pourcentile du prix dans la catégorie
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY c.category_id
            ORDER BY b.price_eur
        ) * 100,
        1
    ) AS "Percentile prix (%)",

    -- Total cumulé des prix dans la catégorie
    ROUND(
        SUM(b.price_eur) OVER (
            PARTITION BY c.category_id
            ORDER BY b.price_eur DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    ) AS "Prix cumulé (€)"

FROM fact_books b
JOIN dim_categories c ON b.category_id = c.category_id
ORDER BY c.category_name, "Rang prix (catégorie)"
LIMIT 20;

-- Résultat attendu : Livres avec leur position relative dans leur catégorie
-- Exemple :
-- | Catégorie | Titre             | Prix (€) | Note | Rang | Écart moyen | Percentile |
-- |-----------|-------------------|----------|------|------|-------------|------------|
-- | Fiction   | Expensive Book    | 59.99    | 4    | 1    | +24.49      | 98.5       |
-- | Fiction   | Another Book      | 45.50    | 3    | 2    | +10.00      | 85.2       |


-- -----------------------------------------------------------------------------
-- REQUÊTE 4 : Classement Top N
-- Top 10 des auteurs les plus prolifiques avec statistiques
-- -----------------------------------------------------------------------------
-- Cette requête identifie les auteurs ayant le plus de citations et
-- calcule des métriques sur leurs contenus.

SELECT
    a.author_name AS "Auteur",
    a.quote_count AS "Nombre de citations",

    -- Longueur moyenne des citations
    ROUND(AVG(q.text_length), 0) AS "Longueur moyenne",

    -- Citation la plus courte et la plus longue
    MIN(q.text_length) AS "Plus courte",
    MAX(q.text_length) AS "Plus longue",

    -- Tags les plus utilisés par cet auteur
    (
        SELECT STRING_AGG(DISTINCT t.tag_name, ', ')
        FROM quote_tags qt
        JOIN dim_tags t ON qt.tag_id = t.tag_id
        JOIN fact_quotes q2 ON qt.quote_id = q2.quote_id
        WHERE q2.author_id = a.author_id
        LIMIT 5
    ) AS "Tags principaux"

FROM dim_authors a
JOIN fact_quotes q ON a.author_id = q.author_id
GROUP BY a.author_id, a.author_name, a.quote_count
ORDER BY a.quote_count DESC
LIMIT 10;

-- Résultat attendu : Top 10 des auteurs les plus cités
-- Exemple :
-- | Auteur              | Nombre de citations | Longueur moyenne | Tags principaux     |
-- |---------------------|---------------------|------------------|---------------------|
-- | Albert Einstein     | 10                  | 145              | inspirational, life |
-- | Marilyn Monroe      | 8                   | 98               | love, friendship    |


-- -----------------------------------------------------------------------------
-- REQUÊTE 5 : Croisement de sources de données
-- Analyse croisée : Livres disponibles vs spécialités des librairies
-- -----------------------------------------------------------------------------
-- Cette requête croise les données des livres (scraping) avec les données
-- des librairies partenaires (Excel) pour identifier les opportunités
-- commerciales : quelles librairies pourraient vendre quels livres.

WITH librairie_categories AS (
    -- Mapping des spécialités de librairies vers les catégories de livres
    SELECT
        l.librairie_id,
        l.nom AS librairie_nom,
        l.ville,
        l.specialite,
        CASE l.specialite
            WHEN 'Littérature' THEN ARRAY['Fiction', 'Poetry', 'Classics', 'Romance']
            WHEN 'Sciences humaines' THEN ARRAY['Philosophy', 'Psychology', 'History', 'Politics']
            WHEN 'Jeunesse' THEN ARRAY['Childrens', 'Young Adult', 'Fantasy']
            WHEN 'Sciences' THEN ARRAY['Science', 'Technology', 'Mathematics']
            WHEN 'Art' THEN ARRAY['Art', 'Music', 'Photography']
            WHEN 'Voyage' THEN ARRAY['Travel', 'Geography']
            WHEN 'BD et Manga' THEN ARRAY['Sequential Art', 'Graphic Novels', 'Comics']
            ELSE ARRAY['Fiction']  -- Par défaut
        END AS categories_associees
    FROM dim_librairies l
),
books_stats AS (
    -- Statistiques par catégorie de livres
    SELECT
        c.category_name,
        COUNT(*) AS nb_livres,
        ROUND(AVG(b.price_eur), 2) AS prix_moyen,
        SUM(b.availability) AS stock_total
    FROM fact_books b
    JOIN dim_categories c ON b.category_id = c.category_id
    GROUP BY c.category_name
)
SELECT
    lc.librairie_nom AS "Librairie",
    lc.ville AS "Ville",
    lc.specialite AS "Spécialité",
    c.category_name AS "Catégorie suggérée",
    bs.nb_livres AS "Livres disponibles",
    bs.prix_moyen AS "Prix moyen (€)",
    bs.stock_total AS "Stock total",

    -- Score d'opportunité (plus le stock est élevé et le prix accessible, mieux c'est)
    ROUND(
        (bs.stock_total::numeric / NULLIF(bs.prix_moyen, 0)) * 10,
        2
    ) AS "Score opportunité"

FROM librairie_categories lc
CROSS JOIN LATERAL unnest(lc.categories_associees) AS cat(category_name)
JOIN books_stats bs ON bs.category_name ILIKE cat.category_name || '%'
JOIN dim_categories c ON c.category_name ILIKE cat.category_name || '%'
ORDER BY "Score opportunité" DESC, lc.librairie_nom
LIMIT 20;

-- Résultat attendu : Croisement librairies/livres avec score d'opportunité
-- Exemple :
-- | Librairie           | Ville | Spécialité       | Catégorie suggérée | Livres | Score |
-- |---------------------|-------|------------------|-------------------|--------|-------|
-- | Librairie du Marais | Paris | Littérature      | Fiction           | 65     | 185.2 |
-- | Le Comptoir Lettres | Lyon  | Sciences humaines| Philosophy        | 11     | 42.5  |


-- =============================================================================
-- REQUÊTES BONUS : Vues matérialisées pour performance
-- =============================================================================

-- Vue pour dashboard des statistiques globales
CREATE OR REPLACE VIEW v_dashboard_global AS
SELECT
    (SELECT COUNT(*) FROM fact_books) AS total_livres,
    (SELECT COUNT(*) FROM fact_quotes) AS total_citations,
    (SELECT COUNT(*) FROM dim_librairies) AS total_librairies,
    (SELECT COUNT(DISTINCT category_id) FROM fact_books) AS nb_categories,
    (SELECT COUNT(DISTINCT author_id) FROM fact_quotes) AS nb_auteurs,
    (SELECT ROUND(AVG(price_eur), 2) FROM fact_books) AS prix_moyen_livres,
    (SELECT ROUND(AVG(rating), 2) FROM fact_books) AS note_moyenne_livres;

-- Exemple d'utilisation :
-- SELECT * FROM v_dashboard_global;


-- Vue pour analyse géographique des librairies
CREATE OR REPLACE VIEW v_analyse_geo_librairies AS
SELECT
    ville,
    COUNT(*) AS nb_librairies,
    ARRAY_AGG(DISTINCT specialite) AS specialites,
    ROUND(AVG(latitude), 6) AS centre_lat,
    ROUND(AVG(longitude), 6) AS centre_lon
FROM dim_librairies
WHERE latitude IS NOT NULL
GROUP BY ville
ORDER BY nb_librairies DESC;

-- Exemple d'utilisation :
-- SELECT * FROM v_analyse_geo_librairies;


-- =============================================================================
-- REQUÊTE DE MAINTENANCE : Vérification de la qualité des données
-- =============================================================================

-- Rapport de qualité des données
SELECT
    'Livres sans catégorie' AS "Contrôle",
    COUNT(*) AS "Nombre",
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ATTENTION' END AS "Statut"
FROM fact_books WHERE category_id IS NULL

UNION ALL

SELECT
    'Citations sans auteur',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ATTENTION' END
FROM fact_quotes WHERE author_id IS NULL

UNION ALL

SELECT
    'Librairies sans coordonnées GPS',
    COUNT(*),
    CASE WHEN COUNT(*) < 3 THEN 'OK' ELSE 'ATTENTION' END
FROM dim_librairies WHERE latitude IS NULL

UNION ALL

SELECT
    'Livres avec prix aberrant (< 1€ ou > 100€)',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ATTENTION' END
FROM fact_books WHERE price_eur < 1 OR price_eur > 100

UNION ALL

SELECT
    'Citations en doublon (même hash)',
    COUNT(*) - COUNT(DISTINCT quote_hash),
    CASE WHEN COUNT(*) = COUNT(DISTINCT quote_hash) THEN 'OK' ELSE 'ATTENTION' END
FROM fact_quotes;
