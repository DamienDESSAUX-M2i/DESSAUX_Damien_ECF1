-- ===
-- QUOTES
-- ===

-- 1. Une requête d'agrégation simple

-- Nombre de citations par tag
SELECT
    t.tag_name,
    COUNT(id_quote) AS total_quotes
FROM quotes_tags AS qt
INNER JOIN tags as t ON qt.id_tag = t.id_tag
GROUP BY t.tag_name
ORDER BY total_quotes DESC;

-- 2. Une requête avec jointure

-- Nombre de citations par auteur
SELECT
    a.author_name,
    COUNT(id_quote) AS total_quotes
FROM quotes AS q
INNER JOIN authors as a ON q.id_author = a.id_author
GROUP BY a.author_name
ORDER BY total_quotes DESC;

-- 3. Une requête avec fonction de fenêtrage (window function)

-- Nombre de tags par auteur
CREATE OR REPLACE VIEW v_quotes_tags AS
SELECT
    q.text,
    a.author_name,
    t.tag_name
FROM quotes_tags AS qt
INNER JOIN tags AS t ON qt.id_tag = t.id_tag
INNER JOIN quotes AS q ON qt.id_quote = q.id_quote
INNER JOIN authors AS a ON q.id_author = a.id_author;

SELECT
    author_name,
    COUNT(tag_name) AS total_tag
FROM v_quotes_tags
GROUP BY author_name
ORDER BY total_tag DESC;

-- 4. Une requête de classement (top N)

-- Top 3 des auteurs les plus prolifiques
SELECT
    a.author_name,
    COUNT(id_quote) AS total_quotes
FROM quotes AS q
INNER JOIN authors as a ON q.id_author = a.id_author
GROUP BY a.author_name
ORDER BY total_quotes DESC
LIMIT 3;

-- 5. Une requête croisant au moins 2 sources de données

-- Nombre de livre dans le librairie 'Librairie du Marais'.
SELECT
    COUNT(b.id_book) AS total_book,
    l.nom_librairie
FROM books AS b, librairies AS l
WHERE l.nom_librairie = 'Librairie du Marais' AND b.id_book <= 10
GROUP BY l.nom_librairie;