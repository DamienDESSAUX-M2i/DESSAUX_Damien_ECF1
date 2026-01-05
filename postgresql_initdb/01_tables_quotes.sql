-- ===
-- Quotes
-- ===

CREATE TABLE IF NOT EXISTS authors (
	id_author INTEGER PRIMARY KEY,
	author_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS quotes (
	id_quote INTEGER PRIMARY KEY,
	text TEXT NOT NULL,
    id_author INTEGER NOT NULL,
    CONSTRAINT fk_id_author FOREIGN KEY (id_author) REFERENCES authors(id_author) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags (
	id_tag INTEGER PRIMARY KEY,
	tag_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS quotes_tags (
	id_quote INTEGER NOT NULL,
    id_tag INTEGER NOT NULL,
	CONSTRAINT pk_quotes_tags PRIMARY KEY (id_quote, id_tag),
    CONSTRAINT fk_id_quote FOREIGN KEY (id_quote) REFERENCES quotes(id_quote) ON DELETE CASCADE,
    CONSTRAINT fk_id_tag FOREIGN KEY (id_tag) REFERENCES tags(id_tag) ON DELETE CASCADE
);