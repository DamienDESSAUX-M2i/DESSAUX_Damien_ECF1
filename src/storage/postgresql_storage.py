import logging

import psycopg

from config.settings import postgresql_config

logger = logging.getLogger("app")


class PostgreSQLStorage:
    def __init__(self):
        self.connection = psycopg.connect(
            postgresql_config.connection_string, row_factory=psycopg.rows.dict_row
        )
        self.cursor = self.connection.cursor()

    # QUOTES - authors

    def select_author(self, id_author: int) -> dict | None:
        try:
            self.cursor.execute(
                "SELECT author_name FROM authors WHERE id_author=%s;",
                (id_author,),
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"select_author_failed: {e}")
            return None

    def insert_into_authors(self, id_author: int, author_name: str) -> dict | None:
        try:
            self.cursor.execute(
                "INSERT INTO authors (id_author, author_name) VALUES (%s, %s) RETURNING *;",
                (id_author, author_name),
            )
            logger.info(f"insert_into_authors: {id_author = }, {author_name = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"insert_into_authors_failed: {e}")
            return None

    def update_author(self, id_author: int, author_name: str) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE authors
                SET author_name=%s
                WHERE id_author=%s
                RETURNING *;
                """,
                (author_name, id_author),
            )
            logger.info(f"update_author: {id_author = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"update_author_failed: {e}")
            return None

    def delete_author(self, id_author: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM authors WHERE id_author=%s RETURNING *;",
                (id_author,),
            )
            logger.info(f"delete_author: {id_author = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"delete_author_failed: {e}")
            return None

    # QUOTES - tags

    def select_tag(self, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "SELECT tag_name FROM tags WHERE id_tag=%s;",
                (id_tag,),
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"select_tag_failed: {e}")
            return None

    def insert_into_tags(self, id_tag: int, tag_name: str) -> dict | None:
        try:
            self.cursor.execute(
                "INSERT INTO tags (id_tag, tag_name) VALUES (%s, %s) RETURNING *;",
                (id_tag, tag_name),
            )
            logger.info(f"insert_into_tags: {id_tag = }, {tag_name = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"insert_into_tags_failed: {e}")
            return None

    def update_tag(self, id_tag: int, tag_name: str) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE tags
                SET tag_name=%s
                WHERE id_tag=%s
                RETURNING *;
                """,
                (tag_name, id_tag),
            )
            logger.info(f"update_tag: {id_tag = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"update_tag_failed: {e}")
            return None

    def delete_tag(self, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM tags WHERE id_tag=%s RETURNING *;",
                (id_tag,),
            )
            logger.info(f"delete_tag: {id_tag = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"delete_tag_failed: {e}")
            return None

    # QUOTES - quotes

    def select_quote(self, id_quote: int) -> dict | None:
        try:
            self.cursor.execute(
                """
                SELECT
                    q.text,
                    a.author_name
                FROM quotes AS q
                LEFT JOIN authors AS a
                ON q.id_author = a.id_author
                WHERE id_quote=%s;
                """,
                (id_quote,),
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"select_quote_failed: {e}")
            return None

    def insert_into_quotes(
        self, id_quote: int, text: str, id_author: int
    ) -> dict | None:
        try:
            self.cursor.execute(
                "INSERT INTO quotes (id_quote, text, id_author) VALUES (%s, %s, %s) RETURNING *;",
                (id_quote, text, id_author),
            )
            logger.info(f"insert_into_quotes: {id_quote = }, {text = }, {id_author = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"insert_into_quotes_failed: {e}")
            return None

    def update_quote(self, id_quote: int, text: str, id_author: int) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE quotes
                SET text=%s, id_author=%s
                WHERE id_quote=%s
                RETURNING *;
                """,
                (text, id_author, id_quote),
            )
            logger.info(f"update_quote: {id_author = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"update_quote_failed: {e}")
            return None

    def delete_quote(self, id_quote: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM quotes WHERE id_quote=%s RETURNING *;",
                (id_quote,),
            )
            logger.info(f"delete_quote: {id_quote = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"delete_quote_failed: {e}")
            return None

    # QUOTES - quotes_tags

    def select_quote_tags(self, id_quote: int) -> list[dict] | None:
        try:
            self.cursor.execute(
                """
                SELECT
                    q.text,
                    a.author_name,
                    t.tag_name
                FROM quotes_tags AS qt
                INNER JOIN tags AS t ON qt.id_tag = t.id_tag
                INNER JOIN quotes AS q ON qt.id_quote = q.id_quote
                INNER JOIN authors AS a ON q.id_author = a.id_author
                WHERE qt.id_quote=%s;
                """,
                (id_quote,),
            )
            return list(self.cursor.fetchall())
        except Exception as e:
            logger.error(f"select_quote_tags_failed: {e}")
            return None

    def insert_into_quotes_tags(self, id_quote: int, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "INSERT INTO quotes_tags (id_quote, id_tag) VALUES (%s, %s) RETURNING *;",
                (id_quote, id_tag),
            )
            logger.info(f"insert_into_quotes_tags: {id_quote = }, {id_tag = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"insert_into_quotes_failed: {e}")
            return None

    def update_quote_tag(
        self, id_quote: int, id_tag: int, new_id_quote: int, new_id_tag: int
    ) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE quotes_tags
                SET id_quote=%s, id_tag=%s
                WHERE id_quote=%s AND id_tag=%s
                RETURNING *;
                """,
                (new_id_quote, new_id_tag, id_quote, id_tag),
            )
            logger.info(f"update_quote: {new_id_quote = }, {new_id_tag = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"update_quote_failed: {e}")
            return None

    def delete_quote_tag(self, id_quote: int, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM quotes_tags WHERE id_quote=%s AND id_tag=%s RETURNING *;",
                (id_quote, id_tag),
            )
            logger.info(f"delete_quote: {id_quote = }, {id_tag = }")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"delete_quote_failed: {e}")
            return None

    def close(self) -> None:
        """Ferme la connexion MongoDB."""
        self.cursor.close()
        self.connection.close()
        logger.info("postgresql_connection_closed")


# Test du module
if __name__ == "__main__":
    print("Test du client MongoDB...")

    postgresql_storage = PostgreSQLStorage()

    # Test insertion
    print("Test insertion")
    postgresql_storage.insert_into_authors(id_author=1, author_name="Albert Einstein")
    postgresql_storage.insert_into_quotes(
        id_quote=1,
        text="The world as we have created it is a process of our thinking. It cannot be changed without changing our thinking.",
        id_author=1,
    )
    postgresql_storage.insert_into_tags(id_tag=1, tag_name="change")
    postgresql_storage.insert_into_tags(id_tag=2, tag_name="deep-thoughts")
    postgresql_storage.insert_into_tags(id_tag=3, tag_name="thinking")
    postgresql_storage.insert_into_tags(id_tag=4, tag_name="world")
    postgresql_storage.insert_into_quotes_tags(id_quote=1, id_tag=1)
    postgresql_storage.insert_into_quotes_tags(id_quote=1, id_tag=2)
    postgresql_storage.insert_into_quotes_tags(id_quote=1, id_tag=3)
    postgresql_storage.insert_into_quotes_tags(id_quote=1, id_tag=4)

    # Test selection
    print("Test selection")
    print(postgresql_storage.select_author(id_author=1))
    print(postgresql_storage.select_tag(id_tag=1))
    print(postgresql_storage.select_quote(id_quote=1))
    print(postgresql_storage.select_quote_tags(id_quote=1))

    # Test deleting
    print("Test deleting")
    print(postgresql_storage.delete_author(id_author=1))
    print(postgresql_storage.delete_tag(id_tag=1))
    print(postgresql_storage.delete_tag(id_tag=2))
    print(postgresql_storage.delete_tag(id_tag=3))
    print(postgresql_storage.delete_tag(id_tag=4))
    print(postgresql_storage.select_quote(id_quote=1) is None)

    # Cleanup
    postgresql_storage.close()
    print("Tests terminés!")
