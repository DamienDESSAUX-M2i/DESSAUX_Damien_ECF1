import datetime
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
                "INSERT INTO authors (id_author, author_name) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING *;",
                (id_author, author_name),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_authors: {result}")
            return result
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
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_author: {result}")
            return result
        except Exception as e:
            logger.error(f"update_author_failed: {e}")
            return None

    def delete_author(self, id_author: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM authors WHERE id_author=%s RETURNING *;",
                (id_author,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_author: {result}")
            return result
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
                "INSERT INTO tags (id_tag, tag_name) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING *;",
                (id_tag, tag_name),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_tags: {result}")
            return result
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
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_tag: {result}")
            return result
        except Exception as e:
            logger.error(f"update_tag_failed: {e}")
            return None

    def delete_tag(self, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM tags WHERE id_tag=%s RETURNING *;",
                (id_tag,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_tag: {result}")
            return result
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
                "INSERT INTO quotes (id_quote, text, id_author) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING RETURNING *;",
                (id_quote, text, id_author),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_quotes: {result}")
            return result
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
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_quote: {result}")
            return result
        except Exception as e:
            logger.error(f"update_quote_failed: {e}")
            return None

    def delete_quote(self, id_quote: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM quotes WHERE id_quote=%s RETURNING *;",
                (id_quote,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_quote: {result}")
            return result
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
                "INSERT INTO quotes_tags (id_quote, id_tag) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING *;",
                (id_quote, id_tag),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_quotes_tags: {result}")
            return result
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
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_quote_tag: {result}")
            return result
        except Exception as e:
            logger.error(f"update_quote_tag_failed: {e}")
            return None

    def delete_quote_tag(self, id_quote: int, id_tag: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM quotes_tags WHERE id_quote=%s AND id_tag=%s RETURNING *;",
                (id_quote, id_tag),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_quote_tag: {result}")
            return result
        except Exception as e:
            logger.error(f"delete_quote_tag_failed: {e}")
            return None

    # LIBRAIRIES - librairies

    def select_librairie(self, id_librairie: int) -> list[dict] | None:
        try:
            self.cursor.execute(
                """
                SELECT *
                FROM librairies
                WHERE qt.id_librairie=%s;
                """,
                (id_librairie,),
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"select_librairie_failed: {e}")
            return None

    def insert_into_librairies(
        self,
        nom_librairie: str,
        adresse: str,
        code_postal: str,
        ville: str,
        contact_initiales: str,
        contact_email: str,
        contact_telephone: str,
        date_partenariat: datetime,
        specialite: str,
        latitude: float,
        longitude: float,
    ) -> dict | None:
        try:
            self.cursor.execute(
                """
                INSERT INTO librairies (nom_librairie, adresse, code_postal, ville, contact_initiales, contact_email, contact_telephone, date_partenariat, specialite, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id_librairie;
                """,
                (
                    nom_librairie,
                    adresse,
                    code_postal,
                    ville,
                    contact_initiales,
                    contact_email,
                    contact_telephone,
                    date_partenariat,
                    specialite,
                    latitude,
                    longitude,
                ),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_librairies: {result}")
            return result
        except Exception as e:
            logger.error(f"insert_into_librairies_failed: {e}")
            return None

    def update_librairie(
        self,
        id_librairie: int,
        nom_librairie: str,
        adresse: str,
        code_postal: str,
        ville: str,
        contact_initiales: str,
        contact_email: str,
        contact_telephone: str,
        date_partenariat: datetime,
        specialite: str,
        latitude: float,
        longitude: float,
    ) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE librairies
                SET nom_librairie=%s, adresse=%s, code_postal=%s, ville=%s, contact_initiales=%s, contact_email=%s, contact_telephone=%s, date_partenariat=%s, specialite=%s, latitude=%s, longitude=%s
                WHERE id_librairie=%s
                RETURNING *;
                """,
                (
                    nom_librairie,
                    adresse,
                    code_postal,
                    ville,
                    contact_initiales,
                    contact_email,
                    contact_telephone,
                    date_partenariat,
                    specialite,
                    id_librairie,
                    latitude,
                    longitude,
                ),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_librairie: {result}")
            return result
        except Exception as e:
            logger.error(f"update_librairie_failed: {e}")
            return None

    def delete_librairie(self, id_librairie: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM librairies WHERE id_librairie=%s RETURNING *;",
                (id_librairie,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_librairie: {result}")
            return result
        except Exception as e:
            logger.error(f"delete_librairie_failed: {e}")
            return None

    # LIBRAIRIES - ca_annuel

    def select_ca_annuel(self, id_ca_annuel: int) -> list[dict] | None:
        try:
            self.cursor.execute(
                """
                SELECT
                    l.nom_librairie,
                    l.adresse,
                    l.code_postal,
                    l.ville,
                    l.contact_initiales,
                    l.contact_email,
                    l.contact_telephone,
                    l.date_partenariat,
                    l.specialite,
                    c.ca_annuel
                FROM ca_annuel AS ca
                INNER JOIN librairies AS l ON ca.id_ca_annuel = l.id_ca_annuel
                WHERE qt.id_ca_annuel=%s;
                """,
                (id_ca_annuel,),
            )
            return list(self.cursor.fetchall())
        except Exception as e:
            logger.error(f"select_ca_annuel_failed: {e}")
            return None

    def insert_into_ca_annuel(self, ca_annuel: float, id_librairie: int) -> dict | None:
        try:
            self.cursor.execute(
                """
                INSERT INTO ca_annuel (ca_annuel, id_librairie)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id_ca_annuel;
                """,
                (ca_annuel, id_librairie),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_ca_annuel: {result}")
            return result
        except Exception as e:
            logger.error(f"insert_into_ca_annuel_failed: {e}")
            return None

    def update_ca_annuel(
        self, id_ca_annuel: int, ca_annuel: float, id_librairie
    ) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE ca_annuel
                SET ca_annuel=%s, id_librairie=%s
                WHERE id_ca_annuel=%s
                RETURNING *;
                """,
                (id_ca_annuel, ca_annuel, id_librairie),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_ca_annuel: {result}")
            return result
        except Exception as e:
            logger.error(f"update_ca_annuel_failed: {e}")
            return None

    def delete_ca_annuel(self, id_ca_annuel: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM ca_annuel WHERE id_ca_annuel=%s RETURNING *;",
                (id_ca_annuel,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_ca_annuel: {result}")
            return result
        except Exception as e:
            logger.error(f"delete_ca_annuel_failed: {e}")
            return None

    # BOOKS

    def select_book(self, id_book: int) -> dict | None:
        try:
            self.cursor.execute(
                "SELECT * FROM books WHERE id_book=%s;",
                (id_book,),
            )
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"select_author_failed: {e}")
            return None

    def insert_into_books(
        self,
        title: str,
        price: float,
        rating: int,
        availability: bool,
        category: str,
        url_image: str,
    ) -> dict | None:
        try:
            self.cursor.execute(
                "INSERT INTO books (title, price, rating, availability, category, url_image) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING *;",
                (title, price, rating, availability, category, url_image),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"insert_into_books: {result}")
            return result
        except Exception as e:
            logger.error(f"insert_into_books_failed: {e}")
            return None

    def update_book(
        self,
        id_book: int,
        title: str,
        price: float,
        rating: int,
        availability: bool,
        category: str,
        url_image: str,
    ) -> dict | None:
        try:
            self.cursor.execute(
                """
                UPDATE books
                SET title=%s, price=%s, rating=%s, availability=%s, category=%s, url_image=%s
                WHERE id_book=%s
                RETURNING *;
                """,
                (id_book, title, price, rating, availability, category, url_image),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"update_book: {result}")
            return result
        except Exception as e:
            logger.error(f"update_book_failed: {e}")
            return None

    def delete_book(self, id_book: int) -> dict | None:
        try:
            self.cursor.execute(
                "DELETE FROM books WHERE id_book=%s RETURNING *;",
                (id_book,),
            )
            self.connection.commit()
            result = self.cursor.fetchone()
            logger.debug(f"delete_book: {result}")
            return result
        except Exception as e:
            logger.error(f"delete_book_failed: {e}")
            return None

    # UTILIS

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
