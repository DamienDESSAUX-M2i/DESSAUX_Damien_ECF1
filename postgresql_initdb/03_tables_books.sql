CREATE TABLE IF NOT EXISTS books(
   id_book SERIAL PRIMARY KEY,
   title VARCHAR(255),
   price DECIMAL(10,2),
   rating INTEGER,
   availability BOOLEAN,
   category VARCHAR(255),
   url_image VARCHAR(255)
);