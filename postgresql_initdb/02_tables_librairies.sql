CREATE TABLE IF NOT EXISTS librairies(
   id_librairie INTEGER PRIMARY KEY,
   nom_librairie VARCHAR(255),
   adresse VARCHAR(255),
   code_postal VARCHAR(255),
   ville VARCHAR(255),
   contact_initiales VARCHAR(255),
   contact_email VARCHAR(255),
   contact_telephone VARCHAR(255),
   date_partenariat DATE,
   specialite VARCHAR(255),
   latitude FLOAT,
   longitude FLOAT
);

CREATE TABLE IF NOT EXISTS ca_annuel(
   id_ca_annuel INTEGER PRIMARY KEY,
   ca_annuel DECIMAL(10,2) CHECK (ca_annuel >= 0),
   id_librairie INT NOT NULL,
   CONSTRAINT fk_id_librairie FOREIGN KEY (id_librairie) REFERENCES librairies(id_librairie) ON DELETE CASCADE
);
