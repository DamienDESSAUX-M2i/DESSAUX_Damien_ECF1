# Livrable 2.4 : Documentation RGPD

- Inventaire des données personnelles collectées

Les données personnelles collectées sont : le nom de contact, l'email de contact et le numéro de téléphone de contact.

- Base légale du traitement pour chaque donnée

Anonimisation des données.

- Mesures de protection mises en œuvre

Anonimisation du nom de contact en gardant uniquement les initiales. Anonimisation de l'email de contact et du téléphone de contact en hashant les données.

- Procédure de suppression sur demande

**PostgreSQL**

Suppression des colonnes `contact_initiales`, `contact_email` et `contact_telephone` de la table `librairies`.

```sql
ALTER TABLE librairies DROP COLUMN contact_initiales, contact_email, contact_telephone;
```

**MinIO**

Modification des fichiers d'exports `exports/librairies_*.json`. Suppression des champs `contact_initiales`, `contact_email` et `contact_telephone`.