# Livrable 1.1 : Dossier d'Architecture Technique (DAT)

**1. Choix d'architecture globale**
- Quelle architecture proposez-vous ? (Data Lake, Data Warehouse, Lakehouse, base NoSQL, autre ?)

Pour le sctockage des images, données bruts et des backups j'utiliserai `MinIO`.
Pour le stockage des données structurées j'utiliserai `PostgreSQL`.

- Pourquoi ce choix plutôt qu'une alternative ?

`MinIO` est basé sur un stockage objet qui permet de stocker tout type de fichier. Il propose également l'ajout de métadata pour enrichir les données.
`PostgreSQL` est un SGBDR idéal pour les données structurées.

- Quels sont les avantages et inconvénients de votre choix ?

Technologie | Avantage | Incovénient
:- | :- | :-
`MinIO` | Stockage object, métadata, on premise, gratuit | Analyse limité
`PostgreSQL` | Système de requête complet permettant une analyse fine des données | Données structurées

**2. Choix des technologies**
- Quelles technologies utilisez-vous pour le stockage des données brutes ? Justifiez.

Pour le stockage des données brut, j'utilise `MinIO` qui est basé sur un stockage objet permettant de stocker tout format de fichier.

- Quelles technologies utilisez-vous pour les données transformées ? Justifiez.

Pour les données tansformées, j'utiliserai `PostgreSQL` qui permet de stocker des données structurées.

- Quelles technologies utilisez-vous pour l'interrogation SQL ? Justifiez.

Pour l'interrogation SQL j'utiliserai python avec la bibliothèque `psycopg`.

- Comparez avec au moins une alternative pour chaque choix.

J'aurais pu utiliser AWS S3 à la place de MinIO. AWS S3 n'est pas on-premise.
J'aurais pu utiliser MongoDB à la place de PostgreSQL. MongoDB est une solution No-SQL qui propose un stockage document correspondant au données non-structurées. 

**3. Organisation des données**
- Comment organisez-vous les données dans votre architecture ?



- Proposez-vous des couches de transformation ? Lesquelles et pourquoi ?



- Quelle convention de nommage adoptez-vous ?




**4. Modélisation des données**
- Quel modèle de données proposez-vous pour la couche finale ?
- Fournissez un schéma (diagramme entité-relation ou autre)
- Justifiez vos choix de modélisation

**5. Conformité RGPD**
- Quelles données personnelles identifiez-vous dans les sources ?
- Quelles mesures de protection proposez-vous ?
- Comment gérez-vous le droit à l'effacement ?
