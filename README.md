# ECE 2026 - Big Data - labs

Sommaire : [TP3 - object storage](#tp3---cloud-storage-et-object-storage) · [TP4 - SQL analytics avec DuckDB](#tp4---sql-analytics-avec-duckdb) · [TP5 - bronze vers silver avec dbt](#tp5---bronze-vers-silver-avec-dbt)

## TP3 - Cloud storage et object storage

Arthur Benatar - ING5 groupe 1

Tout a été fait dans un service vscode-pyspark sur Onyxia (Adaltas), avec mon bucket `user-a-benatar-ece` sur l'endpoint SeaweedFS https://s3.seaweedfs.adm.adaltas.cloud

### Lab 1 - génération des datasets

Projet Python géré avec uv, nom du projet `ece-2026-bigdata`, package `ece_2026_bigdata`.

Fichiers principaux :
- `serialize.py` : fonctions pour écrire les données en csv
- `dataset_users.py` : génère les users avec Faker
- `dataset_orders.py` : génère les commandes liées aux users

Pour lancer :

```
uv sync
uv run dataset-users
uv run dataset-orders
```

J'ai mis une seed à 42 sur Faker pour que les données soient les mêmes à chaque exécution, sinon c'est pénible pour comparer les fichiers entre deux runs.

### Lab 2 - object storage S3

Opérations faites sur le bucket : upload, list, download, mv, delete, ajout de métadonnées, presigned URL, upload multipart d'un fichier de 200 MiB, versioning et lifecycle.

Quelques remarques sur ce qui s'est passé :

Au début j'avais NoSuchBucket et AccessDenied sur le PutObject, en fait le bucket n'existait pas encore. Il est créé quand on ouvre l'explorateur de fichiers dans Onyxia, après ça tout marchait.

ETag vs md5sum : sur `users.csv` envoyé en upload simple, l'ETag renvoyé par head-object est `528045479cf87293c3453ab20ae9028b`, exactement le même que le `md5sum users.csv`. Par contre le fichier de 200 MiB est parti en multipart (l'aws cli découpe en parts de 8 MiB), et là l'ETag finit par `-25` : c'est un md5 calculé à partir des md5 des 25 parts, pas le md5 du fichier. Donc on ne peut pas utiliser l'ETag comme checksum dès qu'il y a du multipart.

Bucket policy : j'ai mis une policy Deny sur s3:DeleteObject pour `bronze/*`, et la suppression est bien refusée (AccessDenied) donc la policy est appliquée. Par contre `delete-bucket-policy` renvoie une InternalError, j'imagine que SeaweedFS n'implémente pas complètement cette partie de l'API S3, ce n'est pas un vrai AWS donc tout n'est pas forcément supporté.

Consistency : la section s'est terminée avec un code 2, à priori à cause du `cp` vers `/dev/null` qui n'est pas supporté par le client et pas un problème de cohérence. S3 (et SeaweedFS) est en read-after-write consistent, donc un objet qu'on vient d'écrire est lisible directement.

Job Kubernetes : le pod ne se créait pas, erreur `failed quota: onyxia-quota: must specify limits.cpu`. Le namespace Onyxia a un quota qui oblige à mettre des limits, j'ai donc ajouté `limits.cpu: 500m` dans `job-upload-bronze.yaml`. Après ça le job passe et dans `bronze/` on retrouve `users.csv` (7351 octets) et `orders.csv` (331568 octets). J'ai supprimé les objets Kubernetes à la fin mais j'ai gardé les données dans bronze pour la suite.

### Questions

1. Pourquoi les credentials sont dans un Secret et pas dans la ConfigMap ?

La ConfigMap c'est pour de la config non sensible (endpoint, nom du bucket, préfixe...), elle est lisible en clair par tous ceux qui ont accès au namespace et finit souvent dans git. Un Secret est fait pour les données sensibles : on peut restreindre son accès avec le RBAC séparément de la ConfigMap, il n'est pas affiché par défaut dans les describe, et il peut être chiffré au repos dans etcd. Attention quand même, par défaut c'est juste du base64 donc ce n'est pas du chiffrement, ça reste mieux que de mettre les clés en dur dans la ConfigMap.

2. Les credentials Onyxia sont temporaires, que se passe-t-il si on relance le job demain ? Comment on fait en prod ?

Les credentials S3 d'Onyxia ont une durée de vie limitée (token STS). Si on relance le job demain avec le même Secret, les clés auront expiré et le job va planter avec une erreur d'accès (genre InvalidAccessKeyId / ExpiredToken), il faudrait recréer le Secret à la main avec de nouvelles clés. En prod on ne met pas des clés à la main dans un Secret, on utilise plutôt un compte de service avec une identité fournie par la plateforme (IRSA sur AWS, Workload Identity sur GCP...) qui permet au pod de récupérer des credentials temporaires renouvelés automatiquement, ou bien un gestionnaire de secrets type Vault / External Secrets qui fait la rotation et injecte les secrets dans le pod.

3. Comment transformer ce job en ingestion quotidienne ?

Le plus simple est de passer du Job à un CronJob Kubernetes avec un schedule genre `0 2 * * *` pour tourner tous les jours à 2h. Il faut aussi que chaque run écrive dans un chemin daté (par exemple `bronze/users/date=2026-10-06/users.csv`) pour ne pas écraser les données de la veille et pouvoir rejouer un jour précis. Il faut penser à `concurrencyPolicy: Forbid` pour éviter que deux runs se chevauchent, à garder un historique des jobs (successfulJobsHistoryLimit / failedJobsHistoryLimit) et à régler le problème des credentials de la question 2. Si le pipeline grossit, un orchestrateur comme Airflow ou Argo Workflows serait plus adapté pour gérer les dépendances, les retries et le monitoring.

## TP4 - SQL analytics avec DuckDB

Fait dans le même service vscode-pyspark sur Onyxia, DuckDB v1.5.5 (préinstallé dans l'image), sur les fichiers `bronze/users.csv` et `bronze/orders.csv` de mon bucket.

Ce qui a été ajouté au projet :
- `src/ece_2026_bigdata/orders_report.py` + la commande `orders-report` dans `pyproject.toml` (rapport mensuel, filtre `-p` sur un produit)
- `sql/tp4_exercises.sql` : mes requêtes pour les 5 exercices
- `*.duckdb` et `init.sql` dans le `.gitignore` (ce n'est pas du code)

```
uv run orders-report            # 2020-01 744 2297 / 2020-02 696 2154 / 2020-03 744 2192 / 2020-04 645 1972
uv run orders-report -p cookie  # 2020-01 128 376 / 2020-02 121 371 / ...
duckdb analytics.duckdb -init init.sql < sql/tp4_exercises.sql
```

### Remarques

- Le service tournait depuis la veille et les credentials S3 d'Onyxia avaient expiré (ils durent 12h). Le secret persistant `s3_onyxia_connection` garde les valeurs du démarrage, donc il a fallu le recréer avec des credentials neufs (`CREATE OR REPLACE PERSISTENT SECRET ...`), sinon toutes les lectures S3 tombent en 403.
- `s5cmd` n'est pas dans mon image vscode-pyspark, j'ai utilisé `aws s3 cp` pour envoyer le gros fichier (multipart automatique, 28 MiB en 2 s).
- Mon générateur ne donne pas exactement les chiffres de l'énoncé pour le gros dataset : 252 416 commandes et 29 Mo de CSV au lieu de 54 Mo. Les dates vont de 2020 à 2048 (une commande par heure), donc `date >= '2100-01-01'` ne renvoie rien.
- Contrôles qualité : 0 commande orpheline, 0 user sans commande, 0 doublon d'uuid.

### Exercices

1. 56,58 commandes par user en moyenne (2829 / 50) et 3,05 de quantité moyenne par commande.
2. `min`/`max` de la date par user + `date_diff('day', ...)`. Les commandes d'un user sont regroupées sur quelques jours (le générateur enchaîne les users), l'écart max est de 5 jours (hoganashlee) et 6 users ont tout commandé le même jour.
3. 6h avec 385 articles, devant 15h (382) et minuit (379). Les écarts sont minuscules, c'est le hasard du générateur, pas une vraie heure de pointe.
4. `lag(quantity) OVER (PARTITION BY product ORDER BY month)` sur les quantités mensuelles. Par exemple donut fait -28,3 % en février, bread +14,9 % en février puis -19,2 % en avril. Avril baisse souvent parce que les données s'arrêtent le 27.
5. 45 users sur 50 ont commandé les 6 produits (`HAVING count(DISTINCT product) = 6`).

### CSV vs Parquet

| | données lues | GET | temps |
|---|---|---|---|
| CSV, `sum(quantity)` par produit | 28,2 MiB | 1 | 1,10 s |
| Parquet, même requête | 203,5 KiB | 4 | 0,14 s |
| Parquet, `count(*) WHERE date >= '2100-01-01'` | 16 KiB | 1 | 0,18 s |

Le CSV oblige à tout télécharger pour lire 2 colonnes, le Parquet lit le footer puis seulement les colonnes `product` et `quantity`. Le gros fichier fait 11,5 Mo en Parquet contre 29,6 Mo en CSV, et le petit `orders` 130 Ko contre 332 Ko. Le filtre sur 2100 ne lit que le footer : les stats min/max des 3 row groups montrent qu'aucun ne peut contenir de date après 2048.

Les réponses aux questions du lab sont dans mon rendu envoyé par mail.

## TP5 - Bronze vers silver avec dbt

Toujours sur Onyxia. dbt-core 1.12.5 + dbt-duckdb 1.11.0 ajoutés au projet uv, projet dbt dans `lab_medallion/`. DuckDB lit les CSV du bronze directement sur S3 avec le secret `s3_onyxia_connection` du TP4.

```
cd lab_medallion
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
uv run dbt build        # seeds + modèles silver + tests
uv run dbt docs generate --static
```

Contenu :
- `models/bronze/sources.yml` : `users` et `orders` déclarés comme sources, `external_location` = `read_csv` sur `s3://<bucket>/bronze/{name}.csv`
- `models/silver/stg_users.sql`, `stg_orders.sql` : typage, renommage, découpage de l'adresse, dédoublonnage des commandes
- `models/silver/silver.yml` : documentation et tests génériques
- `tests/` : tests singuliers, et `tests/generic/test_positive.sql` (test générique maison)
- `seeds/states.csv` (codes des états, territoires et zones militaires) et `seeds/state_zip_ranges.csv` (plages de ZIP par état)

### Ce qui s'est passé

- `dbt test` : les 13 tests génériques passent, ainsi que `assert_orders_have_users`. `--store-failures` crée bien une table par test dans le schéma `main_dbt_test__audit`.
- `assert_orders_after_user_birth` échoue avec **288 commandes** : 5 users sont nés après leur première commande (par exemple dayderek né le 2026-10-02 alors que ses commandes datent de février 2020). Faker génère les dates de naissance par rapport à aujourd'hui alors que les commandes sont en 2020. Passé en `severity='warn'` comme demandé, puis corrigé dans l'exercice 3.
- `dbt build --select silver` sur une base vide plante sur le test `relationships` vers `states` : le seed n'est pas dans la sélection, donc la table n'existe pas. Il faut lancer `dbt build` tout court (ou `dbt seed` avant).
- `dbt ls --select +stg_orders` donne bien `source:lab_medallion.bronze.orders` + `stg_orders`.
- Export Parquet : `silver/dbt/users.parquet` (7 Ko) et `silver/dbt/orders.parquet` (72 Ko). La comparaison avec la vue (`EXCEPT ALL` dans les deux sens) donne 2829 lignes des deux côtés et 0 différence.
- Le `.gitignore` du TP3 ignore `*.csv`, j'ai dû ajouter une exception pour `lab_medallion/seeds/*.csv` sinon les seeds n'étaient pas commités.

### Exercices

1. **`username_normalized`** = `lower(trim(username))`, documentée, tests `unique` + `not_null`.
2. **Tests des commandes** : `quantity` avec `not_null`, mon test générique `positive` et `accepted_values` 1 à 5, `product` avec `not_null` + `accepted_values`, `user_id` avec `relationships` vers `stg_users` (générique) en plus du test singulier `assert_orders_have_users`. Tout passe.
3. **Dates de naissance invalides** : `stg_users` calcule la première commande de chaque user depuis la source `orders`, `birthdate_is_valid` vaut false si la naissance est après cette commande, et dans ce cas `birthdate` est mise à NULL. 5 users sur 50 sont concernés. Le test `assert_orders_after_user_birth` filtre sur `birthdate_is_valid` et repasse en erreur bloquante : il passe.
4. **ZIP incohérents** : seed `state_zip_ranges` (préfixes ZIP à 3 chiffres de l'USPS pour chaque état, territoire et zone militaire AA/AE/AP) et test singulier `assert_users_zip_matches_state` en `warn`. Résultat : **49 users sur 50** ont un ZIP qui ne correspond pas à leur état (ex : TX 55834, alors que 55xxx c'est le Minnesota), seul 1 tombe juste par hasard. Faker tire l'état et le ZIP indépendamment.
   - La donnée de référence devrait venir d'une source officielle (USPS ou les ZCTA du Census), versionnée. En seed c'est bien parce qu'elle est petite (71 lignes) et change rarement ; pour une vraie table ZIP complète (~40 000 codes) je la chargerais plutôt comme une source à part.
   - La validation a sa place dans le silver (c'est là qu'on nettoie), en warning pour ne pas bloquer le pipeline, ou avec une colonne `zip_is_valid` comme pour la date de naissance.
   - Si la référence change, il suffit de relancer `dbt seed` + `dbt build` : comme le bronze n'est jamais modifié, tout le silver se recalcule.
5. **Vue vs table** (testé puis remis en `view`) :

| | view | table |
|---|---|---|
| taille de `lab_medallion.duckdb` | 0,8 Mo | 1,8 Mo |
| `SELECT count(*) FROM main_silver.stg_orders` | 0,39 s | 0,000 s |
| transformation exécutée | à chaque requête | au `dbt run` |
| si le CSV change | visible tout de suite | visible seulement après un nouveau `dbt run` |

La vue ne stocke que la requête, donc chaque lecture retélécharge et reparse le CSV sur S3. La table stocke le résultat dans le fichier DuckDB (plus de place) mais les lectures ne touchent plus au bronze.

### Questions du lab

**Le bronze n'est jamais modifié. Quel intérêt si on trouve un bug dans une transformation six mois plus tard ?** On corrige le modèle et on relance `dbt build` : tout le silver est recalculé depuis les données brutes d'origine. Si on avait écrasé le bronze avec les données transformées, les données d'origine seraient perdues et le bug impossible à rattraper.

**La source relit tout le fichier à chaque requête. Quel format et quelle organisation si les commandes arrivaient toutes les heures pendant des années ?** Du Parquet partitionné par date (`date=2026-10-07/` voire `/hour=08/`), ou une table Iceberg qui gère les partitions et les métadonnées. Chaque ingestion ajoute des fichiers sans réécrire l'existant, les requêtes ne lisent que les partitions utiles, et côté dbt un modèle `incremental` ne traiterait que les nouvelles partitions.

**Les modèles silver sont des vues. Que se passe-t-il quand on interroge une vue, et quand est-ce préférable à une table ?** La requête de la vue est exécutée à chaque fois, ici ça veut dire relire les CSV sur S3. C'est préférable quand les données sont petites, peu interrogées ou qu'on veut toujours la dernière version de la source sans relancer dbt, et ça ne prend pas de place. Dès que c'est lu souvent ou que c'est lourd à calculer, une table est mieux.

**Un UUID est stocké sur 16 octets. Combien en `VARCHAR` ?** 36 caractères (32 hexadécimaux + 4 tirets), donc 36 octets, plus de 2 fois plus. Et dans DuckDB une chaîne de plus de 12 caractères n'est pas stockée en ligne, il faut en plus un en-tête de 16 octets avec un pointeur.

**Quelle stratégie pour un dashboard sur l'âge des clients ? Et pour les ventes par région ?** Pour l'âge, mettre les dates invalides à NULL (et les exclure du calcul) : un âge faux fausserait le dashboard et on ne doit pas inventer de valeur. Pour les ventes par région, surtout ne pas rejeter les users concernés : leur date de naissance n'a rien à voir avec les ventes et on perdrait leurs commandes. C'est pour ça que j'ai gardé les lignes avec `birthdate` à NULL + `birthdate_is_valid` plutôt que de les supprimer.

**Les adresses associent des états à des ZIP aléatoires (`KY 01352`). Quel test le détecterait, avec quelles données de référence ?** Un test qui vérifie que le ZIP est dans une des plages de son état, avec une table de référence état → plages de ZIP (USPS / Census). C'est ce que j'ai fait dans l'exercice 4 : `KY 01352` est bien détecté (01xxx c'est le Massachusetts).
