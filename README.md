# ECE 2026 - Big Data - TP3 Cloud storage et object storage

Arthur Benatar - ING5 groupe 1

Tout a été fait dans un service vscode-pyspark sur Onyxia (Adaltas), avec mon bucket `user-a-benatar-ece` sur l'endpoint SeaweedFS https://s3.seaweedfs.adm.adaltas.cloud

## Lab 1 - génération des datasets

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

## Lab 2 - object storage S3

Opérations faites sur le bucket : upload, list, download, mv, delete, ajout de métadonnées, presigned URL, upload multipart d'un fichier de 200 MiB, versioning et lifecycle.

Quelques remarques sur ce qui s'est passé :

Au début j'avais NoSuchBucket et AccessDenied sur le PutObject, en fait le bucket n'existait pas encore. Il est créé quand on ouvre l'explorateur de fichiers dans Onyxia, après ça tout marchait.

ETag vs md5sum : sur `users.csv` envoyé en upload simple, l'ETag renvoyé par head-object est `528045479cf87293c3453ab20ae9028b`, exactement le même que le `md5sum users.csv`. Par contre le fichier de 200 MiB est parti en multipart (l'aws cli découpe en parts de 8 MiB), et là l'ETag finit par `-25` : c'est un md5 calculé à partir des md5 des 25 parts, pas le md5 du fichier. Donc on ne peut pas utiliser l'ETag comme checksum dès qu'il y a du multipart.

Bucket policy : j'ai mis une policy Deny sur s3:DeleteObject pour `bronze/*`, et la suppression est bien refusée (AccessDenied) donc la policy est appliquée. Par contre `delete-bucket-policy` renvoie une InternalError, j'imagine que SeaweedFS n'implémente pas complètement cette partie de l'API S3, ce n'est pas un vrai AWS donc tout n'est pas forcément supporté.

Consistency : la section s'est terminée avec un code 2, à priori à cause du `cp` vers `/dev/null` qui n'est pas supporté par le client et pas un problème de cohérence. S3 (et SeaweedFS) est en read-after-write consistent, donc un objet qu'on vient d'écrire est lisible directement.

Job Kubernetes : le pod ne se créait pas, erreur `failed quota: onyxia-quota: must specify limits.cpu`. Le namespace Onyxia a un quota qui oblige à mettre des limits, j'ai donc ajouté `limits.cpu: 500m` dans `job-upload-bronze.yaml`. Après ça le job passe et dans `bronze/` on retrouve `users.csv` (7351 octets) et `orders.csv` (331568 octets). J'ai supprimé les objets Kubernetes à la fin mais j'ai gardé les données dans bronze pour la suite.

## Questions

1. Pourquoi les credentials sont dans un Secret et pas dans la ConfigMap ?

La ConfigMap c'est pour de la config non sensible (endpoint, nom du bucket, préfixe...), elle est lisible en clair par tous ceux qui ont accès au namespace et finit souvent dans git. Un Secret est fait pour les données sensibles : on peut restreindre son accès avec le RBAC séparément de la ConfigMap, il n'est pas affiché par défaut dans les describe, et il peut être chiffré au repos dans etcd. Attention quand même, par défaut c'est juste du base64 donc ce n'est pas du chiffrement, ça reste mieux que de mettre les clés en dur dans la ConfigMap.

2. Les credentials Onyxia sont temporaires, que se passe-t-il si on relance le job demain ? Comment on fait en prod ?

Les credentials S3 d'Onyxia ont une durée de vie limitée (token STS). Si on relance le job demain avec le même Secret, les clés auront expiré et le job va planter avec une erreur d'accès (genre InvalidAccessKeyId / ExpiredToken), il faudrait recréer le Secret à la main avec de nouvelles clés. En prod on ne met pas des clés à la main dans un Secret, on utilise plutôt un compte de service avec une identité fournie par la plateforme (IRSA sur AWS, Workload Identity sur GCP...) qui permet au pod de récupérer des credentials temporaires renouvelés automatiquement, ou bien un gestionnaire de secrets type Vault / External Secrets qui fait la rotation et injecte les secrets dans le pod.

3. Comment transformer ce job en ingestion quotidienne ?

Le plus simple est de passer du Job à un CronJob Kubernetes avec un schedule genre `0 2 * * *` pour tourner tous les jours à 2h. Il faut aussi que chaque run écrive dans un chemin daté (par exemple `bronze/users/date=2026-10-06/users.csv`) pour ne pas écraser les données de la veille et pouvoir rejouer un jour précis. Il faut penser à `concurrencyPolicy: Forbid` pour éviter que deux runs se chevauchent, à garder un historique des jobs (successfulJobsHistoryLimit / failedJobsHistoryLimit) et à régler le problème des credentials de la question 2. Si le pipeline grossit, un orchestrateur comme Airflow ou Argo Workflows serait plus adapté pour gérer les dépendances, les retries et le monitoring.
