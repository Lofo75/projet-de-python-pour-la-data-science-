# Collecte des transferts des utilisateurs Aave V3 — contexte et demande

Ce document résume une conversation précédente pour pouvoir reprendre le travail
dans un nouvel environnement (nouvelle conversation, nouveau service Onyxia).

## La demande

Collecter **l'ensemble des transferts de chaque utilisateur** présent dans
`aave_v3_events.parquet` (207 780 adresses uniques) et les enregistrer dans
**un seul fichier** : `s3://bastienlevyguinot/diffusion/transfers.parquet`.

Approche recommandée (à confirmer avant d'écrire le script) :

- **Source : API Etherscan V2**, endpoint `module=account&action=tokentx&address=<user>`
  (`https://api.etherscan.io/v2/api`, `chainid=1`). Un appel renvoie les transferts
  ERC-20 **envoyés et reçus** d'une adresse, jusqu'à 10 000 lignes.
- **Périmètre : transferts ERC-20 uniquement**, à partir du lancement d'Aave V3
  (`startblock=16496817`, premier événement du pool). L'ETH natif
  (`txlist` + `txlistinternal`) pourra être ajouté plus tard ; cela triple le nombre d'appels.
- **Volume estimé** : ~210 000 appels, soit ~2–3 jours avec le plan gratuit
  (de l'ordre de 100 000 appels/jour et 3–5 appels/s, à vérifier sur le compte).

Points de conception décidés :

- **Pas un fichier par utilisateur** (207k petits fichiers = très lent sur S3).
  Pendant la collecte, écrire **un Parquet par lot** d'environ 1 000 adresses
  (`aave_v3/transfers_parts/batch_XXXXX.parquet`, avec une colonne `user`),
  reprendre automatiquement à partir des lots déjà écrits, puis fusionner en
  `transfers.parquet` à la fin. Même logique que `resume_block` / `merge_parts`
  dans `fetch_events_web3.py`.
- **Adresses avec plus de 10 000 transferts** (bots, contrats) : Etherscan tronque
  à 10 000 lignes ; redécouper la plage de blocs (`startblock`/`endblock`) dans ce cas.
- **Rate limit** : gérer les erreurs de limite (retry avec backoff et jitter) et le
  quota journalier (attendre puis reprendre), comme `get_logs` dans `fetch_events_web3.py`.
- Garder les montants bruts en chaîne (`value`) avec `tokenDecimal`, `contractAddress`,
  `tokenSymbol`, `from`, `to`, `blockNumber`, `timeStamp`, `hash`, `logIndex`.

Pourquoi pas Alchemy : le plan gratuit a un **quota mensuel** (~30 M d'unités de calcul,
partagé entre les clés d'un même compte) ; `alchemy_getAssetTransfers` demande 2 appels
par adresse (un par sens) + pagination, soit ≥ 50 M d'unités → au moins deux mois.
Le quota a déjà été épuisé une fois pendant la collecte des événements.

Alternative non retenue pour l'instant : BigQuery
(`bigquery-public-data.crypto_ethereum.token_transfers`) — une seule requête avec
jointure sur la liste des adresses, mais nécessite un compte Google Cloud.

## Données déjà disponibles (S3 Onyxia / SSP Cloud)

Bucket `bastienlevyguinot`, endpoint `minio.lab.sspcloud.fr` :

| Fichier | Contenu |
|---|---|
| `aave_v3/aave_v3_events.parquet` | 3 919 908 événements Supply / Withdraw / Borrow / Repay / LiquidationCall, blocs 16 496 817 → 25 218 786 |
| `aave_v3/aave_v3_reserve_data.parquet` | Événements `ReserveDataUpdated` (taux et index des réserves) |
| `aave_v3/aave_v3_events_parts/`, `aave_v3/aave_v3_reserve_parts/` | Segments intermédiaires (20 000 blocs chacun) |
| `diffusion/aave_v3_events.parquet` | Copie partagée avec les collègues |

Colonne `user` de `aave_v3_events.parquet` : compte dont la position change
(`onBehalfOf` pour Supply et Borrow). 207 780 adresses uniques ; 218 001 en ajoutant
`caller` et `liquidator`. Liste des utilisateurs :

```python
users = pd.read_parquet("s3://bastienlevyguinot/aave_v3/aave_v3_events.parquet",
                        columns=["user"])["user"].unique()
```

## Scripts du dépôt

- `fetch_events_web3.py` : collecte des événements Aave V3 via Alchemy (web3.py),
  écrit sur S3 par segments avec reprise automatique, puis fusionne.
- `fetch_events_etherscan.py` : premiers essais avec l'API Etherscan (lit `ETHERSCAN_API_KEY`).
- `S3_to_S3_tools/sync_diffusion.py` : permet à un collègue de copier
  `bastienlevyguinot/diffusion/` dans son propre dossier `diffusion/`
  (saute les fichiers déjà présents avec la même taille ; `--dry-run` pour tester).

## Mise en place d'un nouvel environnement Onyxia

- **S3** : aucune configuration. Onyxia injecte `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_S3_ENDPOINT` ; `s3fs.S3FileSystem()`
  et `pd.read_parquet("s3://...")` fonctionnent directement. Les jetons expirent après
  un certain temps : redémarrer le service ou les renouveler pour les longues collectes.
- **Clés API** : à ajouter comme secrets Onyxia (variables d'environnement)
  `ALCHEMY_API_KEY` et `ETHERSCAN_API_KEY`. Ne jamais les écrire dans le dépôt.
- **Paquets** : `pip install web3` (absent par défaut) ; `pandas`, `pyarrow`, `s3fs`,
  `python-dotenv`, `requests` sont déjà présents.
