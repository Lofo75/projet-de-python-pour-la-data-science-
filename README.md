# Où va l'argent emprunté sur Aave ?

**Projet de Python pour la data science.** Ce projet mesure l'**exposition** que
les utilisateurs du protocole de prêt Aave V3 construisent avec l'argent qu'ils
y obtiennent : combien d'USD, d'ETH ou de BTC ils détiennent, et combien ils
doivent, grâce à Aave.

> **Version provisoire.** Le projet est en cours et sera amélioré par la suite.
> Les limites connues et les prochaines étapes sont listées
> [en fin de document](#état-du-projet-et-suite).

---

## Sommaire

1. [La question : quelle exposition Aave crée-t-il ?](#1-la-question--quelle-exposition-aave-crée-t-il-)
2. [L'idée centrale : filtrer les transactions par provenance](#2-lidée-centrale--filtrer-les-transactions-par-provenance)
3. [Des transactions à l'exposition](#3-des-transactions-à-lexposition)
4. [Le pipeline](#4-le-pipeline)
5. [Exécuter le projet](#5-exécuter-le-projet)
6. [Organisation du dépôt](#6-organisation-du-dépôt)
7. [État du projet et suite](#état-du-projet-et-suite)

---

## 1. La question : quelle exposition Aave crée-t-il ?

[Aave](https://aave.com) est le principal protocole de prêt d'Ethereum. On y
dépose des actifs en garantie (*Supply*) et on y emprunte d'autres actifs
(*Borrow*). Un emprunt n'est presque jamais une fin en soi : l'utilisateur
emprunte des USDC pour acheter de l'ETH (il parie sur la hausse de l'ETH avec
de l'argent emprunté), emprunte de l'ETH pour le vendre (il parie sur sa
baisse), ou redépose ce qu'il emprunte pour amplifier une position (*looping*).

Ces stratégies ne se voient pas dans les données d'Aave : Aave sait seulement
que l'utilisateur doit 1 000 USDC. **Ce qu'il en a fait se passe ailleurs**,
dans les échanges et les transferts de son wallet. Nous reconstruisons donc,
pour chaque utilisateur, l'exposition qui en résulte :

- **ce qu'il détient** grâce à Aave, par famille d'actifs (USD, ETH, BTC…) ;
- **ce qu'il doit** à Aave, dans les mêmes familles ;
- **l'exposition nette**, la différence des deux, mois par mois.

**Ce que nous ne mesurons pas, volontairement.** L'exposition est mesurée **en
quantités**, pas en valeur :
- **pas de prix** : 1 ETH détenu compte pour 1 ETH, quel que soit son cours.
  On décrit la *position* prise par l'utilisateur, indépendamment de ce
  qu'elle lui a rapporté. Les quantités se comparent dans le temps, alors
  que des montants en dollars mélangeraient le comportement de l'utilisateur
  et les mouvements du marché ;
- **pas de rendement (*yield*)** : les intérêts reçus sur les dépôts et payés
  sur les dettes ne sont pas intégrés. Ils s'accumulent sans aucun transfert
  (un aToken « grossit » tout seul), et ne changent pas la nature de la
  position.

## 2. L'idée centrale : filtrer les transactions par provenance

**Le problème.** Le wallet d'un utilisateur d'Aave sert à bien d'autres choses
qu'Aave : salaires reçus, interaction avec d'autres protocoles de finance décentralisée, airdrops,
paiement, ... .
Deux approches simples existent, et aucune ne convient :

| Approche | Ce qu'elle mesure | Pourquoi elle ne convient pas |
|---|---|---|
| Regarder **tout le wallet** | tout ce que l'utilisateur détient | on attribue à Aave des fonds qui n'ont rien à voir avec lui |
| Regarder **les seules transactions Aave** | les emprunts et dépôts eux-mêmes | on perd tout ce qui est fait *après* : l'achat d'ETH avec les USDC empruntés n'apparaît pas |

**Notre méthode : le *filtering*.** Nous suivons **la provenance de l'argent**.
Chaque utilisateur a un **registre** qui indique, pour chaque token, la
quantité de son solde qui vient d'Aave. On lit son historique dans l'ordre
chronologique, et chaque transaction reçoit une **fraction tracée `f`** entre
0 et 1 : la part de la transaction payée avec de l'argent venu d'Aave.

- une transaction Aave a `f = 1` ;
- une autre transaction a la fraction des tokens tracés qu'elle dépense : si
  elle paie 1 500 USDC dont 1 000 venus d'Aave, `f = 2/3`, et les tokens
  reçus en échange sont tracés aux 2/3 ;
- une transaction qui ne dépense rien de tracé a `f = 0`. En particulier,
  **l'argent qui arrive de l'extérieur n'est jamais tracé**.

On ne garde que les transactions avec `f > 0`, avec leur fraction.

**Exemple.** Alice emprunte 1 000 USDC sur Aave (`f = 1`) et les échange contre
0,5 ETH (`f = 1`) : elle détient 0,5 ETH tracé et doit 1 000 USDC, c'est une
position « longue ETH avec levier ». Le même jour, elle reçoit 2 ETH d'une
plateforme d'échange (`f = 0`) : ils ne comptent pas, car ils ne viennent pas
d'Aave.

**Pourquoi c'est nouveau.** L'identification des comportements *on-chain*
s'appuie d'habitude sur des étiquettes d'adresses (exchange, protocole, bot…)
ou sur des agrégats de wallets entiers. Le *filtering* travaille au niveau de
la **transaction**, et sépare *dans un même wallet* les flux financés par un
protocole de tous les autres. On peut ainsi attribuer un comportement à une
source de financement précise, Aave ici, sans être pollué par le reste de
l'activité du wallet.

Les règles détaillées et leurs justifications sont dans
[README_FILTERING.md](README_FILTERING.md). En voici les principales :
- les soldes tracés ne deviennent jamais négatifs, sinon un emprunt peut
  « combler » un solde négatif et disparaître ;
- quand plusieurs tokens tracés sont dépensés, `f` est le plus petit des
  ratios (le maillon le plus faible) ;
- les transferts de montant nul (souvent des arnaques) sont ignorés ;
- l'ETH natif et le WETH (la même valeur, sous deux formes) partagent une
  entrée du registre.

En parallèle, on enregistre le **portefeuille d'origine** : l'argent propre de
l'utilisateur entré dans ces flux (ses dépôts sur Aave, et les tokens personnels
dépensés avec des tokens tracés). Il permettra de comparer l'exposition
construite avec Aave à ce que l'utilisateur a apporté.

## 3. Des transactions à l'exposition

**Familles d'actifs.** Il existe des dizaines de milliers de tokens, mais
beaucoup représentent la même chose : USDC, USDT et DAI sont tous des dollars ;
WETH, stETH ou les aTokens d'Aave sur l'ETH sont tous de l'ETH. Chaque token
est rattaché à une **famille** ([classification/](classification/),
[scripts/token_families.py](scripts/token_families.py)) :
1. **par son adresse**, dans une référence de 448 contrats
   ([data/reference/token_reference.json](data/reference/token_reference.json),
   construite à partir des registres d'Aave et d'Uniswap) ;
2. à défaut, **par son sigle**, dans une table de 240 alias
   ([data/reference/symbol_aliases.json](data/reference/symbol_aliases.json)),
   en retirant les préfixes d'Aave (`aEthUSDC` → `USDC` → USD) ;
3. sinon, ou si les informations du token sont contradictoires, il va dans
   `Autre`.

La classification garde aussi le **type** de chaque token (stablecoin, reçu de
dépôt, dette…), qui distingue ce que l'utilisateur détient de ce qu'il doit.

**Exposition et profils** ([statistiques desriptives/stat_des.py](statistiques%20desriptives/stat_des.py)).
Pour les familles USD, ETH et BTC, on calcule chaque mois, pour chaque
utilisateur, le solde tracé **détenu** et **dû** dans chaque famille. Puis :
- l'exposition de l'ensemble des utilisateurs (détenu, dû, net) ;
- un **profil** par utilisateur et par mois : épargnant (sans dette), long ETH
  ou BTC avec levier (détient ETH/BTC, doit des USD), court ETH (doit de l'ETH,
  détient des USD), *looping* (détient et doit la même famille) ;
- l'évolution des profils et la variation mensuelle de l'exposition nette.

## 4. Le pipeline

```
 RÉCUPÉRATION                    NETTOYAGE                            ANALYSE
 événements Aave   ─┐
 transferts ERC-20 ─┼─▶ preprocessing ─▶ filtering ─▶ classification ─▶ exposition, profils
 flux d'ETH natif  ─┘   (wallets)        (provenance)  (familles)        (statistiques)
```

| Étape | Script | Rôle |
|---|---|---|
| Récupération | `fetch_events_web3.py` | événements du contrat Aave V3 (nœud RPC) |
| | `fetch_transfers_etherscan.py` | transferts de tokens de chaque utilisateur (API Etherscan) |
| | `fetch_eth_etherscan.py` | flux d'ETH natif de chaque utilisateur (API Etherscan) |
| Preprocessing | `preprocess_events_and_transfers.py` | ne garder que les wallets (pas les contrats, tokens, robots) à l'historique complet |
| Filtering | `match_events_transfers.py` | relier chaque transfert à l'événement Aave de sa transaction, contrôler la cohérence des deux sources |
| | `trace_wallets.py` | suivre la provenance : fraction tracée, registres, portefeuille d'origine |
| Classification | `classification/classification_tokens.ipynb` | rattacher chaque token à une famille |
| Statistiques | `statistiques desriptives/stat_des.py` | exposition et profils |

**Les données en chiffres.** 3,9 millions d'événements Aave V3 (janvier 2023 –
mai 2026) de 207 780 utilisateurs ; 175 446 wallets gardés après le
preprocessing ; 51,4 millions de transferts et de flux d'ETH, dont **9,25
millions (18 %) liés à de l'argent venu d'Aave** après le filtering.

La récupération est décrite en détail dans le notebook
[rapport_projet_python_pour_la_datascience.ipynb](rapport_projet_python_pour_la_datascience.ipynb).

## 5. Exécuter le projet

Les données ne sont pas dans Git (fichiers Parquet de plusieurs centaines de Mo).
Elles sont sur le stockage S3 du SSP Cloud (Onyxia), dans le dossier
`bastienlevyguinot/diffusion/`, **lisible publiquement**.

### Installation

```bash
git clone https://github.com/Lofo75/projet-de-python-pour-la-data-science-.git
cd projet-de-python-pour-la-data-science-
pip install -r requirements.txt
```

Toutes les commandes se lancent **depuis la racine du dépôt**.

### Option A — reproduire les analyses à partir des données filtrées (recommandé)

**1. Classification des tokens.** Ouvrir et exécuter
[classification/classification_tokens.ipynb](classification/classification_tokens.ipynb).
Au premier lancement, il télécharge les transferts filtrés (~565 Mo, aucune clé
nécessaire) dans `data/`, puis écrit `data/token_families.parquet` et
`data/transfers_wallets_classified.parquet`.

**2. Statistiques descriptives.** Ce script lit les données sur le S3 avec
`s3fs` : il faut un service Onyxia (SSP Cloud), qui fournit les identifiants
S3 (`AWS_*`) et votre nom d'utilisateur (`VAULT_TOP_DIR`). Copier d'abord les
données dans votre propre espace, puis lancer le script :

```bash
python S3_to_S3_tools/sync_diffusion.py          # copie bastienlevyguinot/diffusion dans <vous>/diffusion
python "statistiques desriptives/stat_des.py"
```

Les tableaux s'affichent dans le terminal et les graphiques sont enregistrés
dans `figures/`. Si l'étape 1 a été faite, la classification est relue depuis
`data/token_families.parquet` ; sinon, elle est recalculée.

### Option B — tout reconstruire depuis la blockchain

Compter **plusieurs jours de calcul** : les API gratuites limitent le nombre
d'appels (3 par seconde pour Etherscan), et il y a plus de 200 000
utilisateurs. Les scripts sauvegardent leur progression par morceaux et
reprennent là où ils s'étaient arrêtés.

Prérequis :
- un service Onyxia pour le stockage S3. Les chemins de sortie sont écrits en
  dur dans les scripts (`s3://bastienlevyguinot/...`) : remplacez-les par votre
  propre espace ;
- une clé [Alchemy](https://www.alchemy.com/) et une clé
  [Etherscan](https://etherscan.io/apis) (plans gratuits), dans un fichier
  `.env` à la racine :
  ```
  ALCHEMY_API_KEY=...
  ETHERSCAN_KEY=...
  ```

Puis, dans l'ordre :

```bash
python fetch_events_web3.py
python fetch_transfers_etherscan.py
python fetch_eth_etherscan.py
python preprocess_events_and_transfers.py
python match_events_transfers.py
python trace_wallets.py
```

puis l'option A. Les étapes du pipeline sont détaillées dans
[README_FILTERING.md](README_FILTERING.md).

> `match_events_transfers.py` et `trace_wallets.py` lisent encore les fichiers
> de la version précédente du preprocessing (`*_wallets.parquet`). Pour une
> reconstruction complète, leurs constantes d'entrée doivent être pointées vers
> les sorties de `preprocess_events_and_transfers.py` (`*_preprocessed.parquet`).

## 6. Organisation du dépôt

```
├── fetch_events_web3.py                 récupération : événements Aave
├── fetch_transfers_etherscan.py         récupération : transferts ERC-20
├── fetch_eth_etherscan.py               récupération : ETH natif
├── preprocess_events_and_transfers.py   preprocessing : garder les wallets
├── match_events_transfers.py            filtering : relier transferts et événements
├── trace_wallets.py                     filtering : suivre la provenance
├── classification/                      classification des tokens en familles (notebook)
├── scripts/token_families.py            fonctions de classification
├── data/reference/                      références de tokens (adresses, sigles)
├── statistiques desriptives/stat_des.py exposition et profils
├── S3_to_S3_tools/sync_diffusion.py     copie des données dans son espace S3
├── notebook_copies/                     versions Google Colab des scripts du pipeline
├── fetch_events_test_data.py/           première récupération via Etherscan, pour comparaison
├── rapport_projet_python_pour_la_datascience.ipynb   rapport : récupération et nettoyage
├── README_FILTERING.md                  détail des règles du filtering
└── requirements.txt
```

## État du projet et suite

**Limites connues.**
- **Chaque wallet est suivi seul.** Si Alice envoie son emprunt à Bob, c'est
  une simple réception pour Bob : l'argent est perdu de vue.
- **L'argent qui sort puis revient est perdu.** Des tokens envoyés sur une
  plateforme d'échange ou vers une autre blockchain, puis récupérés,
  ressemblent à une nouvelle arrivée.
- **Certains dépôts ne laissent rien de visible en retour** (positions de
  liquidité sous forme de NFT, ponts vers d'autres blockchains).
- **Pas de prix ni de rendement**, par choix (section 1) : l'exposition est en
  quantités.
- **Seul le marché principal d'Aave V3 sur Ethereum** est couvert.
- **Les résultats du filtering viennent de la version précédente du
  preprocessing** (175 453 wallets au lieu de 175 446), qui appliquait les
  mêmes règles.

**Prochaines étapes.**
- relancer le filtering sur la sortie du nouveau preprocessing ;
- suivre l'argent entre wallets du jeu de données ;
- étendre les familles d'actifs au-delà d'USD, ETH et BTC pour les
  statistiques.
