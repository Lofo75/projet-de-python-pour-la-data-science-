# l'objectif est dans ce fichier de travailler les données pour en tirer des informations
#
# ÉTAPE 1 : charger les données finales, les regarder, vérifier qu'elles sont
# cohérentes, et préparer les colonnes pour les statistiques.


import os             # pour lire les variables d'environnement d'Onyxia
import pandas as pd   # pour manipuler des tableaux de données (DataFrame)

# VAULT_TOP_DIR contient le nom de ton bucket ("louis75")
VAULT = os.environ["VAULT_TOP_DIR"]
DOSSIER = f"s3://{VAULT}/diffusion"

FICHIER_FLUX       = f"{DOSSIER}/transfers_wallets_traced.parquet"  # les transferts gardés par le filtrage
FICHIER_EVENEMENTS = f"{DOSSIER}/aave_v3_events_wallets.parquet"    # les événements Aave des wallets gardés
FICHIER_PORTEFEUIL = f"{DOSSIER}/original_portfolios.parquet"       # l'argent propre apporté par chaque user

flux = pd.read_parquet(FICHIER_FLUX)                 # 9,25 millions de lignes 
evenements = pd.read_parquet(FICHIER_EVENEMENTS)     # 2,5 millions de lignes
portefeuilles = pd.read_parquet(FICHIER_PORTEFEUIL)  # 0,8 million de lignes


print("flux          :", flux.shape)
print("evenements    :", evenements.shape)
print("portefeuilles :", portefeuilles.shape)


#petite vérification 

print(evenements["event_name"].value_counts())   # combien de fois chaque type d'événement
print(portefeuilles["source"].value_counts())    # combien de supply et de complement