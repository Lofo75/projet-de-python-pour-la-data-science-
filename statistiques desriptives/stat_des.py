# l'objectif est dans ce fichier de travailler les données pour en tirer des informations
#
# ÉTAPE 1 : charger les données finales, les regarder, vérifier qu'elles sont
# cohérentes, et préparer les colonnes pour les statistiques.


import matplotlib.pyplot as plt
import numpy as np
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

#un aperçu des données
print(flux.iloc[0].T)
print(evenements.iloc[0].T)
print(portefeuilles.iloc[0].T)



# 10.0 (un float) et pas 10 : certains tokens ont 27 décimales, en entier ça déborde
flux["montant"] = flux["value"].astype(float) / (10.0 ** flux["token_decimal"])
flux["date"] = pd.to_datetime(flux["timestamp"], unit="s")

# table de référence : un bloc -> sa date, triée par bloc
ref = (flux[["block_number", "timestamp"]]
       .drop_duplicates("block_number")
       .sort_values("block_number"))
evenements["timestamp"] = np.interp(evenements["block_number"],
                                    ref["block_number"], ref["timestamp"])
evenements["date"] = pd.to_datetime(evenements["timestamp"], unit="s")

# symbole et décimales des tokens dans les événements 
tokens = flux[["token_address", "token_symbol", "token_decimal"]].copy()
tokens["token_address"] = tokens["token_address"].str.lower()
tokens = tokens.drop_duplicates("token_address")

evenements["reserve"] = evenements["reserve"].str.lower()
evenements = evenements.merge(tokens, left_on="reserve",
                              right_on="token_address", how="left")
evenements["montant"] = evenements["amount_raw"].astype(float) / (10.0 ** evenements["token_decimal"])

# vérifications
print("événements sans token trouvé :", evenements["token_symbol"].isna().sum())
print("période des événements :", evenements["date"].min(), "->", evenements["date"].max())


# ÉTAPE 3 : combien d'événements de chaque type, mois par mois

evenements["mois"] = evenements["date"].dt.to_period("M")   # 2024-03-17 -> 2024-03

# une ligne par mois, une colonne par type d'événement
par_mois = evenements.groupby(["mois", "event_name"]).size().unstack(fill_value=0)
print(par_mois.tail(12))   # les 12 derniers mois

ax = par_mois.plot(figsize=(12, 5))
ax.set_title("Événements Aave V3 par mois")
ax.set_xlabel("Mois")
ax.set_ylabel("Nombre d'événements")
plt.savefig("evenements_par_mois.png", dpi=150, bbox_inches="tight")
plt.close()