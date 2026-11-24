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




# Tentative de reconstruction de l'expo

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
DERNIER_MOIS = pd.Period("2026-05", freq="M")   # fin des données
flux["user_l"] = flux["user"].str.lower()        # une seule fois, pour retrouver vite un user


def portefeuille(adresse):
    adresse = adresse.lower()
    f = flux[flux["user_l"] == adresse].copy()     # les lignes de ce user seulement
    f["token"] = f["token_address"].str.lower()    # ETH et WETH : même adresse, un seul solde
    f["ordre_tx"] = f.groupby("tx_hash", sort=False).ngroup()   # n° de transaction, dans l'ordre du fichier

    # chaque ligne devient un mouvement : - montant entier si envoyé, + part tracée si reçu
    sorties = f[f["from"].str.lower() == adresse].assign(mouvement=-f["montant"], sens=0)
    entrees = f[f["to"].str.lower() == adresse].assign(mouvement=f["traced_fraction"] * f["montant"], sens=1)
    m = pd.concat([sorties, entrees]).sort_values(["token", "ordre_tx", "sens"])

    # solde par token, sans jamais descendre sous 0
    cumul = m.groupby("token")["mouvement"].cumsum()
    m["solde"] = cumul - cumul.groupby(m["token"]).cummin().clip(upper=0)

    # photo à la fin de chaque mois, mois sans activité recopiés du précédent
    m["mois"] = m["date"].dt.to_period("M")
    tableau = m.groupby(["mois", "token"])["solde"].last().unstack()
    tableau = tableau.reindex(pd.period_range(tableau.index.min(), DERNIER_MOIS, freq="M")).ffill().fillna(0)

    # noms lisibles, tokens jamais détenus retirés, dettes en négatif
    noms = m.drop_duplicates("token").set_index("token")["token_symbol"]
    noms[WETH] = "ETH+WETH"
    tableau = tableau.loc[:, (tableau > 1e-9).any()].rename(columns=noms)
    dettes = tableau.columns.str.startswith(("variableDebt", "stableDebt"))
    tableau.loc[:, dettes] = -tableau.loc[:, dettes]
    return tableau


def tracer_portefeuille(adresse, nb_tokens=8):
    tableau = portefeuille(adresse)
    principaux = (tableau.abs() > 1e-9).sum().sort_values(ascending=False).index[:nb_tokens]
    axes = tableau[principaux].plot(subplots=True, sharex=True, legend=False,
                                    figsize=(10, 2 * len(principaux)))
    for ax, nom in zip(axes, principaux):
        ax.set_ylabel(nom, rotation=0, ha="right")
    axes[0].set_title(f"Portefeuille tracé de {adresse[:10]}…")
    plt.savefig(f"portefeuille_{adresse[:10]}.png", dpi=150, bbox_inches="tight")
    plt.close()


exemple = "0x7FA5195595EFE0dFbc79f03303448af3FbE4ea91"
print(portefeuille(exemple).round(4))
tracer_portefeuille(exemple)


#exposition agrégée de tous les utilisateurs 
# ÉTAPE A (globale) : solde tracé de chaque user, token par token, après chaque transaction

f = flux.copy()
f["user_l"] = f["user"].str.lower()
f["token"]  = f["token_address"].str.lower()      # ETH et WETH : même adresse, un seul solde
f["ordre_tx"] = f.groupby(["user_l", "tx_hash"], sort=False).ngroup()   # n° de transaction, ordre du fichier

# un mouvement par sens : - montant entier si envoyé, + part tracée si reçu
sorties = f[f["from"].str.lower() == f["user_l"]].assign(mouvement=-f["montant"], sens=0)
entrees = f[f["to"].str.lower() == f["user_l"]].assign(mouvement=f["traced_fraction"] * f["montant"], sens=1)
mvts = pd.concat([sorties, entrees]).sort_values(["user_l", "token", "ordre_tx", "sens"])
del f, sorties, entrees                           # libère la mémoire

# solde par (user, token), sans jamais descendre sous 0
cumul = mvts.groupby(["user_l", "token"])["mouvement"].cumsum()
mvts["solde"] = cumul - cumul.groupby([mvts["user_l"], mvts["token"]]).cummin().clip(upper=0)

print("solde minimum (doit être >= 0) :", mvts["solde"].min())





# EXPOSITION AGRÉGÉE : total de chaque token détenu par l'ensemble des users, mois par mois
# (nécessite mvts de l'étape A globale, avec la colonne "solde")
# EXPOSITION AGRÉGÉE : total de chaque monnaie dans tous les portefeuilles, mois par mois
# (nécessite mvts de l'étape A globale)

MONNAIES = {
    "USDC":     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "USDT":     "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "DAI":      "0x6b175474e89094c44da98b954eedeac495271d0f",
    "ETH+WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "WBTC":     "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
}
DERNIER_MOIS = pd.Period("2026-05", freq="M")

# on ne garde que les mouvements de ces monnaies
m = mvts[mvts["token"].isin(MONNAIES.values())].copy()
m["mois"] = m["date"].dt.to_period("M")

# de combien le solde de chaque user a bougé à chaque mouvement
m["variation"] = m.groupby(["user_l", "token"])["solde"].diff().fillna(m["solde"])

# on additionne les variations de tous les users par mois, puis on cumule dans le temps
total = m.groupby(["mois", "token"])["variation"].sum().unstack(fill_value=0)
tous_les_mois = pd.period_range(total.index.min(), DERNIER_MOIS, freq="M")
total = total.reindex(tous_les_mois, fill_value=0).cumsum()
total = total.rename(columns={adresse: nom for nom, adresse in MONNAIES.items()})

print(total.shape)          # (nombre de mois, nombre de monnaies)
print(total.round(0))

axes = total.plot(subplots=True, sharex=True, legend=False, figsize=(10, 2 * len(MONNAIES)))
for ax, nom in zip(axes, total.columns):
    ax.set_ylabel(nom, rotation=0, ha="right")
axes[0].set_title("Quantité totale détenue grâce à Aave, tous users confondus")
plt.savefig("exposition_agregee.png", dpi=150, bbox_inches="tight")
plt.close()

