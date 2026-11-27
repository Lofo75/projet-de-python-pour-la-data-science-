# l'objectif est dans ce fichier de travailler les données pour en tirer des informations
#
# ÉTAPE 1 : charger les données finales, les regarder, vérifier qu'elles sont
# cohérentes, et préparer les colonnes pour les statistiques.


import json
import os             # pour lire les variables d'environnement d'Onyxia
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd   # pour manipuler des tableaux de données (DataFrame)
import s3fs

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

#suite de la tentative avec l'utilisation des familles 

# ÉTAPE C : exposition par famille (USD, ETH, BTC…)

# dossier racine du dépôt (là où se trouvent scripts/ et data/reference/)
RACINE = Path.cwd() if (Path.cwd() / "scripts").exists() else Path.cwd().parent
sys.path.insert(0, str(RACINE / "scripts"))
from token_families import classify_tokens, token_inventory

FICHIER_FAMILLES = RACINE / "data" / "token_families.parquet"
if FICHIER_FAMILLES.is_file():
    familles = pd.read_parquet(FICHIER_FAMILLES)
else:
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://" + os.environ["AWS_S3_ENDPOINT"]})
    with fs.open(FICHIER_FLUX) as fichier:
        inventaire = token_inventory(fichier)
    refs = RACINE / "data" / "reference"
    aliases = json.loads((refs / "symbol_aliases.json").read_text())
    reference = pd.DataFrame(json.loads((refs / "token_reference.json").read_text()))
    familles = classify_tokens(inventaire, reference, aliases)
    familles.to_parquet(FICHIER_FAMILLES, index=False)   # pour ne pas recalculer la prochaine fois

# on ne garde que les types dont 1 token = 1 unité de la famille
TYPES_1POUR1 = ["stablecoin", "spot", "native_and_wrapped", "wrapped_asset", "deposit_receipt", "debt"]
fam = familles[["token_address", "family", "token_kind"]].copy()
fam["token_address"] = fam["token_address"].str.lower()
fam = fam[fam["family"].ne("Autre") & fam["token_kind"].isin(TYPES_1POUR1)]
fam["poste"] = np.where(fam["token_kind"] == "debt", "dette", "avoir")
fam = fam.set_index("token_address")[["family", "poste"]]

print(fam["family"].value_counts().head(10))


def portefeuille_familles(adresse):
    """Tableau mois x (famille, poste) : quantités détenues et dues, par famille."""
    adresse = adresse.lower()
    f = flux[flux["user_l"] == adresse].copy()
    f["token"] = f["token_address"].str.lower()
    f = f[f["token"].isin(fam.index)]                 # seulement les tokens classés
    f["ordre_tx"] = f.groupby("tx_hash", sort=False).ngroup()

    # même calcul de soldes que l'étape A, token par token
    sorties = f[f["from"].str.lower() == adresse].assign(mouvement=-f["montant"], sens=0)
    entrees = f[f["to"].str.lower() == adresse].assign(mouvement=f["traced_fraction"] * f["montant"], sens=1)
    m = pd.concat([sorties, entrees]).sort_values(["token", "ordre_tx", "sens"])
    cumul = m.groupby("token")["mouvement"].cumsum()
    m["solde"] = cumul - cumul.groupby(m["token"]).cummin().clip(upper=0)

    # photo de fin de mois, token par token
    m["mois"] = m["date"].dt.to_period("M")
    tableau = m.groupby(["mois", "token"])["solde"].last().unstack()
    tableau = tableau.reindex(pd.period_range(tableau.index.min(), DERNIER_MOIS, freq="M")).ffill().fillna(0)

    # on remplace chaque token par (famille, poste), puis on additionne les colonnes identiques
    tableau.columns = pd.MultiIndex.from_frame(fam.loc[tableau.columns].reset_index(drop=True))
    return tableau.T.groupby(level=["family", "poste"]).sum().T


def tracer_portefeuille_familles(adresse, nb_familles=3):
    t = portefeuille_familles(adresse)
    principales = t.max().groupby(level="family").max().nlargest(nb_familles).index
    zero = pd.Series(0.0, index=t.index)
    dates = t.index.to_timestamp()

    fig, axes = plt.subplots(len(principales), 1, figsize=(10, 3 * len(principales)),
                             sharex=True, squeeze=False)
    for ax, famille in zip(axes[:, 0], principales):
        avoir = t.get((famille, "avoir"), zero)
        dette = t.get((famille, "dette"), zero)
        ax.plot(dates, avoir, label="détenu")
        ax.plot(dates, dette, label="dû à Aave")
        ax.plot(dates, avoir - dette, "--", color="black", label="net")
        ax.set_title(f"Famille {famille}")
        ax.legend()
        ax.grid(alpha=0.3)
    axes[0, 0].figure.suptitle(f"Portefeuille tracé de {adresse[:10]}… par famille")
    plt.tight_layout()
    plt.savefig(f"portefeuille_familles_{adresse[:10]}.png", dpi=150, bbox_inches="tight")
    plt.close()


exemple = "0x7FA5195595EFE0dFbc79f03303448af3FbE4ea91"
print(portefeuille_familles(exemple).round(4))
tracer_portefeuille_familles(exemple)



#maintenant on va faire d'autres statistiques descriptives en utilisant la classification 
# ÉTAPE 6 : exposition par famille (USD, ETH, BTC)

# on réutilise la classification de l'étape C : adresse -> famille
famille_de = familles.assign(token_address=familles["token_address"].str.lower()).set_index("token_address")["family"]

def famille_principale(f):
    # on garde les trois grandes familles, le reste est regroupé dans "Autre"
    return f if f in ("USD", "ETH", "BTC") else "Autre"

print(famille_de.map(famille_principale).value_counts())


# fonction pour dessiner un tableau croisé sous forme de carte de chaleur
def carte_chaleur(tableau, titre, xlabel, ylabel, fichier):
    pct = 100 * tableau.div(tableau.sum(axis=1), axis=0)     # % de chaque ligne

    fig, ax = plt.subplots(figsize=(7, 5))
    image = ax.imshow(pct, cmap="Blues", vmin=0, vmax=100)  # couleur = % de la ligne

    # noms des familles sur les axes
    ax.set_xticks(range(len(tableau.columns)), tableau.columns)
    ax.set_yticks(range(len(tableau.index)), tableau.index)

    # dans chaque case : le % et, en dessous, le nombre
    for i in range(len(tableau.index)):
        for j in range(len(tableau.columns)):
            couleur = "white" if pct.iloc[i, j] > 60 else "black"
            ax.text(j, i, f"{pct.iloc[i, j]:.0f} %\n({tableau.iloc[i, j]:,})",
                    ha="center", va="center", color=couleur, fontsize=9)

    ax.set_title(titre)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.colorbar(image, ax=ax, label="% de la ligne")
    plt.savefig(fichier, dpi=150, bbox_inches="tight")
    plt.close()


# 6.1 : ce que les utilisateurs déposent et ce qu'ils empruntent

ev = evenements[evenements["event_name"].isin(["Supply", "Borrow"])].copy()
ev["user"] = ev["user"].str.lower()
ev["famille"] = ev["reserve"].map(famille_de).map(famille_principale)   # reserve est déjà en minuscules

# une ligne par utilisateur et par famille déposée / empruntée
depots = ev[ev["event_name"] == "Supply"][["user", "famille"]].drop_duplicates()
emprunts = ev[ev["event_name"] == "Borrow"][["user", "famille"]].drop_duplicates()

# pour chaque utilisateur, chaque famille déposée avec chaque famille empruntée
croisement = depots.merge(emprunts, on="user", suffixes=("_deposee", "_empruntee"))

print("utilisateurs qui déposent :", depots["user"].nunique())
print("utilisateurs qui empruntent :", emprunts["user"].nunique())

tableau_61 = pd.crosstab(croisement["famille_deposee"], croisement["famille_empruntee"])
carte_chaleur(tableau_61,
              "Ce que les utilisateurs empruntent, selon ce qu'ils déposent",
              "Famille empruntée", "Famille déposée",
              "depot_emprunt_familles.png")





# ÉTAPE A (globale) : solde tracé de chaque user, token par token, après chaque transaction
# on ne garde que les tokens classés de l'étape C : les soldes étant calculés token par token,
# le résultat est le même et on utilise beaucoup moins de mémoire

f = flux[flux["token_address"].str.lower().isin(fam.index)].copy()
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


# STAT 1 : exposition nette agrégée par famille, mois par mois
# (nécessite mvts de l'étape A globale et fam de l'étape C)

# on ne garde que les mouvements des tokens classés (1 token = 1 unité de la famille)
m = mvts[mvts["token"].isin(fam.index)].copy()
m = m.join(fam, on="token")                       # ajoute les colonnes family et poste
m["mois"] = m["date"].dt.to_period("M")

# de combien le solde de chaque user a bougé à chaque mouvement
m["variation"] = m.groupby(["user_l", "token"])["solde"].diff().fillna(m["solde"])

# on additionne les variations de tous les users, par mois, famille et poste, puis on cumule
total = m.groupby(["mois", "family", "poste"])["variation"].sum().unstack(["family", "poste"], fill_value=0)
tous_les_mois = pd.period_range(total.index.min(), DERNIER_MOIS, freq="M")
total = total.reindex(tous_les_mois, fill_value=0).cumsum()

print(total.tail(3).round(0))   # les 3 derniers mois

# un graphique par grande famille : détenu, dû à Aave, net
FAMILLES = ["USD", "ETH", "BTC"]
zero = pd.Series(0.0, index=total.index)
dates = total.index.to_timestamp()

fig, axes = plt.subplots(len(FAMILLES), 1, figsize=(10, 3 * len(FAMILLES)), sharex=True)
for ax, famille in zip(axes, FAMILLES):
    avoir = total.get((famille, "avoir"), zero)
    dette = total.get((famille, "dette"), zero)
    ax.plot(dates, avoir, label="détenu")
    ax.plot(dates, dette, label="dû à Aave")
    ax.plot(dates, avoir - dette, "--", color="black", label="net")
    ax.axhline(0, color="grey", linewidth=0.8)
    ax.set_title(f"Famille {famille}")
    ax.set_ylabel(f"quantité ({famille})")
    ax.legend()
    ax.grid(alpha=0.3)
fig.suptitle("Exposition de l'ensemble des utilisateurs, grâce à l'argent issu d'Aave")
plt.tight_layout()
plt.savefig("exposition_nette_familles.png", dpi=150, bbox_inches="tight")
plt.close()


# STAT 2 : profils des utilisateurs à la fin des données
# (nécessite m de la STAT 1)

# dernier solde de chaque user pour chaque token
dernier = m.groupby(["user_l", "token"]).tail(1)

# on additionne par user, famille et poste : une ligne par user
positions = dernier.groupby(["user_l", "family", "poste"])["solde"].sum().unstack(["family", "poste"], fill_value=0)
colonnes = pd.MultiIndex.from_product([["USD", "ETH", "BTC"], ["avoir", "dette"]], names=["family", "poste"])
positions = positions.reindex(columns=colonnes, fill_value=0)   # toutes les colonnes existent, même vides

# en dessous de ces seuils, on considère que le solde est nul (poussières)
SEUIL = {"USD": 1, "ETH": 0.001, "BTC": 0.0001}
detient = {f: positions[(f, "avoir")] > SEUIL[f] for f in SEUIL}   # True si le user détient la famille
doit = {f: positions[(f, "dette")] > SEUIL[f] for f in SEUIL}      # True si le user doit la famille

# on range chaque user dans un profil (la première condition vraie l'emporte)
conditions = [
    ~(detient["USD"] | detient["ETH"] | detient["BTC"] | doit["USD"] | doit["ETH"] | doit["BTC"]),
    ~(doit["USD"] | doit["ETH"] | doit["BTC"]),
    detient["ETH"] & doit["USD"],
    detient["BTC"] & doit["USD"],
    doit["ETH"] & detient["USD"],
    (detient["ETH"] & doit["ETH"]) | (detient["USD"] & doit["USD"]) | (detient["BTC"] & doit["BTC"]),
]
profils = [
    "aucune position",
    "épargnant (sans dette)",
    "long ETH avec levier (détient ETH, doit USD)",
    "long BTC avec levier (détient BTC, doit USD)",
    "court ETH (doit ETH, détient USD)",
    "looping (détient et doit la même famille)",
]
positions["profil"] = np.select(conditions, profils, default="autre emprunteur")

# part des users dans chaque profil (sans ceux qui n'ont plus aucune position)
actifs = positions[positions["profil"] != "aucune position"]
parts = 100 * actifs["profil"].value_counts(normalize=True)
print("users avec une position en mai 2026 :", len(actifs))
print(parts.round(1))

ax = parts.sort_values().plot.barh(figsize=(9, 4), color="#38768e")
ax.set_xlabel("% des utilisateurs ayant une position")
ax.set_title("Profils des utilisateurs à la fin des données (mai 2026)")
for i, valeur in enumerate(parts.sort_values()):
    ax.text(valeur + 0.5, i, f"{valeur:.1f} %", va="center")
plt.savefig("profils_utilisateurs.png", dpi=150, bbox_inches="tight")
plt.close()


# STAT 3 : concentration de l'exposition nette à l'ETH
# (nécessite positions et SEUIL de la STAT 2)

# exposition nette à l'ETH de chaque user : ce qu'il détient moins ce qu'il doit
net_eth = positions[("ETH", "avoir")] - positions[("ETH", "dette")]
net_eth = net_eth[net_eth > SEUIL["ETH"]]                # users exposés positivement à l'ETH
net_eth = net_eth.sort_values(ascending=False)           # du plus exposé au moins exposé

# part cumulée : les k premiers users détiennent quelle part du total ?
part_cumulee = net_eth.cumsum() / net_eth.sum()
n = len(net_eth)

print("users exposés positivement à l'ETH :", n)
print("exposition nette totale (ETH) :", round(net_eth.sum(), 1))
for p in [0.01, 0.10, 0.50]:
    k = max(1, int(n * p))                               # nombre de users dans le top p
    print(f"les {p:.0%} les plus exposés détiennent {100 * part_cumulee.iloc[k - 1]:.1f} % de l'exposition")

# courbe de concentration
x = 100 * np.arange(1, n + 1) / n                        # % des users (du plus exposé au moins exposé)
y = 100 * part_cumulee.values                            # % de l'exposition totale qu'ils détiennent

fig, ax = plt.subplots(figsize=(6, 5))
ax.plot(x, y, label="observé")
ax.plot([0, 100], [0, 100], "--", color="grey", label="si tous les users étaient égaux")
ax.set_xlabel("% des utilisateurs, du plus exposé au moins exposé")
ax.set_ylabel("% de l'exposition nette à l'ETH")
ax.set_title("Concentration de l'exposition à l'ETH (mai 2026)")
ax.legend()
ax.grid(alpha=0.3)
plt.savefig("concentration_eth.png", dpi=150, bbox_inches="tight")
plt.close()


# STAT 2 bis : évolution des profils des utilisateurs, mois par mois
# (nécessite m de la STAT 1, SEUIL et profils de la STAT 2)

# variation du solde de chaque user, par mois, famille et poste (on garde USD, ETH et BTC)
v = m[m["family"].isin(["USD", "ETH", "BTC"])]
v = v.groupby(["user_l", "mois", "family", "poste"])["variation"].sum().unstack(["family", "poste"], fill_value=0)
colonnes = pd.MultiIndex.from_product([["USD", "ETH", "BTC"], ["avoir", "dette"]], names=["family", "poste"])
v = v.reindex(columns=colonnes, fill_value=0)

# on complète les mois sans mouvement (variation 0), puis on cumule : solde de fin de mois de chaque user
tous_les_mois = pd.period_range(m["mois"].min(), DERNIER_MOIS, freq="M")
grille = pd.MultiIndex.from_product([v.index.get_level_values("user_l").unique(), tous_les_mois],
                                    names=["user_l", "mois"])
soldes = v.reindex(grille, fill_value=0).groupby(level="user_l").cumsum()

# même classement que la STAT 2, appliqué à chaque user et à chaque mois
detient = {f: soldes[(f, "avoir")] > SEUIL[f] for f in SEUIL}
doit = {f: soldes[(f, "dette")] > SEUIL[f] for f in SEUIL}
conditions = [
    ~(detient["USD"] | detient["ETH"] | detient["BTC"] | doit["USD"] | doit["ETH"] | doit["BTC"]),
    ~(doit["USD"] | doit["ETH"] | doit["BTC"]),
    detient["ETH"] & doit["USD"],
    detient["BTC"] & doit["USD"],
    doit["ETH"] & detient["USD"],
    (detient["ETH"] & doit["ETH"]) | (detient["USD"] & doit["USD"]) | (detient["BTC"] & doit["BTC"]),
]
profil_mois = pd.Series(np.select(conditions, profils, default="autre emprunteur"), index=soldes.index)

# nombre de users dans chaque profil, mois par mois (sans "aucune position")
evolution = profil_mois[profil_mois != "aucune position"].groupby(level="mois").value_counts().unstack(fill_value=0)
parts_mois = 100 * evolution.div(evolution.sum(axis=1), axis=0)
print(parts_mois.tail(3).round(1))

fig, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
evolution.index = evolution.index.to_timestamp()
parts_mois.index = parts_mois.index.to_timestamp()
evolution.plot.area(ax=axes[0], legend=False)
axes[0].set_ylabel("nombre d'utilisateurs")
axes[0].set_title("Utilisateurs ayant une position, par profil")
parts_mois.plot.area(ax=axes[1], legend=False)
axes[1].set_ylabel("% des utilisateurs")
axes[1].set_title("Part de chaque profil")
axes[1].set_xlabel("Mois")
axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)
plt.savefig("profils_evolution.png", dpi=150, bbox_inches="tight")
plt.close()