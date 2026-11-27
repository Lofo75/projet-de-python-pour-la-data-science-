# Statistiques descriptives sur les utilisateurs d'Aave V3
#
# 1. nombre d'événements Aave par mois
# 2. STAT 1   : exposition de l'ensemble des users par famille (détenu, dû à Aave, net)
# 3. STAT 2   : profils des users à la fin des données
# 4. STAT 2bis: évolution des profils mois par mois
# 5. STAT 4   : variation mensuelle (en %) de l'exposition nette par famille
#
#   création d'un tableau : une ligne = (user, mois), colonnes = (famille, poste) avec poste = avoir ou dette


import json
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

#on commence par définir ce qu'on va utiliser 
FAMILLES = ["USD", "ETH", "BTC"]
POSTES = ["avoir", "dette"]
DERNIER_MOIS = pd.Period("2026-05", freq="M")          # fin des données
SEUIL = {"USD": 1, "ETH": 0.001, "BTC": 0.0001}        # si en dessous du seuil on ne regarde pas car sinon trop de bruit dans les graphiques 
# types de tokens pour lesquels 1 token = 1 unité de la famille
TYPES_1POUR1 = ["stablecoin", "spot", "native_and_wrapped", "wrapped_asset", "deposit_receipt", "debt"]

DOSSIER_FIGURES = Path("figures")
DOSSIER_FIGURES.mkdir(exist_ok=True)

#on prépare les données 

def charger_donnees():
    dossier = f"s3://{os.environ['VAULT_TOP_DIR']}/diffusion"
    fichier_flux = f"{dossier}/transfers_wallets_traced.parquet"            #on récupère les transferts
    flux = pd.read_parquet(fichier_flux, columns=[                          #on ne prend pas toutes les colonnes 
        "user", "tx_hash", "block_number", "timestamp", "from", "to",           
        "token_address", "token_decimal", "value", "traced_fraction"])
    evenements = pd.read_parquet(f"{dossier}/aave_v3_events_wallets.parquet",       #les différents évènements 
                                 columns=["block_number", "event_name"])
    return flux, evenements, fichier_flux

#on convertit les données pour les exploiter
def preparer_flux(flux):
    # 10.0 (float) et pas 10 : certains tokens ont 27 décimales, un entier déborderait
    flux["montant"] = flux["value"].astype(float) / (10.0 ** flux["token_decimal"])     #dans le DataFrame 1,5 USDC est stocké comme 1 500 000
    flux["mois"] = pd.to_datetime(flux["timestamp"], unit="s").dt.to_period("M")
    flux["user_l"] = flux["user"].str.lower()
    flux["token"] = flux["token_address"].str.lower()   # ETH et WETH ont la même adresse : un seul solde
    flux["from"] = flux["from"].str.lower()
    flux["to"] = flux["to"].str.lower()
    return flux

#on va utiliser le fichier de famille de ruben qui associe les transferts à des familles 

def charger_familles():
    """Table token -> (family, poste), limitée aux familles USD, ETH, BTC."""
    familles = pd.read_parquet("s3://louis75/diffusion/token_families.parquet")    # la table déjà calculée, dans mon dossier diffusion

    fam = familles[["token_address", "family", "token_kind"]].copy()
    fam["token_address"] = fam["token_address"].str.lower()             #on remet en minuscule pour que toutes les sources de données soient de même format
    fam = fam[fam["family"].isin(FAMILLES) & fam["token_kind"].isin(TYPES_1POUR1)]  #on élimine tous les tokens qui ne sont pas 1pour1 (AAVE, sDai...)
    fam["poste"] = np.where(fam["token_kind"] == "debt", "dette", "avoir")          #on définit dette et avoir, si ce n'est pas une dette c'est un avoir 
    return fam.drop_duplicates("token_address").set_index("token_address")[["family", "poste"]]


fam = charger_familles()
print("tokens classés :", len(fam))
print(pd.crosstab(fam["family"], fam["poste"]))

#on créé un tableau qui montre ce que chaque user possède et doit (uniquement dans les monnaies considérées)par mois

def soldes_mensuels(flux, fam):
    f = flux[flux["token"].isin(fam.index)].copy()              #on garde que les tokens qui nous intéressent 
    f["ordre_tx"] = f.groupby(["user_l", "tx_hash"], sort=False).ngroup()       #des transactions peuvent avoir lieu dans le même bloc donc on numérote par ordre

    # chaque transfert devient un mouvement : - montant entier si envoyé, + part tracée si reçu
    sorties = f[f["from"] == f["user_l"]].assign(mouvement=-f["montant"], sens=0)
    entrees = f[f["to"] == f["user_l"]].assign(mouvement=f["traced_fraction"] * f["montant"], sens=1)
    m = (pd.concat([sorties, entrees])                                                  #on empile les deux tableaux 
           .sort_values(["user_l", "token", "block_number", "ordre_tx", "sens"])
           .reset_index(drop=True))
    del f, sorties, entrees     #sert à libérer de la mémoire

    # solde par (user, token) qui ne descend jamais sous 0 :
    # on retire au cumul le point le plus bas déjà atteint (s'il est négatif)
    cle = [m["user_l"], m["token"]]             #on définit un compte 
    cumul = m.groupby(cle)["mouvement"].cumsum()                #on fait la somme cumulé par token
    m["solde"] = cumul - cumul.groupby(cle).cummin().clip(upper=0)      

    # variation du solde à chaque mouvement, puis somme par (user, mois, famille, poste)
    m["variation"] = m.groupby(cle)["solde"].diff().fillna(m["solde"])          #on calcule les variations [kème valeur]-[k-1ème valeur]
    m = m.join(fam, on="token")                                         #on ajoute famille et poste 
    v = (m.groupby(["user_l", "mois", "family", "poste"])["variation"].sum()
          .unstack(["family", "poste"], fill_value=0))
    colonnes = pd.MultiIndex.from_product([FAMILLES, POSTES], names=["family", "poste"])
    v = v.reindex(columns=colonnes, fill_value=0)

    # on complète les mois sans mouvement, puis on cumule : solde de fin de mois
    tous_les_mois = pd.period_range(m["mois"].min(), DERNIER_MOIS, freq="M")            #liste de tous les mois
    grille = pd.MultiIndex.from_product([v.index.get_level_values("user_l").unique(), tous_les_mois],           #Il range le résultat dans un MultiIndex, c'est-à-dire une liste d'étiquettes à deux parties, utilisable comme étiquettes de lignes d'un tableau.
                                        names=["user_l", "mois"])
    return v.reindex(grille, fill_value=0).groupby(level="user_l").cumsum()     #on ajoute les lignes manquantes 


#on va classer les profils pour faire des statistiques descriptives en créant des catégories correspondants à des comportements

PROFILS = [
    "aucune position",
    "épargnant (sans dette)",
    "long ETH avec levier (détient ETH, doit USD)",
    "long BTC avec levier (détient BTC, doit USD)",
    "court ETH (doit ETH, détient USD)",
    "looping (détient et doit la même famille)",
]


def classer(soldes):
    """Un profil par ligne. L'ordre compte : la première condition vraie l'emporte."""
    detient = {f: soldes[(f, "avoir")] > SEUIL[f] for f in FAMILLES}
    doit = {f: soldes[(f, "dette")] > SEUIL[f] for f in FAMILLES}
    une_dette = doit["USD"] | doit["ETH"] | doit["BTC"]
    un_avoir = detient["USD"] | detient["ETH"] | detient["BTC"]
    conditions = [
        ~(un_avoir | une_dette),                # 1 aucune position (le ~ prend le contraire de l'un ou l'autre )
        ~une_dette,                             # 2 épargnant
        detient["ETH"] & doit["USD"],           # 3 long ETH
        detient["BTC"] & doit["USD"],           # 4 long BTC
        doit["ETH"] & detient["USD"],           # 5 court ETH
        (detient["ETH"] & doit["ETH"]) | (detient["USD"] & doit["USD"]) | (detient["BTC"] & doit["BTC"]),   # 6 looping
    ]
    return pd.Series(np.select(conditions, PROFILS, default="autre emprunteur"), index=soldes.index)



#on trace maintenant les statistiques qui nous intéressent 


def stat_evenements_par_mois(evenements, flux):
    # les événements en eux mêmes n'ont pas de date : on l'interpole à partir des blocs des transferts
    ref = flux[["block_number", "timestamp"]].drop_duplicates("block_number").sort_values("block_number")
    ts = np.interp(evenements["block_number"], ref["block_number"], ref["timestamp"])
    mois = pd.to_datetime(ts, unit="s").to_period("M")

    par_mois = evenements.groupby([mois, evenements["event_name"]]).size().unstack(fill_value=0)        #groupby sur deux critères : le mois (calculé juste au-dessus) et le type d'événement
    print("\nÉvénements par mois (12 derniers) :\n", par_mois.tail(12))

    par_mois.index = par_mois.index.to_timestamp()
    ax = par_mois.plot(figsize=(12, 5))                 # figsize = (largeur, hauteur) en pouces
    ax.set_title("Événements Aave V3 par mois")
    ax.set_xlabel("Mois")
    ax.set_ylabel("Nombre d'événements")
    ax.grid(alpha=0.3)
    plt.savefig(DOSSIER_FIGURES / "0_evenements_par_mois.png", dpi=150, bbox_inches="tight")            #on enregistre en png 
    plt.close()


def stat1_exposition(soldes):
    """Somme des soldes de tous les users, mois par mois."""
    total = soldes.groupby(level="mois").sum()
    print("\nSTAT 1 - exposition totale (3 derniers mois) :\n", total.tail(3).round(0))

    dates = total.index.to_timestamp()
    fig, axes = plt.subplots(len(FAMILLES), 1, figsize=(10, 3 * len(FAMILLES)), sharex=True)
    for ax, famille in zip(axes, FAMILLES):
        avoir, dette = total[(famille, "avoir")], total[(famille, "dette")]
        ax.plot(dates, avoir, label="détenu")
        ax.plot(dates, dette, label="dû à Aave")
        ax.plot(dates, avoir - dette, "--", color="black", label="net")
        ax.axhline(0, color="grey", linewidth=0.8)
        ax.set_title(f"Famille {famille}")
        ax.set_ylabel(f"quantité ({famille})")
        ax.legend()
        ax.grid(alpha=0.3)
    fig.suptitle("STAT 1 - Exposition de l'ensemble des utilisateurs")
    plt.tight_layout()
    plt.savefig(DOSSIER_FIGURES / "1_exposition_familles.png", dpi=150, bbox_inches="tight")
    plt.close()
    return total


def stat2_profils_fin(soldes):
    """Profils au dernier mois des données."""
    profil = classer(soldes.xs(DERNIER_MOIS, level="mois"))
    actifs = profil[profil != "aucune position"]
    parts = (100 * actifs.value_counts(normalize=True)).sort_values()
    print(f"\nSTAT 2 - users avec une position en {DERNIER_MOIS} : {len(actifs):,}")
    print(parts.round(1))

    ax = parts.plot.barh(figsize=(9, 4), color="#38768e")
    for i, valeur in enumerate(parts):
        ax.text(valeur + 0.5, i, f"{valeur:.1f} %", va="center")
    ax.set_xlabel("% des utilisateurs ayant une position")
    ax.set_title(f"STAT 2 - Profils des utilisateurs en {DERNIER_MOIS}")
    plt.savefig(DOSSIER_FIGURES / "2_profils_fin.png", dpi=150, bbox_inches="tight")
    plt.close()


def stat2bis_profils_evolution(soldes):
    """Même classement que la STAT 2, appliqué à chaque mois."""
    profil = classer(soldes)
    profil = profil[profil != "aucune position"]
    evolution = profil.groupby(level="mois").value_counts().unstack(fill_value=0)
    parts = 100 * evolution.div(evolution.sum(axis=1), axis=0)
    print("\nSTAT 2bis - part de chaque profil (3 derniers mois) :\n", parts.tail(3).round(1))

    evolution.index = evolution.index.to_timestamp()
    parts.index = parts.index.to_timestamp()
    fig, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
    evolution.plot.area(ax=axes[0], legend=False)
    axes[0].set_ylabel("nombre d'utilisateurs")
    axes[0].set_title("Utilisateurs ayant une position, par profil")
    parts.plot.area(ax=axes[1], legend=False)
    axes[1].set_ylabel("% des utilisateurs")
    axes[1].set_title("Part de chaque profil")
    axes[1].set_xlabel("Mois")
    axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)
    fig.suptitle("STAT 2bis - Évolution des profils")
    plt.savefig(DOSSIER_FIGURES / "2bis_profils_evolution.png", dpi=150, bbox_inches="tight")
    plt.close()


def stat4_variation_exposition(total):
    """Variation en % de l'exposition nette d'un mois sur l'autre. """
    net = pd.DataFrame({f: total[(f, "avoir")] - total[(f, "dette")] for f in FAMILLES})
    precedent = net.shift(1)                    #décales toutes les valeurs d'une ligne vers le bas 
    variation = 100 * (net - precedent) / precedent.abs()       #calcul le pourcentage de variation 

    base_trop_petite = precedent.abs() < 0.01 * net.abs().max()   # moins de 1 % du maximum, on compare chaque case au seuil de sa colonne
    change_de_signe = np.sign(net) != np.sign(precedent)                
    variation = variation.mask(base_trop_petite | change_de_signe)

    print("\nSTAT 4 - variation mensuelle de l'exposition nette, en % (6 derniers mois) :\n",
          variation.tail(6).round(1))

    dates = variation.index.to_timestamp()
    fig, axes = plt.subplots(len(FAMILLES), 1, figsize=(11, 3 * len(FAMILLES)), sharex=True)
    for ax, famille in zip(axes, FAMILLES):
        valeurs = variation[famille]
        couleurs = np.where(valeurs.fillna(0) >= 0, "#2a9d8f", "#e76f51")
        ax.bar(dates, valeurs.fillna(0), width=20, color=couleurs)
        ax.axhline(0, color="grey", linewidth=0.8)
        ax.set_title(f"Famille {famille}")
        ax.set_ylabel("variation (%)")
        ax.grid(alpha=0.3)
    fig.suptitle("STAT 4 - Variation mensuelle de l'exposition nette (en %)")
    plt.tight_layout()
    plt.savefig(DOSSIER_FIGURES / "4_variation_exposition.png", dpi=150, bbox_inches="tight")
    plt.close()
    return variation


if __name__ == "__main__":
    flux, evenements, fichier_flux = charger_donnees()
    flux = preparer_flux(flux)
    fam = charger_familles()
    print("flux :", flux.shape, "| événements :", evenements.shape, "| tokens classés :", len(fam))

    stat_evenements_par_mois(evenements, flux)

    soldes = soldes_mensuels(flux, fam)
    print("soldes :", soldes.shape, "| solde minimum (doit être >= 0) :", soldes.min().min())

    total = stat1_exposition(soldes)
    stat2_profils_fin(soldes)
    stat2bis_profils_evolution(soldes)
    stat4_variation_exposition(total)
