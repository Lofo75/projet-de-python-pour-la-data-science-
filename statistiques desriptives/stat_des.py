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
    fichier_flux = f"{dossier}/transfers_wallets_traced.parquet"
    flux = pd.read_parquet(fichier_flux, columns=[
        "user", "tx_hash", "block_number", "timestamp", "from", "to",
        "token_address", "token_decimal", "value", "traced_fraction"])
    evenements = pd.read_parquet(f"{dossier}/aave_v3_events_wallets.parquet",
                                 columns=["block_number", "event_name"])
    return flux, evenements, fichier_flux


def preparer_flux(flux):
    # 10.0 (float) et pas 10 : certains tokens ont 27 décimales, un entier déborderait
    flux["montant"] = flux["value"].astype(float) / (10.0 ** flux["token_decimal"])
    flux["mois"] = pd.to_datetime(flux["timestamp"], unit="s").dt.to_period("M")
    flux["user_l"] = flux["user"].str.lower()
    flux["token"] = flux["token_address"].str.lower()   # ETH et WETH ont la même adresse : un seul solde
    flux["from"] = flux["from"].str.lower()
    flux["to"] = flux["to"].str.lower()
    return flux


def charger_familles(fichier_flux):
    """Table token -> (family, poste), limitée aux familles USD, ETH, BTC."""
    racine = Path.cwd() if (Path.cwd() / "scripts").exists() else Path.cwd().parent
    fichier = racine / "data" / "token_families.parquet"

    if fichier.is_file():
        familles = pd.read_parquet(fichier)
    else:                                   # premier lancement : on calcule et on garde le résultat
        import s3fs
        sys.path.insert(0, str(racine / "scripts"))
        from token_families import classify_tokens, token_inventory
        fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "https://" + os.environ["AWS_S3_ENDPOINT"]})
        with fs.open(fichier_flux) as f:
            inventaire = token_inventory(f)
        refs = racine / "data" / "reference"
        aliases = json.loads((refs / "symbol_aliases.json").read_text())
        reference = pd.DataFrame(json.loads((refs / "token_reference.json").read_text()))
        familles = classify_tokens(inventaire, reference, aliases)
        familles.to_parquet(fichier, index=False)

    fam = familles[["token_address", "family", "token_kind"]].copy()
    fam["token_address"] = fam["token_address"].str.lower()
    fam = fam[fam["family"].isin(FAMILLES) & fam["token_kind"].isin(TYPES_1POUR1)]
    fam["poste"] = np.where(fam["token_kind"] == "debt", "dette", "avoir")
    return fam.drop_duplicates("token_address").set_index("token_address")[["family", "poste"]]

def soldes_mensuels(flux, fam):
    f = flux[flux["token"].isin(fam.index)].copy()
    f["ordre_tx"] = f.groupby(["user_l", "tx_hash"], sort=False).ngroup()

    # chaque transfert devient un mouvement : - montant entier si envoyé, + part tracée si reçu
    sorties = f[f["from"] == f["user_l"]].assign(mouvement=-f["montant"], sens=0)
    entrees = f[f["to"] == f["user_l"]].assign(mouvement=f["traced_fraction"] * f["montant"], sens=1)
    m = (pd.concat([sorties, entrees])
           .sort_values(["user_l", "token", "block_number", "ordre_tx", "sens"])
           .reset_index(drop=True))
    del f, sorties, entrees

    # solde par (user, token) qui ne descend jamais sous 0 :
    # on retire au cumul le point le plus bas déjà atteint (s'il est négatif)
    cle = [m["user_l"], m["token"]]
    cumul = m.groupby(cle)["mouvement"].cumsum()
    m["solde"] = cumul - cumul.groupby(cle).cummin().clip(upper=0)

    # variation du solde à chaque mouvement, puis somme par (user, mois, famille, poste)
    m["variation"] = m.groupby(cle)["solde"].diff().fillna(m["solde"])
    m = m.join(fam, on="token")
    v = (m.groupby(["user_l", "mois", "family", "poste"])["variation"].sum()
          .unstack(["family", "poste"], fill_value=0))
    colonnes = pd.MultiIndex.from_product([FAMILLES, POSTES], names=["family", "poste"])
    v = v.reindex(columns=colonnes, fill_value=0)

    # on complète les mois sans mouvement, puis on cumule : solde de fin de mois
    tous_les_mois = pd.period_range(m["mois"].min(), DERNIER_MOIS, freq="M")
    grille = pd.MultiIndex.from_product([v.index.get_level_values("user_l").unique(), tous_les_mois],
                                        names=["user_l", "mois"])
    return v.reindex(grille, fill_value=0).groupby(level="user_l").cumsum()


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
        ~(un_avoir | une_dette),
        ~une_dette,
        detient["ETH"] & doit["USD"],
        detient["BTC"] & doit["USD"],
        doit["ETH"] & detient["USD"],
        (detient["ETH"] & doit["ETH"]) | (detient["USD"] & doit["USD"]) | (detient["BTC"] & doit["BTC"]),
    ]
    return pd.Series(np.select(conditions, PROFILS, default="autre emprunteur"), index=soldes.index)


# ---------------------------------------------------------------------------
# Les statistiques
# ---------------------------------------------------------------------------

def stat_evenements_par_mois(evenements, flux):
    # les événements n'ont pas de date : on l'interpole à partir des blocs des transferts
    ref = flux[["block_number", "timestamp"]].drop_duplicates("block_number").sort_values("block_number")
    ts = np.interp(evenements["block_number"], ref["block_number"], ref["timestamp"])
    mois = pd.to_datetime(ts, unit="s").to_period("M")

    par_mois = evenements.groupby([mois, evenements["event_name"]]).size().unstack(fill_value=0)
    print("\nÉvénements par mois (12 derniers) :\n", par_mois.tail(12))

    par_mois.index = par_mois.index.to_timestamp()
    ax = par_mois.plot(figsize=(12, 5))
    ax.set_title("Événements Aave V3 par mois")
    ax.set_xlabel("Mois")
    ax.set_ylabel("Nombre d'événements")
    ax.grid(alpha=0.3)
    plt.savefig(DOSSIER_FIGURES / "0_evenements_par_mois.png", dpi=150, bbox_inches="tight")
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
    """Variation en % de l'exposition nette d'un mois sur l'autre.

    Exemple : 100 ETH au mois 0, 102 ETH au mois 1  ->  +2 %.
    On divise par la valeur absolue du mois précédent, pour que le signe se lise
    toujours pareil (+ = l'exposition augmente). Le % n'a pas de sens quand le mois
    précédent est presque nul ou quand le net change de signe : ces mois sont retirés.
    """
    net = pd.DataFrame({f: total[(f, "avoir")] - total[(f, "dette")] for f in FAMILLES})
    precedent = net.shift(1)
    variation = 100 * (net - precedent) / precedent.abs()

    base_trop_petite = precedent.abs() < 0.01 * net.abs().max()   # moins de 1 % du maximum
    change_de_signe = np.sign(net) != np.sign(precedent)
    variation = variation.mask(base_trop_petite | change_de_signe)

    print("\nSTAT 4 - variation mensuelle de l'exposition nette, en % (6 derniers mois) :\n",
          variation.tail(6).round(1))

    dates = variation.index.to_timestamp()
    fig, axes = plt.subplots(len(FAMILLES), 1, figsize=(11, 3 * len(FAMILLES)), sharex=True)
    for ax, famille in zip(axes, FAMILLES):
        valeurs = variation[famille]
        couleurs = np.where(valeurs >= 0, "#2a9d8f", "#e76f51")
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


# ---------------------------------------------------------------------------
# Lancement
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    flux, evenements, fichier_flux = charger_donnees()
    flux = preparer_flux(flux)
    fam = charger_familles(fichier_flux)
    print("flux :", flux.shape, "| événements :", evenements.shape, "| tokens classés :", len(fam))

    stat_evenements_par_mois(evenements, flux)

    soldes = soldes_mensuels(flux, fam)
    print("soldes :", soldes.shape, "| solde minimum (doit être >= 0) :", soldes.min().min())

    total = stat1_exposition(soldes)
    stat2_profils_fin(soldes)
    stat2bis_profils_evolution(soldes)
    stat4_variation_exposition(total)
