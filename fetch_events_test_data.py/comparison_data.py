#on compare les deux sources de données 
import os
import glob
import pandas as pd
import s3fs

VAULT = os.environ["VAULT_TOP_DIR"]
ALCHEMY_FILE = f"{VAULT}/diffusion/aave_v3_events.parquet"


fs = s3fs.S3FileSystem()

from pathlib import Path

# dossier data/ du projet, quel que soit l'endroit d'où on lance le script
DATA = Path(__file__).resolve().parent.parent / "data"
csv_files = sorted(glob.glob(str(DATA / "aave_*.csv")))

print(csv_files)
print(len(csv_files), "fichiers CSV trouvés")

# Charger les deux sources

# amount_raw lu comme texte : ces nombres sont trop grands pour un entier classique
# Chargement des 5 CSV locaux
ref = pd.concat([
    pd.read_csv(f, dtype={"amount_raw": str})
    for f in csv_files
], ignore_index=True)

# Chargement du fichier Parquet sur S3
print("ALCHEMY_FILE :", ALCHEMY_FILE)
print("Chemin S3 :", "s3://" + ALCHEMY_FILE)
alc = pd.read_parquet("s3://" + ALCHEMY_FILE)

alc["amount_raw"] = alc["amount_raw"].astype(str)

print("référence :", ref.shape, "| alchemy :", alc.shape)
print("colonnes alchemy :", list(alc.columns))

# On met les fichiers au même format pour la comparaison 
# Alchemy écrit les adresses avec des majuscules, les CSV en minuscules -> tout en minuscules
for df in (ref, alc):
    for col in ["event", "tx_hash", "reserve", "user"]:
        if col in df:
            df[col] = df[col].str.lower()
    df["block_number"] = df["block_number"].astype("int64")
    df["log_index"] = df["log_index"].astype("int64")


# On garde seulement la période en commun 
debut = max(ref["block_number"].min(), alc["block_number"].min())
fin = min(ref["block_number"].max(), alc["block_number"].max())
print("Période commune : blocs", debut, "->", fin)

ref = ref[ref["block_number"].between(debut, fin)]
alc = alc[alc["block_number"].between(debut, fin)]

KEY = ["tx_hash", "log_index"]     # identifie un log de façon unique par un log index et un tx_hash
print("Doublons : référence =", ref.duplicated(KEY).sum(), "| alchemy =", alc.duplicated(KEY).sum())
ref = ref.drop_duplicates(KEY)
alc = alc.drop_duplicates(KEY)


# On regarde quels logs sont dans les deux sources 
cols = [c for c in ["event", "block_number", "reserve", "user", "amount_raw"] if c in ref and c in alc]

merged = ref[KEY + cols].merge(alc[KEY + cols], on=KEY, how="outer",
                               suffixes=("_ref", "_alc"), indicator=True)       #on associe les lignes des deux tableaux qui ont la même valeur dans les colonnes KEY, outer permet de garder tous les évènements de chaque côté 


print(merged["_merge"].value_counts())

if "event" in cols:
    merged["event"] = merged["event_ref"].fillna(merged["event_alc"])          #prends event_ref ; s'il est vide, prends event_alc 
    print(pd.crosstab(merged["event"], merged["_merge"]))                       #on vérifie que les logs sont dans both 


# On regarde si pour ces logs égaux, les valeurs sont identiques 
both = merged[merged["_merge"] == "both"]               #l'événement a été trouvé par Etherscan et par Alchemy

for col in cols:
    diff = both[col + "_ref"] != both[col + "_alc"]                     #On compare, ligne par ligne, la valeur d'Etherscan et celle d'Alchemy
    print(f"{col} : {diff.sum()} différence(s) sur {len(both)}")
    if diff.any():
        print(both.loc[diff, KEY + [col + "_ref", col + "_alc"]].head(5).to_string(index=False))        #on affiche 5 exemples de désaccord 

both["user_ref_clean"] = both["user_ref"].str.lower()
both["user_alc_clean"] = both["user_alc"].str.lower()

diff_user = both[
    both["user_ref_clean"] != both["user_alc_clean"]
]

print("Différences réelles :", len(diff_user))

# Vérification des sources: est ce que mon "user" correspond au "caller" de web3
verif = ref[KEY + ["event", "user"]].merge(alc[KEY + ["user", "caller"]], on=KEY,
                                           suffixes=("_ref", "_alc"))
verif["caller"] = verif["caller"].str.lower()

# les différences sur user interviennent sur quels évènements 
ecart_user = verif["user_ref"] != verif["user_alc"]
print("Différences sur user, par type d'événement :")
print(verif.loc[ecart_user, "event"].value_counts(), "\n")

# au vu du résultat précédent on regarde si sur ces évènements là on a bien les mêmes users entre etherscan et alchemy qui les range dans certains cas dans caller (voir notebook)
sb = verif[verif["event"].isin(["supply", "borrow"])]
ecart_caller = sb["user_ref"] != sb["caller"]
print(f"supply et borrow : {ecart_caller.sum()} différence(s) entre mon user et son caller, sur {len(sb):,} événements")
