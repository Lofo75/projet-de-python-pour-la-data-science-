# %% 1. Chemins ---------------------------------------------------------------
# Dans VS Code, exécute chaque bloc "# %%" avec "Run Cell", dans l'ordre.
import os
import glob
import pandas as pd
import s3fs

VAULT = os.environ["VAULT_TOP_DIR"]
ALCHEMY_FILE = f"{VAULT}/diffusion/aave_v3_events.parquet"


fs = s3fs.S3FileSystem()

csv_files = glob.glob("*.csv")

print(csv_files)
print(len(csv_files), "fichiers CSV trouvés")

# %% 2. Charger les deux sources ------------------------------------------------

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

# %% 3. Mettre les deux au même format ---------------------------------------------
# Alchemy écrit les adresses avec des majuscules, les CSV en minuscules -> tout en minuscules
for df in (ref, alc):
    for col in ["event", "tx_hash", "reserve", "user"]:
        if col in df:
            df[col] = df[col].str.lower()
    df["block_number"] = df["block_number"].astype("int64")
    df["log_index"] = df["log_index"].astype("int64")


# %% 4. Garder seulement la période commune ----------------------------------------
debut = max(ref["block_number"].min(), alc["block_number"].min())
fin = min(ref["block_number"].max(), alc["block_number"].max())
print("Période commune : blocs", debut, "->", fin)

ref = ref[ref["block_number"].between(debut, fin)]
alc = alc[alc["block_number"].between(debut, fin)]

KEY = ["tx_hash", "log_index"]     # identifie un log de façon unique
print("Doublons : référence =", ref.duplicated(KEY).sum(), "| alchemy =", alc.duplicated(KEY).sum())
ref = ref.drop_duplicates(KEY)
alc = alc.drop_duplicates(KEY)


# %% 5. Quels logs sont dans les deux sources ? --------------------------------------
cols = [c for c in ["event", "block_number", "reserve", "user", "amount_raw"] if c in ref and c in alc]

merged = ref[KEY + cols].merge(alc[KEY + cols], on=KEY, how="outer",
                               suffixes=("_ref", "_alc"), indicator=True)

# both = dans les deux | left_only = seulement dans la référence | right_only = seulement chez Alchemy
print(merged["_merge"].value_counts())

if "event" in cols:
    merged["event"] = merged["event_ref"].fillna(merged["event_alc"])
    print(pd.crosstab(merged["event"], merged["_merge"]))


# %% 6. Pour les logs présents partout : les valeurs sont-elles identiques ? -------
both = merged[merged["_merge"] == "both"]

for col in cols:
    diff = both[col + "_ref"] != both[col + "_alc"]
    print(f"{col} : {diff.sum()} différence(s) sur {len(both)}")
    if diff.any():
        print(both.loc[diff, KEY + [col + "_ref", col + "_alc"]].head(5).to_string(index=False))

both["user_ref_clean"] = both["user_ref"].str.lower()
both["user_alc_clean"] = both["user_alc"].str.lower()

diff_user = both[
    both["user_ref_clean"] != both["user_alc_clean"]
]

print("Différences réelles :", len(diff_user))

print(
    diff_user[
        KEY + ["user_ref", "user_alc"]
    ].head(20).to_string(index=False)
)
