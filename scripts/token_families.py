import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Dans les données du groupe, l'ETH natif a aussi été rangé sous l'adresse de WETH.
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def token_inventory(path, batch_size=250000):
    """
    Passer du tableau de transferts à un tableau avec une ligne par token.

    En gros, on compte combien de fois chaque token apparaît, en gardant ses noms
    et ses décimales. L'adresse du contrat sert à identifier le token.

    On lit 250 000 transferts à la fois pour limiter la mémoire utilisée.
    """
    metadata = ["token_address", "token_symbol", "token_name", "token_decimal"]
    columns = metadata + ["tracked", "value"]
    counts = []
    parquet = pq.ParquetFile(path)

    # Chaque lot devient un petit DataFrame. On ne charge que les colonnes utiles.
    for chunk in parquet.iter_batches(batch_size=batch_size, columns=columns):
        df = chunk.to_pandas()
        df["token_address"] = df["token_address"].str.lower()
        df["token_symbol"] = df["token_symbol"].fillna("")
        df["token_name"] = df["token_name"].fillna("")

        # value est du texte. Cette expression reconnaît "0", "00", etc.
        df["is_zero"] = df["value"].str.fullmatch(r"0+").fillna(False)

        # On compte les transferts pour chaque combinaison adresse / nom / décimales. Cela permet de
        # repérer ensuite une adresse associée à plusieurs noms.
        # size compte les lignes et sum compte les True. dropna=False garde les infos manquantes.
        totals = df.groupby(metadata, dropna=False).agg(
            n_rows=("tracked", "size"),
            n_tracked=("tracked", "sum"),
            n_zero=("is_zero", "sum"),
        ).reset_index()
        counts.append(totals)

    # Le même token peut revenir dans plusieurs lots. On rassemble leurs comptages.
    variants = pd.concat(counts, ignore_index=True)
    variants = variants.groupby(metadata, dropna=False, as_index=False)[
        ["n_rows", "n_tracked", "n_zero"]
    ].sum()

    rows = []
    for address, group in variants.groupby("token_address", sort=True):
        # On retient le nom le plus fréquent pour l'affichage.
        # En cas d'égalité, le tri alphabétique permet de garder un choix reproductible.
        primary = group.sort_values(
            ["n_rows", "token_symbol", "token_name"], ascending=[False, True, True]
        ).iloc[0]
        symbols = sorted(group["token_symbol"].unique())
        names = sorted(group["token_name"].unique())
        decimals = sorted(int(value) for value in group["token_decimal"].dropna().unique())

        # Pour une même adresse, plusieurs sigles, noms ou décimales sont à vérifier.
        conflict = len(symbols) != 1 or len(names) != 1 or len(decimals) != 1
        conflict = conflict or group["token_decimal"].isna().any()

        # Exception connue dans notre fichier : ETH et WETH partagent déjà une adresse.
        if (
            address == WETH
            and set(symbols) <= {"ETH", "WETH"}
            and set(names) <= {"Ether", "Wrapped Ether"}
            and decimals == [18]
        ):
            conflict = False

        # Une ligne par adresse, avec les variantes conservées dans des listes.
        rows.append({
            "token_address": address,
            "token_symbol": primary["token_symbol"],
            "token_name": primary["token_name"],
            "token_decimal": decimals[0] if len(decimals) == 1 else None,
            "symbols": symbols,
            "names": names,
            "decimals": decimals,
            "metadata_conflict": bool(conflict),
            "n_rows": int(group["n_rows"].sum()),
            "n_tracked": int(group["n_tracked"].sum()),
            "n_zero": int(group["n_zero"].sum()),
        })

    # Les tokens les plus présents dans les transferts apparaissent en premier.
    return pd.DataFrame(rows).sort_values(
        ["n_rows", "token_address"], ascending=[False, True], ignore_index=True
    )


def propose_family(symbol, aliases):
    """
    Chercher la famille d'un token à partir de son sigle.

    Exemple direct : DAI -> USD.
    Pour Aave, on coupe le préfixe : aEthUSDC -> USDC -> USD.
    Le préfixe aEth indique ici un reçu Aave sur Ethereum, pas une famille ETH.
    """
    symbol = (symbol or "").strip()

    # Les clés du JSON sont en majuscules, donc on compare dans le même format.
    if symbol.upper() in aliases:
        return aliases[symbol.upper()]["family"]

    # On reconnaît les préfixes des reçus et des dettes Aave. La dernière parenthèse capture le
    # sigle restant, par exemple USDC dans aEthUSDC.
    match = re.fullmatch(
        r"(?:variableDebt|stableDebt|a)(?:EthLido|Eth|HorRwa)?(.+)", symbol
    )
    if match and match[1].upper() in aliases:
        return aliases[match[1].upper()]["family"]

    # Si aucune règle trouvée
    return None


def classify_tokens(inventory, reference, aliases):
    """
    Attribuer une famille aux tokens de l'inventaire.

    On cherche d'abord l'adresse dans la référence. Si elle n'y est pas,
    on accepte la famille trouvée par le sigle. Le statut garde cette distinction.
    Les cas sans correspondance ou avec des infos contradictoires vont dans Autre.
    """
    # Pratique pour le notebook, on n'a pas à modifier les tableaux.
    inventory = inventory.copy()
    reference = reference.copy()
    inventory["token_address"] = inventory["token_address"].str.lower()
    reference["token_address"] = reference["token_address"].str.lower()

    # left garde tous les tokens, même ceux qui n'ont pas de référence. One_to_one vérifie qu'une
    # adresse n'apparaît pas plusieurs fois dans une table.
    tokens = inventory.merge(
        reference, on="token_address", how="left", validate="one_to_one"
    )
    tokens["proposed_family"] = tokens["token_symbol"].map(
        lambda symbol: propose_family(symbol, aliases)
    )

    # Ces deux séries de True/False indiquent quelles lignes on va remplir. Le ~ est une négation.
    # On utilise le nom seulement si l'adresse n'a PAS donné de famille.
    by_address = tokens["family"].notna()
    by_name = ~by_address & tokens["proposed_family"].notna()
    tokens["status"] = "unresolved"
    tokens.loc[by_address, "status"] = "reference_match"
    tokens.loc[by_name, "family"] = tokens.loc[by_name, "proposed_family"]
    tokens.loc[by_name, "status"] = "name_match"

    # Pour les classements par nom, on récupère aussi le type indiqué dans le JSON. Bien pour
    # distinguer ETH d'un reçu de staking lié à l'ETH par ex.
    symbols = tokens["token_symbol"].str.strip()
    kinds = {symbol: info.get("token_kind", "unknown") for symbol, info in aliases.items()}
    types = symbols.str.upper().map(kinds).fillna("unknown")

    # Si on a dû couper un préfixe Aave, on conserve le type reçu ou dette. Un sigle déjà présent
    # tel quel dans le JSON garde le type donné par ce JSON.
    aave = ~symbols.str.upper().isin(aliases) & by_name
    types.loc[aave & symbols.str.startswith("a")] = "deposit_receipt"
    types.loc[aave & symbols.str.startswith(("variableDebt", "stableDebt"))] = "debt"
    tokens.loc[by_name, "token_kind"] = types[by_name]

    # Même si une famille a été trouvée, on vérifie les informations du token. On compare les
    # décimales si la référence les fournit et on vérifie le format d'adresse.
    bad_decimals = (
        tokens["expected_decimals"].notna()
        & tokens["token_decimal"].ne(tokens["expected_decimals"])
    )
    bad_address = ~tokens["token_address"].str.fullmatch(r"0x[0-9a-f]{40}", na=False)
    review = tokens["metadata_conflict"] | bad_decimals | bad_address
    tokens.loc[review, "family"] = None
    tokens.loc[review, "status"] = "metadata_review"

    tokens["family"] = tokens["family"].fillna("Autre")  # Les non identifiés
    tokens["token_kind"] = tokens["token_kind"].fillna("unknown")
    return tokens


def family_summary(tokens):
    """
    Compter les tokens et les transferts par famille, Autre compris.
    On mesure des fréquences d'apparition, pas des montants investis.
    """
    stats = tokens.groupby("family").agg(
        n_tokens=("token_address", "size"),
        n_rows=("n_rows", "sum"),
        n_tracked=("n_tracked", "sum"),
        n_zero=("n_zero", "sum"),
    )
    # Les autres lignes sont celles hors transactions Aave, conservées par le traçage.
    stats["n_other"] = stats["n_rows"] - stats["n_tracked"]
    stats["share_tokens_pct"] = 100 * stats["n_tokens"] / stats["n_tokens"].sum()
    stats["share_rows_pct"] = 100 * stats["n_rows"] / stats["n_rows"].sum()
    return stats.sort_values("n_rows", ascending=False)


def classify_transfers(path, output, tokens, batch_size=250000):
    """
    Reprendre les transferts et ajouter la famille, le type et le statut du token.
    On garde les lignes et les colonnes initiales, puis on écrit le nouveau Parquet.
    Comme pour l'inventaire, on travaille par lots pour limiter la mémoire utilisée.
    """
    # L'adresse devient l'index de la table de correspondance utilisée par map.
    labels = tokens.set_index("token_address")
    columns = ["family", "token_kind", "status"]
    parquet = pq.ParquetFile(path)

    # Le schéma décrit les colonnes et leurs types. On ajoute trois colonnes de texte.
    schema = parquet.schema_arrow
    for column in columns:
        schema = schema.append(pa.field(column, pa.string()))

    # Le with ferme le fichier une fois l'écriture terminée.
    # ParquetWriter permet d'ajouter les lots successifs dans un seul Parquet.
    with pq.ParquetWriter(output, schema, compression="snappy") as writer:
        for chunk in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([chunk])
            addresses = chunk.column("token_address").to_pandas().str.lower()

            # Chaque adresse retrouve sa famille, son type et son statut.
            # On ajoute ces colonnes sans toucher aux valeurs du tableau initial.
            for column in columns:
                values = addresses.map(labels[column])
                table = table.append_column(column, pa.array(values, type=pa.string()))
            writer.write_table(table)
