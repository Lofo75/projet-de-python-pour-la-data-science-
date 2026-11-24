"""
Construire un inventaire de tokens, puis leur associer une famille économique.
Périmètre : Ethereum, après le traçage documenté dans README_FILTERING.md.
Le statut distingue le classement par adresse du classement par symbole.
"""

import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Adresse du contrat WETH sur Ethereum. Le pipeline y range aussi l'ETH natif.
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def token_inventory(path, batch_size=250000):
    """Compter les lignes par contrat en conservant les variantes de métadonnées.
    En gros : on transforme un tableau de transferts en un tableau de tokens, avec une ligne par adresse, des comptages et des infos sur le token.

    Le fichier contient 9 millions de transferts, mais les mêmes tokens reviennent énormément.
    On extrait donc les tokens !
    """

    metadata = ["token_address", "token_symbol", "token_name", "token_decimal"]
    columns = metadata + ["tracked", "value"]
    counts = []
    parquet = pq.ParquetFile(path)

    # On traite 250 000 lignes à la fois ; chaque lot devient un DataFrame pandas.
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        part = batch.to_pandas()
        part["token_address"] = part["token_address"].str.lower()
        part["token_symbol"] = part["token_symbol"].fillna("")
        part["token_name"] = part["token_name"].fillna("")
        part["is_zero"] = part["value"].str.fullmatch(r"0+").fillna(False)

        # size compte toutes les lignes ; sum compte les True des colonnes booléennes.
        # On distingue encore les variantes de nom ou de décimales d'une adresse.
        batch_counts = part.groupby(metadata, dropna=False).agg(
            n_rows=("tracked", "size"),
            n_tracked=("tracked", "sum"),
            n_zero=("is_zero", "sum"),
        ).reset_index()
        counts.append(batch_counts)

    # Un token peut apparaître dans plusieurs lots, donc on additionne leurs comptages.
    variants = pd.concat(counts, ignore_index=True)
    variants = variants.groupby(metadata, dropna=False, as_index=False)[
        ["n_rows", "n_tracked", "n_zero"]
    ].sum()

    rows = []
    # On construit maintenant une seule ligne par adresse de token.
    for address, group in variants.groupby("token_address", sort=True):
        # Le libellé le plus fréquent sert à l'affichage ; les variantes restent.
        main = group.sort_values(
            ["n_rows", "token_symbol", "token_name"], ascending=[False, True, True]
        ).iloc[0]
        symbols = sorted(group["token_symbol"].unique())
        names = sorted(group["token_name"].unique())
        decimals = sorted(int(x) for x in group["token_decimal"].dropna().unique())
        # Plusieurs libellés ou décimales pour une adresse demandent une vérification.
        conflict = len(symbols) != 1 or len(names) != 1 or len(decimals) != 1
        conflict = conflict or group["token_decimal"].isna().any()
        # Le pipeline du groupe a volontairement rangé ETH sous l'adresse WETH.
        if (address == WETH and set(symbols) <= {"ETH", "WETH"}
                and set(names) <= {"Ether", "Wrapped Ether"} and decimals == [18]):
            conflict = False
        rows.append({
            "token_address": address,
            "token_symbol": main["token_symbol"],
            "token_name": main["token_name"],
            "token_decimal": decimals[0] if len(decimals) == 1 else None,
            "symbols": symbols,
            "names": names,
            "decimals": decimals,
            "metadata_conflict": bool(conflict),
            "n_rows": int(group["n_rows"].sum()),
            "n_tracked": int(group["n_tracked"].sum()),
            "n_zero": int(group["n_zero"].sum()),
        })
    return pd.DataFrame(rows).sort_values(
        ["n_rows", "token_address"], ascending=[False, True], ignore_index=True
    )


def propose_family(symbol, aliases):
    """Trouver une famille par alias exact, ou par préfixe Aave.

    Exemple : DAI -> USD ; aEthUSDC -> USDC -> USD. Le préfixe aEth désigne
    ici un reçu Aave sur Ethereum, pas une exposition à ETH.
    """
    symbol = (symbol or "").strip()
    if symbol.upper() in aliases:
        return aliases[symbol.upper()]["family"]
    match = re.fullmatch(
        r"(?:variableDebt|stableDebt|a)(?:EthLido|Eth|HorRwa)?(.+)", symbol
    )
    if match and match[1].upper() in aliases:
        return aliases[match[1].upper()]["family"]
    return None


def classify_tokens(inventory, reference, aliases):
    """Classer par adresse puis accepter le nom si l'adresse n'est pas référencée.

    reference est une table figée, sourcée, indépendante des comptages.
    Une décimale contradictoire ou plusieurs métadonnées bloquent l'affectation.
    """
    inventory = inventory.copy()
    reference = reference.copy()
    inventory["token_address"] = inventory["token_address"].str.lower()
    reference["token_address"] = reference["token_address"].str.lower()
    result = inventory.merge(reference, on="token_address", how="left", validate="one_to_one")
    result["proposed_family"] = result["token_symbol"].map(
        lambda symbol: propose_family(symbol, aliases)
    )
    by_address = result["family"].notna()
    by_name = ~by_address & result["proposed_family"].notna()
    result["status"] = "unresolved"
    result.loc[by_address, "status"] = "reference_match"
    result.loc[by_name, "family"] = result.loc[by_name, "proposed_family"]
    result.loc[by_name, "status"] = "name_match"

    # Le type reste utile : un reçu ou une dette n'est pas le token sous-jacent.
    symbols = result["token_symbol"].str.strip()
    kinds = {symbol: info.get("token_kind", "unknown") for symbol, info in aliases.items()}
    name_kind = symbols.str.upper().map(kinds).fillna("unknown")
    aave_prefix = ~symbols.str.upper().isin(aliases) & by_name
    name_kind.loc[aave_prefix & symbols.str.startswith("a")] = "deposit_receipt"
    name_kind.loc[aave_prefix & symbols.str.startswith(("variableDebt", "stableDebt"))] = "debt"
    result.loc[by_name, "token_kind"] = name_kind[by_name]

    wrong_decimal = (result["expected_decimals"].notna()
                     & result["token_decimal"].ne(result["expected_decimals"]))
    invalid_address = ~result["token_address"].str.fullmatch(r"0x[0-9a-f]{40}", na=False)
    review = result["metadata_conflict"] | wrong_decimal | invalid_address
    result.loc[review, "family"] = None
    result.loc[review, "status"] = "metadata_review"
    result["family"] = result["family"].fillna("Autre")
    result["token_kind"] = result["token_kind"].fillna("unknown")
    return result


def family_summary(tokens):
    """Résumer des fréquences de lignes, sans les interpréter comme des montants."""
    summary = tokens.groupby("family").agg(
        n_tokens=("token_address", "size"),
        n_rows=("n_rows", "sum"),
        n_tracked=("n_tracked", "sum"),
        n_zero=("n_zero", "sum"),
    )
    summary["n_other"] = summary["n_rows"] - summary["n_tracked"]
    summary["share_tokens_pct"] = 100 * summary["n_tokens"] / summary["n_tokens"].sum()
    summary["share_rows_pct"] = 100 * summary["n_rows"] / summary["n_rows"].sum()
    return summary.sort_values("n_rows", ascending=False)


def classify_transfers(path, destination, tokens, batch_size=250000):
    """Ajouter famille, type et statut aux transferts, puis écrire un Parquet.

    On garde toutes les lignes et colonnes initiales. La lecture par lots évite
    de charger les 9 millions de transferts en mémoire d'un seul coup.
    """
    labels = tokens.set_index("token_address")
    columns = ["family", "token_kind", "status"]
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    for column in columns:
        schema = schema.append(pa.field(column, pa.string()))

    with pq.ParquetWriter(destination, schema, compression="snappy") as writer:
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            # map retrouve la classification à partir de l'adresse du token.
            addresses = batch.column("token_address").to_pandas().str.lower()
            for column in columns:
                values = addresses.map(labels[column])
                table = table.append_column(column, pa.array(values, type=pa.string()))
            writer.write_table(table)
