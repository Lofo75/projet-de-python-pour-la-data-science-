"""
Drop the transfers of the users that are clearly not wallets.

Some "users" of aave_v3_events.parquet are token contracts (e.g. USDC) or burn
addresses, not wallets. Their transfers (every transfer of the token, by
anyone) would swamp the data, so they are removed. A user is dropped if it is:
  - reserve: an Aave V3 reserve, i.e. in the reserve or collateral_asset
    column of EVENTS_FILE;
  - token:   an ERC-20 contract, i.e. in the token_address column of any
    transfer (catches aTokens, debt tokens and every other token that moved);
  - burn:    in BURN_ADDRESSES.
Addresses are compared case-insensitively. Contracts that are not tokens
(routers, bots, Uniswap PoolManager) are kept: telling them from wallets needs
eth_getCode.

The batches of IN_DIR are read twice, one at a time: once for the token
addresses, once to write them without the dropped users to OUT_DIR under the
same name, so the raw batches stay untouched. The dropped addresses, with
their reasons and number of transfers removed, are saved to DROPPED_FILE.
Usage:
    python3 drop_non_wallets.py
"""
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

EVENTS_FILE  = "s3://bastienlevyguinot/aave_v3/aave_v3_events.parquet"
IN_DIR       = "s3://bastienlevyguinot/aave_v3/transfers_parts"
OUT_DIR      = "s3://bastienlevyguinot/aave_v3/transfers_parts_wallets"
DROPPED_FILE = "s3://bastienlevyguinot/aave_v3/dropped_addresses.csv"

BURN_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",  # null address (mint / burn)
    "0x000000000000000000000000000000000000dead",
    "0xdead000000000000000042069420694206942069",
    *(f"0x{i:040x}" for i in range(1, 11)),        # precompiles 0x01-0x0a
}

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


def load_reserves():
    """Lowercased addresses of the reserves (borrowed, supplied or used as collateral)."""
    events = pd.read_parquet(EVENTS_FILE, columns=["reserve", "collateral_asset"])
    return set(pd.concat([events["reserve"], events["collateral_asset"]]).dropna().str.lower())


def load_tokens(fs, paths):
    """Lowercased token address -> symbol, for every token that appears in the batches."""
    tokens = {}
    for path in paths:
        with fs.open(path, "rb") as f:
            df = pq.read_table(f, columns=["token_address", "token_symbol"]).to_pandas()
        df = df.drop_duplicates("token_address")
        tokens.update(zip(df["token_address"].str.lower(), df["token_symbol"]))
    return tokens


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    fs.invalidate_cache(IN_DIR)
    paths = sorted(p for p in fs.glob(f"{IN_DIR}/batch_*.parquet") if BATCH_RE.search(p))

    reserves = load_reserves()
    tokens = load_tokens(fs, paths)
    print(f"{len(reserves)} reserves, {len(tokens):,} tokens, {len(BURN_ADDRESSES)} burn addresses")

    dropped = []  # one row per dropped user
    n_kept = 0
    for path in paths:
        with fs.open(path, "rb") as f:
            table = pq.read_table(f)
        users = pd.Series(table.column("user").to_pylist())
        lower = users.str.lower()
        is_dropped = lower.isin(reserves) | lower.isin(tokens) | lower.isin(BURN_ADDRESSES)
        rules = [("reserve", reserves), ("token", tokens), ("burn", BURN_ADDRESSES)]
        for user, n in users[is_dropped].value_counts().items():
            u = user.lower()
            reasons = [reason for reason, addresses in rules if u in addresses]
            dropped.append({"address": user, "reason": "+".join(reasons),
                            "token_symbol": tokens.get(u), "n_transfers": n})
        table = table.filter(pa.array(~is_dropped.to_numpy()))
        with fs.open(f"{OUT_DIR}/{path.rsplit('/', 1)[-1]}", "wb") as f:
            pq.write_table(table, f)
        n_kept += table.num_rows

    dropped = pd.DataFrame(dropped, columns=["address", "reason", "token_symbol", "n_transfers"])
    dropped = dropped.sort_values("n_transfers", ascending=False)
    with fs.open(DROPPED_FILE, "w") as f:
        dropped.to_csv(f, index=False)

    print(f"{len(paths)} batches written to {OUT_DIR}, {n_kept:,} transfers kept")
    print(f"{len(dropped)} addresses dropped ({dropped['n_transfers'].sum():,} transfers), saved to {DROPPED_FILE}")
    print(dropped["reason"].value_counts().to_string())
    print(dropped.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
