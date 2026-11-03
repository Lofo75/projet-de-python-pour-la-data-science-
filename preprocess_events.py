"""
Keep the Aave V3 events of wallets only.

Drops the events whose user is in DROPPED_FILE (reserves, tokens, burn
addresses and contracts, written by drop_non_wallets.py and
transfers_drop_contracts.py), and those of the null address, which
fetch_transfers_etherscan.py never fetched. Addresses are compared
case-insensitively. The result is saved to OUT_FILE, so its users are the
users of diffusion/transfers_wallets.parquet. Usage:
    python3 preprocess_events.py
"""
import pandas as pd
import s3fs

EVENTS_FILE  = "s3://bastienlevyguinot/aave_v3/aave_v3_events.parquet"
DROPPED_FILE = "s3://bastienlevyguinot/aave_v3/dropped_addresses.csv"
OUT_FILE     = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"

NULL_ADDRESS = "0x0000000000000000000000000000000000000000"


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    with fs.open(DROPPED_FILE, "r") as f:
        dropped = set(pd.read_csv(f)["address"].str.lower()) | {NULL_ADDRESS}

    events = pd.read_parquet(EVENTS_FILE)
    is_dropped = events["user"].str.lower().isin(dropped)
    kept = events[~is_dropped]
    with fs.open(OUT_FILE, "wb") as f:
        kept.to_parquet(f, index=False)

    print(f"{len(events):,} events, {is_dropped.sum():,} dropped "
          f"({events.loc[is_dropped, 'user'].nunique():,} users), {len(kept):,} saved to {OUT_FILE}")
    print(pd.DataFrame({"kept": kept["event_name"].value_counts(),
                        "dropped": events.loc[is_dropped, "event_name"].value_counts()}).to_string())


if __name__ == "__main__":
    main()
