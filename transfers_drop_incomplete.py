"""
Drop the users whose transfer history was cut by MAX_CALLS_PER_USER but who
have no event after the cut (drop_truncated.py only catches those who have).

Run after drop_truncated.py. Every user with at least MIN_TRANSFERS transfers
in TRANSFERS_FILE is checked with one Etherscan call: any transfer between the
block after its last saved transfer and END_BLOCK (the last block of the
fetch) means its history is incomplete. fetch_transfers_etherscan.py only
keeps complete blocks, so nothing of the last saved block is missing.

The incomplete users are appended to DROPPED_FILE (reason "incomplete") and
their rows removed from the batches of WALLETS_DIR, TRANSFERS_FILE,
EVENTS_FILE and CHECKS_FILE, as in drop_truncated.py. The Etherscan key is
read from the ETHERSCAN_KEY environment variable. Usage:
    python3 transfers_drop_incomplete.py
"""
import os
import re

import pandas as pd
import pyarrow.parquet as pq
import requests
import s3fs

from drop_truncated import filter_file, without_users
from fetch_transfers_etherscan import CALLS_PER_SEC, RateLimiter, call

WALLETS_DIR    = "s3://bastienlevyguinot/aave_v3/transfers_parts_wallets"
DROPPED_FILE   = "s3://bastienlevyguinot/aave_v3/dropped_addresses.csv"
CHECKS_FILE    = "s3://bastienlevyguinot/aave_v3/events_transfers_checks.parquet"
TRANSFERS_FILE = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
EVENTS_FILE    = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"

END_BLOCK     = 26_291_614  # last block of the fetch_transfers_etherscan.py run
MIN_TRANSFERS = 15_000      # users below cannot have hit MAX_CALLS_PER_USER (20 calls of up to 1,000 rows)

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


def heavy_users(fs):
    """user -> (number of transfers, last block) for the users with MIN_TRANSFERS+ transfers."""
    with fs.open(TRANSFERS_FILE, "rb") as f:
        table = pq.read_table(f, columns=["user", "block_number"])
    stats = table.group_by("user").aggregate([("block_number", "count"), ("block_number", "max")]).to_pandas()
    stats = stats.rename(columns={"block_number_count": "n_transfers", "block_number_max": "last_block"})
    return stats[stats["n_transfers"] >= MIN_TRANSFERS].sort_values("n_transfers", ascending=False)


def main():
    api_key = os.environ.get("ETHERSCAN_KEY", "").strip()
    if not api_key:
        raise SystemExit("ETHERSCAN_KEY is not set: add it as an Onyxia secret and restart the service")

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    candidates = heavy_users(fs)
    print(f"{len(candidates)} users with {MIN_TRANSFERS:,}+ transfers to check")

    session = requests.Session()
    limiter = RateLimiter(CALLS_PER_SEC)
    candidates["missing_after"] = [
        len(call(session, limiter, api_key, {
            "module": "account", "action": "tokentx", "address": user,
            "startblock": last + 1, "endblock": END_BLOCK, "page": 1, "offset": 1, "sort": "asc",
        })) > 0
        for user, last in zip(candidates["user"], candidates["last_block"])
    ]
    incomplete = set(candidates.loc[candidates["missing_after"], "user"])
    print(f"{len(incomplete)} incomplete users")
    if not incomplete:
        return

    fs.invalidate_cache(WALLETS_DIR)
    paths = sorted(p for p in fs.glob(f"{WALLETS_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    for path in paths:
        with fs.open(path, "rb") as f:
            table = pq.read_table(f)
        if pd.Series(table.column("user").to_pylist()).isin(incomplete).any():
            with fs.open(path, "wb") as f:
                pq.write_table(without_users(table, incomplete), f)

    with fs.open(DROPPED_FILE, "r") as f:
        dropped = pd.read_csv(f)
    new = candidates.loc[candidates["missing_after"], ["user", "n_transfers"]].rename(columns={"user": "address"})
    dropped = pd.concat([dropped, new.assign(reason="incomplete")], ignore_index=True)
    dropped = dropped.sort_values("n_transfers", ascending=False)
    with fs.open(DROPPED_FILE, "w") as f:
        dropped.to_csv(f, index=False)
    print(f"{len(new)} users added to {DROPPED_FILE}, {new['n_transfers'].sum():,} transfers removed")

    for path in [TRANSFERS_FILE, EVENTS_FILE, CHECKS_FILE]:
        n_before, n_after = filter_file(fs, path, incomplete)
        print(f"{path}: {n_before - n_after:,} rows removed, {n_after:,} left")


if __name__ == "__main__":
    main()
