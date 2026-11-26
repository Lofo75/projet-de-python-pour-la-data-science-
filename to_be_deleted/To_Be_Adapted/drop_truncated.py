"""
Drop the users whose transfer history was cut by MAX_CALLS_PER_USER.

Run after match_events_transfers.py. A user is truncated when one of its
events happened after the last transfer fetched for it (after_last_transfer
in CHECKS_FILE): its transfers stop partway through its history, so all its
data is dropped.

The truncated users are appended to DROPPED_FILE (reason "truncated"), and
their rows are removed from the batches of WALLETS_DIR (in place), from
TRANSFERS_FILE (which keeps its "tracked" column), from EVENTS_FILE and from
CHECKS_FILE. The users already listed as truncated in DROPPED_FILE are used
again, so running the script twice changes nothing. Usage:
    python3 drop_truncated.py
"""
import re

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import s3fs

WALLETS_DIR    = "s3://bastienlevyguinot/aave_v3/transfers_parts_wallets"
DROPPED_FILE   = "s3://bastienlevyguinot/aave_v3/dropped_addresses.csv"
CHECKS_FILE    = "s3://bastienlevyguinot/aave_v3/events_transfers_checks.parquet"
TRANSFERS_FILE = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
EVENTS_FILE    = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


def without_users(table, users):
    """Rows of table whose user is not in users."""
    return table.filter(pc.invert(pc.is_in(table.column("user"), pa.array(sorted(users), pa.string()))))


def filter_file(fs, path, users):
    """Remove the users' rows from a Parquet file, one row group at a time; returns (rows before, after)."""
    tmp = f"{path}.tmp"  # path is replaced only once complete
    n_before = n_after = 0
    with fs.open(path, "rb") as src, fs.open(tmp, "wb") as dst:
        reader = pq.ParquetFile(src)
        with pq.ParquetWriter(dst, reader.schema_arrow) as writer:
            for i in range(reader.num_row_groups):
                table = reader.read_row_group(i)
                kept = without_users(table, users)
                writer.write_table(kept)
                n_before += table.num_rows
                n_after += kept.num_rows
    fs.mv(tmp, path)
    return n_before, n_after


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    with fs.open(DROPPED_FILE, "r") as f:
        dropped = pd.read_csv(f)
    with fs.open(CHECKS_FILE, "rb") as f:
        checks = pd.read_parquet(f, columns=["user", "after_last_transfer"])
    truncated = set(checks.loc[checks["after_last_transfer"], "user"])
    truncated |= set(dropped.loc[dropped["reason"] == "truncated", "address"])
    print(f"{len(truncated)} truncated users")

    # Batches: remove the users and count their transfers for DROPPED_FILE
    fs.invalidate_cache(WALLETS_DIR)
    paths = sorted(p for p in fs.glob(f"{WALLETS_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    n_transfers = {}
    for path in paths:
        with fs.open(path, "rb") as f:
            table = pq.read_table(f)
        users = pd.Series(table.column("user").to_pylist())
        if not users.isin(truncated).any():
            continue
        n_transfers.update(users[users.isin(truncated)].value_counts())
        with fs.open(path, "wb") as f:
            pq.write_table(without_users(table, truncated), f)

    new = sorted(truncated - set(dropped.loc[dropped["reason"] == "truncated", "address"]))
    if new:
        new = pd.DataFrame({"address": new, "reason": "truncated"})
        new["n_transfers"] = new["address"].map(n_transfers).fillna(0).astype(int)
        dropped = pd.concat([dropped, new], ignore_index=True).sort_values("n_transfers", ascending=False)
        with fs.open(DROPPED_FILE, "w") as f:
            dropped.to_csv(f, index=False)
    print(f"{len(new)} users added to {DROPPED_FILE}, "
          f"{sum(n_transfers.values()):,} transfers removed from {WALLETS_DIR}")

    for path in [TRANSFERS_FILE, EVENTS_FILE, CHECKS_FILE]:
        n_before, n_after = filter_file(fs, path, truncated)
        print(f"{path}: {n_before - n_after:,} rows removed, {n_after:,} left")


if __name__ == "__main__":
    main()
