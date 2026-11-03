"""
Completeness checks on the batches saved by fetch_transfers_etherscan.py.

1. Capped blocks: (user, block) with exactly MAX_WINDOW (10,000) transfers.
   fetch_transfers_etherscan.py pages through a block with 1,000+ transfers of
   one address on its own and stops at Etherscan's cap of 10,000 rows, so such
   a block was very likely truncated, while every block below is complete.
2. Paged blocks: (user, block) with MAX_ROWS (1,000) transfers or more. Their
   rows were fetched over several pages, which assumes Etherscan returns them
   in a stable order; otherwise rows may be duplicated or skipped.
3. Missing values: NA counts in the integer columns (to_table turns values
   that cannot be parsed into NA instead of failing).

The batches are read one at a time from OUT_DIR (a user is in a single batch,
so its blocks are never split across batches), which also works while the
fetch is still running. Usage:
    python3 transfers_fetching_checks.py
"""
import re

import pandas as pd
import pyarrow.parquet as pq
import s3fs

OUT_DIR = "s3://bastienlevyguinot/aave_v3/transfers_parts"

MAX_ROWS   = 1_000   # Etherscan cap on rows per call
MAX_WINDOW = 10_000  # Etherscan cap on page * offset for one query

INT_COLUMNS = ["block_number", "timestamp", "token_decimal"]

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


def scan_batches(fs):
    """(user, block_number, n_transfers) of the blocks with MAX_ROWS+ transfers of the user, and NA counts."""
    fs.invalidate_cache(OUT_DIR)
    paths = sorted(p for p in fs.glob(f"{OUT_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    paged = []
    n_na = pd.Series(0, index=INT_COLUMNS)
    n_rows = 0
    for path in paths:
        with fs.open(path, "rb") as f:
            df = pq.read_table(f, columns=["user", *INT_COLUMNS]).to_pandas()
        counts = df.groupby(["user", "block_number"]).size()
        paged.append(counts[counts >= MAX_ROWS])
        n_na += df[INT_COLUMNS].isna().sum()
        n_rows += len(df)
    print(f"{len(paths)} batches read, {n_rows:,} transfers")
    paged = pd.concat(paged) if paged else pd.Series(dtype="int64")
    return paged.rename("n_transfers").reset_index(), n_na, n_rows


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    paged, n_na, n_rows = scan_batches(fs)

    capped = paged[paged["n_transfers"] == MAX_WINDOW]
    print(f"\n1. {len(capped):,} blocks with exactly {MAX_WINDOW:,} transfers, "
          f"{capped['user'].nunique():,} addresses")
    if len(capped):
        print(capped.to_string(index=False))

    print(f"\n2. {len(paged):,} blocks with {MAX_ROWS:,}+ transfers, {paged['user'].nunique():,} addresses")
    if len(paged):
        print(paged.to_string(index=False))

    print("\n3. Missing values")
    print(pd.DataFrame({"n_na": n_na, "share": n_na / max(n_rows, 1)}).to_string())

    return sorted(capped["user"].unique())


if __name__ == "__main__":
    main()
