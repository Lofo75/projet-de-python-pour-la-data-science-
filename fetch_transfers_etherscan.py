"""
Fetch every ERC-20 transfer (sent and received) of each Aave V3 user with the
Etherscan V2 API (module=account, action=tokentx) and save them to a single
Parquet file on the Onyxia S3 (MinIO) storage.

Users are the unique "user" addresses of EVENTS_FILE, except the null address
(mint/burn counterparty, not a wallet). They are processed in
batches of BATCH_SIZE addresses, each saved as one Parquet part under OUT_DIR
(with a "user" column), so an interrupted run resumes after the last saved
batch. Once every batch is fetched they are merged into OUT_FILE.

Etherscan returns at most 1,000 rows per call. When an address has more, the
rows of the last (possibly incomplete) block are dropped and the next call
starts from that block, until the whole range is covered.

A transfer between two Aave users appears twice, once for each user.

The Etherscan key is read from the ETHERSCAN_API_KEY environment variable
(an Onyxia secret, not the .env file). S3 credentials and endpoint are read
from the AWS_* environment variables that Onyxia injects into the service.

All settings are the constants below. Usage:
    python3 fetch_transfers_etherscan.py            # full run (resumes automatically)
    python3 fetch_transfers_etherscan.py --test 20  # fetch 20 users, print stats, save nothing
"""
import argparse
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs

ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1
START_BLOCK  = 16_496_817  # first Aave V3 Pool event

EVENTS_FILE = "s3://bastienlevyguinot/aave_v3/aave_v3_events.parquet"
OUT_DIR     = "s3://bastienlevyguinot/aave_v3/transfers_parts"
OUT_FILE    = "s3://bastienlevyguinot/diffusion/transfers.parquet"

BATCH_SIZE    = 1_000   # users per saved Parquet part
MAX_ROWS      = 1_000   # Etherscan cap on rows per call
CALLS_PER_SEC = 4       # free plan: 5 calls/s, keep a margin
WORKERS       = 4       # parallel calls (the rate limiter is shared)

# Etherscan field -> column name (snake_case, as in aave_v3_events.parquet).
# value is kept exact as a string (divide by 10**token_decimal for token units).
FIELDS = {
    "blockNumber":      "block_number",
    "timeStamp":        "timestamp",
    "hash":             "tx_hash",
    "from":             "from",
    "to":               "to",
    "contractAddress":  "token_address",
    "tokenSymbol":      "token_symbol",
    "tokenName":        "token_name",
    "tokenDecimal":     "token_decimal",
    "value":            "value",
}
SCHEMA = pa.schema([
    ("user",           pa.string()),
    ("block_number",   pa.int64()),
    ("timestamp",      pa.int64()),
    ("tx_hash",        pa.string()),
    ("from",           pa.string()),
    ("to",             pa.string()),
    ("token_address",  pa.string()),
    ("token_symbol",   pa.string()),
    ("token_name",     pa.string()),
    ("token_decimal",  pa.int64()),
    ("value",          pa.string()),
])
INT_COLUMNS = [f.name for f in SCHEMA if f.type == pa.int64()]

NULL_ADDRESS = "0x0000000000000000000000000000000000000000"

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


class RateLimiter:
    """Spaces calls at least 1 / calls_per_sec apart, across all threads."""

    def __init__(self, calls_per_sec):
        self.interval = 1 / calls_per_sec
        self.next_time = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            wait = self.next_time - now
            self.next_time = max(now, self.next_time) + self.interval
        if wait > 0:
            time.sleep(wait)


def call(session, limiter, api_key, params, max_retries=10):
    """One Etherscan call with backoff on rate limits and errors; waits out the daily quota."""
    params = {"chainid": ETH_CHAIN_ID, "apikey": api_key, **params}
    attempt = 0
    while True:
        limiter.wait()
        try:
            r = session.get(ETHERSCAN_BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            error = str(e)
        else:
            result = data.get("result")
            if data.get("status") == "1" or isinstance(result, list):
                return result  # "No transactions found" comes with status 0 and an empty list
            error = f"{data.get('message')}: {result}"
            if "daily" in error.lower():
                print(f"Daily quota reached ({error}), waiting 1 h", flush=True)
                time.sleep(3600)
                continue
            if "invalid api key" in error.lower():
                raise SystemExit(f"Etherscan refused the key: {error}")
        attempt += 1
        if attempt == max_retries:
            raise RuntimeError(f"Etherscan call failed for {params.get('address')}: {error}")
        time.sleep(min(2 ** attempt, 60) * random.uniform(0.5, 1.5))


def fetch_user(session, limiter, api_key, user, end_block):
    """All ERC-20 transfers of one address between START_BLOCK and end_block."""
    rows = []
    start = START_BLOCK
    while True:
        result = call(session, limiter, api_key, {
            "module": "account", "action": "tokentx", "address": user,
            "startblock": start, "endblock": end_block,
            "page": 1, "offset": MAX_ROWS, "sort": "asc",
        })
        if len(result) < MAX_ROWS:
            rows += result
            break
        # Truncated: keep the complete blocks, start again from the last one
        last = int(result[-1]["blockNumber"])
        complete = [t for t in result if int(t["blockNumber"]) < last]
        if not complete:  # a single block with MAX_ROWS transfers: cannot split further
            print(f"Warning: {user} has >= {MAX_ROWS} transfers in block {last}, some are missing", flush=True)
            rows += result
            start = last + 1
        else:
            rows += complete
            start = last
    return [{"user": user, **{col: t.get(field) for field, col in FIELDS.items()}} for t in rows]


def to_table(rows):
    df = pd.DataFrame(rows, columns=SCHEMA.names)
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # "" -> NA
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def done_batches(fs):
    """Indexes of the batches already saved in OUT_DIR."""
    fs.invalidate_cache(OUT_DIR)
    return {int(m.group(1)) for p in fs.glob(f"{OUT_DIR}/batch_*.parquet") if (m := BATCH_RE.search(p))}


def merge_batches(fs, n_batches):
    """Concatenate the batches into OUT_FILE one at a time, to keep memory low."""
    n_rows = 0
    with fs.open(OUT_FILE, "wb") as f, pq.ParquetWriter(f, SCHEMA) as writer:
        for i in range(n_batches):
            with fs.open(f"{OUT_DIR}/batch_{i:05d}.parquet", "rb") as part:
                table = pq.read_table(part, schema=SCHEMA)
            writer.write_table(table)
            n_rows += table.num_rows
    return n_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--test", type=int, metavar="N", help="fetch only N users and save nothing")
    args = parser.parse_args()

    api_key = os.environ.get("ETHERSCAN_KEY", "").strip()
    if not api_key:
        raise SystemExit("ETHERSCAN_KEY is not set: add it as an Onyxia secret and restart the service")

    session = requests.Session()
    limiter = RateLimiter(CALLS_PER_SEC)
    end_block = int(call(session, limiter, api_key, {"module": "proxy", "action": "eth_blockNumber"}), 16)

    users = pd.read_parquet(EVENTS_FILE, columns=["user"])["user"].dropna().unique()
    users = sorted(u for u in users if u.lower() != NULL_ADDRESS)
    print(f"{len(users):,} users, blocks {START_BLOCK:,}-{end_block:,}")

    def fetch(user):
        return fetch_user(session, limiter, api_key, user, end_block)

    if args.test:
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            rows = [r for batch in executor.map(fetch, users[:args.test]) for r in batch]
        elapsed = time.time() - t0
        df = to_table(rows).to_pandas()
        print(df.groupby("user").size().describe().to_string())
        print(df.head().to_string())
        print(f"{len(df):,} transfers for {args.test} users in {elapsed:.1f} s "
              f"-> full run ETA {elapsed / args.test * len(users) / 3600:.1f} h")
        return

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    batches = [users[i:i + BATCH_SIZE] for i in range(0, len(users), BATCH_SIZE)]
    done = done_batches(fs)
    todo = [i for i in range(len(batches)) if i not in done]
    if done:
        print(f"Resuming: {len(done)} of {len(batches)} batches already saved")

    n_transfers = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for k, i in enumerate(todo, 1):
            rows = [r for batch in executor.map(fetch, batches[i]) for r in batch]
            table = to_table(rows)
            with fs.open(f"{OUT_DIR}/batch_{i:05d}.parquet", "wb") as f:
                pq.write_table(table, f)

            n_transfers += table.num_rows
            elapsed = time.time() - t0
            eta_h = elapsed / k * (len(todo) - k) / 3600
            print(f"[{(len(done) + k) / len(batches):6.1%}] batch {i:05d} | "
                  f"{table.num_rows:7,d} transfers (total {n_transfers:,}) | ETA {eta_h:.1f} h", flush=True)

    n_rows = merge_batches(fs, len(batches))
    print(f"Saved {n_rows:,} transfers to {OUT_FILE}")


if __name__ == "__main__":
    main()
