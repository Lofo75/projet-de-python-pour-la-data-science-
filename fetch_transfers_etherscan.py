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
SKIPPED_FILE = "s3://bastienlevyguinot/diffusion/skipped_users_during_transfers_fetching.parquet"

BATCH_SIZE    = 1_000   # users per saved Parquet part
MAX_ROWS      = 1_000   # rows per call
CALLS_PER_SEC = 2.8     # free plan: 3 calls/s, keep a margin
WORKERS       = 4       # parallel calls (the rate limiter is shared)
MAX_CALLS_PER_USER = 20 # contracts/bots (burn address, DEX pools, MEV) have millions of transfers:
                        # an address still truncated after 20 calls (~20,000 transfers) is skipped

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
    ("value",          pa.string()), # value is kept exact as a string (divide by 10**token_decimal for token units)
])
INT_COLUMNS = [f.name for f in SCHEMA if f.type == pa.int64()]

# Addresses with no transfer saved, so that they are not mistaken for users with zero transfers.
# reason: "too_many_transfers" (still truncated after MAX_CALLS_PER_USER calls)
#      or "big_block" (MAX_ROWS+ transfers in a single block)
SKIPPED_SCHEMA = pa.schema([
    ("user",           pa.string()),
    ("reason",         pa.string()),
    ("transfers_seen", pa.int64()),  # complete-block transfers fetched before giving up
    ("block_reached",  pa.int64()),  # block where the fetching stopped
])

NULL_ADDRESS = "0x0000000000000000000000000000000000000000"


class RateLimiter:

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
    """One Etherscan call with backoff on rate limits and errors"""
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
            if "jsonrpc" in data and "error" not in data and isinstance(result, str) and result.startswith("0x"):
                return result  # proxy module answers in JSON-RPC format, without a status field
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
    """All ERC-20 transfers of one address between START_BLOCK and end_block.
    Returns (rows, None) when the address is complete, or ([], skip) when it is skipped: an address is either complete or absent, never partial.
    """
    rows = []
    start = START_BLOCK
    for _ in range(MAX_CALLS_PER_USER):
        result = call(session, limiter, api_key, {
            "module": "account", "action": "tokentx", "address": user,
            "startblock": start, "endblock": end_block,
            "page": 1, "offset": MAX_ROWS, "sort": "asc",
        })
        if len(result) < MAX_ROWS:
            rows += result
            return [{"user": user, **{col: t.get(field) for field, col in FIELDS.items()}} for t in rows], None
        # Truncated: keep the complete blocks, start again from the last one
        last = int(result[-1]["blockNumber"])
        complete = [t for t in result if int(t["blockNumber"]) < last]
        if not complete:  # MAX_ROWS+ transfers in a single block: a contract, not a wallet
            return [], {"user": user, "reason": "big_block", "transfers_seen": len(rows), "block_reached": last}
        rows += complete
        start = last
    return [], {"user": user, "reason": "too_many_transfers", "transfers_seen": len(rows), "block_reached": start}


def to_table(rows):
    df = pd.DataFrame(rows, columns=SCHEMA.names)
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def saved_parts(fs, prefix):
    """Indexes of the parts <prefix>_XXXXX.parquet already saved in OUT_DIR."""
    fs.invalidate_cache(OUT_DIR)
    pattern = re.compile(rf"{prefix}_(\d+)\.parquet$")
    return {int(m.group(1)) for p in fs.glob(f"{OUT_DIR}/{prefix}_*.parquet") if (m := pattern.search(p))}


def merge_parts(fs, prefix, n_batches, schema, out_file):
    """Concatenate the parts into out_file one at a time, to keep memory low."""
    n_rows = 0
    with fs.open(out_file, "wb") as f, pq.ParquetWriter(f, schema) as writer:
        for i in range(n_batches):
            with fs.open(f"{OUT_DIR}/{prefix}_{i:05d}.parquet", "rb") as part:
                table = pq.read_table(part, schema=schema)
            writer.write_table(table)
            n_rows += table.num_rows
    return n_rows


def main():
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

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    batches = [users[i:i + BATCH_SIZE] for i in range(0, len(users), BATCH_SIZE)]
    done = saved_parts(fs, "batch") & saved_parts(fs, "skipped")
    todo = [i for i in range(len(batches)) if i not in done]
    if done:
        print(f"Resuming: {len(done)} of {len(batches)} batches already saved")

    n_transfers = n_skipped = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for k, i in enumerate(todo, 1):
            results = list(executor.map(fetch, batches[i]))
            table = to_table([r for user_rows, _ in results for r in user_rows])
            skipped = pa.Table.from_pylist([s for _, s in results if s], schema=SKIPPED_SCHEMA)
            with fs.open(f"{OUT_DIR}/skipped_{i:05d}.parquet", "wb") as f:
                pq.write_table(skipped, f)
            with fs.open(f"{OUT_DIR}/batch_{i:05d}.parquet", "wb") as f:
                pq.write_table(table, f)

            n_transfers += table.num_rows
            n_skipped += skipped.num_rows
            elapsed = time.time() - t0
            eta_h = elapsed / k * (len(todo) - k) / 3600
            print(f"[{(len(done) + k) / len(batches):6.1%}] batch {i:05d} | "
                  f"{table.num_rows:7,d} transfers (total {n_transfers:,}) | "
                  f"{skipped.num_rows:3d} skipped (total {n_skipped:,}) | ETA {eta_h:.1f} h", flush=True)

    n_rows = merge_parts(fs, "batch", len(batches), SCHEMA, OUT_FILE)
    print(f"Saved {n_rows:,} transfers to {OUT_FILE}")
    n_rows = merge_parts(fs, "skipped", len(batches), SKIPPED_SCHEMA, SKIPPED_FILE)
    print(f"Saved {n_rows:,} skipped users to {SKIPPED_FILE}")


if __name__ == "__main__":
    main()
