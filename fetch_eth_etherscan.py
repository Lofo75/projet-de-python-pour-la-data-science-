"""
Fetch the native ETH flows of every wallet with the Etherscan V2 API and save
them to a single Parquet file on the Onyxia S3 (MinIO) storage.

tokentx (fetch_transfers_etherscan.py) only returns ERC-20 transfers. Native
ETH moves in two ways, one Etherscan action each (the "kind" column):
  - "normal"   (action=txlist): the transactions sent or received by the
    address. value is the ETH sent by "from" to "to" (0 for most calls).
    Every transaction is kept, value 0 included: method_id and "to" tell
    which contract the user called (a plain ERC-20 transfer() is 0xa9059cbb).
  - "internal" (action=txlistinternal): ETH sent by a contract during a
    transaction, e.g. a router paying out the ETH of a swap, or the Aave
    WrappedTokenGateway paying out a borrow or a withdrawal in ETH.
A failed transaction or call moves no ETH: is_error = 1 (gas is still paid,
see gas_used * gas_price for normal transactions).

Users are the wallets kept by the previous steps: the users of
WALLETS_EVENTS_FILE, the same set as transfers_wallets.parquet (contracts,
tokens, burn addresses, truncated and incomplete users are already dropped,
see dropped_addresses.csv). The block range is the same as the ERC-20
transfers: START_BLOCK up to the last block of TRANSFERS_FILE.

Paging works as in fetch_transfers_etherscan.py (1,000 rows per call, the
rows of the last block dropped and fetched again from that block), and each
(user, kind) is cut after MAX_CALLS_PER_USER calls. The cut users are
listed in STATUS_DIR with their number of rows, so that they can be dropped
later. Users are processed in batches of BATCH_SIZE; each batch is saved as
one part under OUT_DIR, so an interrupted run resumes after the last saved
batch. At the end the parts are merged into OUT_FILE, each user's rows being
contiguous (normal then internal, each in ascending block order).

Etherscan keys are read from the .env file (or the environment): every
variable named ETHERSCAN_API_KEY or ETHERSCAN_API_KEY_<n>, plus ETHERSCAN_KEY
(the Onyxia secret). The free plan allows 3 calls/s and 100,000 calls/day per
key. A key that hits a rate limit hands over to the next one; a key that
hits its daily quota rests for KEY_REST seconds, and the run waits only when
every key is resting. With at least 2 calls per user (~351k calls) and two
keys, the full run takes about 2 days. S3
credentials come from the AWS_* environment variables injected by Onyxia.

Usage:
    python3 fetch_eth_etherscan.py            # full run (resumes automatically)
    python3 fetch_eth_etherscan.py --test 20  # fetch 20 users, print stats, save nothing
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
from dotenv import load_dotenv

from fetch_transfers_etherscan import (CALLS_PER_SEC, ETH_CHAIN_ID, ETHERSCAN_BASE_URL, MAX_ROWS,
                                       MAX_WINDOW, START_BLOCK, RateLimiter)

WALLETS_EVENTS_FILE = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"
TRANSFERS_FILE      = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
OUT_DIR    = "s3://bastienlevyguinot/aave_v3/eth_parts"
STATUS_DIR = "s3://bastienlevyguinot/aave_v3/eth_status_parts"
OUT_FILE   = "s3://bastienlevyguinot/diffusion/eth_transfers_wallets.parquet"

BATCH_SIZE         = 1_000  # users per saved Parquet part
WORKERS            = 4      # parallel calls (the rate limiter is shared)
MAX_CALLS_PER_USER = 20     # per (user, kind): keep the first ~20,000 rows of bots
KEY_REST           = 3600   # seconds a key rests after hitting its daily quota
KEY_RE = re.compile(r"ETHERSCAN_API_KEY(_\d+)?$")

ACTIONS = {"normal": "txlist", "internal": "txlistinternal"}

# Etherscan field -> column name. value is in wei, kept exact as a string.
FIELDS = {
    "blockNumber": "block_number",
    "timeStamp":   "timestamp",
    "hash":        "tx_hash",
    "from":        "from",
    "to":          "to",
    "value":       "value",
    "isError":     "is_error",
    "methodId":    "method_id",    # normal only
    "gasUsed":     "gas_used",
    "gasPrice":    "gas_price",    # normal only
    "traceId":     "trace_id",     # internal only: position of the call in the transaction
    "type":        "call_type",    # internal only: call, create, ...
    "contractAddress": "contract_address",  # address created by the transaction, if any
}
SCHEMA = pa.schema([
    ("user",             pa.string()),
    ("kind",             pa.string()),
    ("block_number",     pa.int64()),
    ("timestamp",        pa.int64()),
    ("tx_hash",          pa.string()),
    ("from",             pa.string()),
    ("to",               pa.string()),
    ("value",            pa.string()),
    ("is_error",         pa.int64()),
    ("method_id",        pa.string()),
    ("gas_used",         pa.string()),
    ("gas_price",        pa.string()),
    ("trace_id",         pa.string()),
    ("call_type",        pa.string()),
    ("contract_address", pa.string()),
])
INT_COLUMNS = [f.name for f in SCHEMA if f.type == pa.int64()]
STATUS_SCHEMA = pa.schema([("user", pa.string()), ("kind", pa.string()),
                           ("n_rows", pa.int64()), ("truncated", pa.bool_())])


class KeyPool:
    """Etherscan keys used one at a time, shared by all threads: on a rate limit the next key
    takes over, a key at its daily quota rests for KEY_REST seconds."""

    def __init__(self, keys):
        self.keys = dict(keys)  # name -> key, in order of use (names are printed, never keys)
        self.names = list(self.keys)
        self.current = 0
        self.resting = {}  # name -> time.monotonic() at which it can be used again
        self.lock = threading.Lock()

    def get(self):
        """(name, key) of the current key, waiting if every key is resting."""
        while True:
            with self.lock:
                if not self.names:
                    raise SystemExit("Etherscan refused every key")
                now = time.monotonic()
                for j in range(len(self.names)):
                    k = (self.current + j) % len(self.names)
                    name = self.names[k]
                    if self.resting.get(name, 0) <= now:
                        self.current = k
                        return name, self.keys[name]
                wait = min(self.resting[n] for n in self.names) - now
            print(f"Every key is at its daily quota, waiting {wait / 60:.0f} min", flush=True)
            time.sleep(wait)

    def switch(self, name):
        """Hand over to the next key (if name is still the current one)."""
        with self.lock:
            if self.names and self.names[self.current % len(self.names)] == name:
                self.current = (self.current + 1) % len(self.names)

    def rest(self, name, seconds):
        with self.lock:
            if self.resting.get(name, 0) <= time.monotonic():
                print(f"{name} reached its daily quota: resting {seconds / 60:.0f} min", flush=True)
            self.resting[name] = time.monotonic() + seconds
        self.switch(name)

    def drop(self, name):
        with self.lock:
            if name in self.names:
                print(f"{name} refused by Etherscan: no longer used", flush=True)
                self.names.remove(name)
                self.current = 0


def call(session, limiter, keys, params, max_retries=10):
    """One Etherscan call with backoff on errors, switching keys on rate limits."""
    attempt = 0
    while True:
        name, key = keys.get()
        limiter.wait()
        try:
            r = session.get(ETHERSCAN_BASE_URL, timeout=30,
                            params={"chainid": ETH_CHAIN_ID, "apikey": key, **params})
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            error = str(e)
        else:
            result = data.get("result")
            if data.get("status") == "1" or isinstance(result, list):
                return result  # "No transactions found" comes with status 0 and an empty list
            error = f"{data.get('message')}: {result}"
            low = error.lower()
            if "daily" in low:
                keys.rest(name, KEY_REST)
                continue
            if "invalid api key" in low:
                keys.drop(name)
                continue
            if "rate limit" in low:  # calls per second: the next key takes over
                keys.switch(name)
        attempt += 1
        if attempt == max_retries:
            raise RuntimeError(f"Etherscan call failed for {params.get('address')}: {error}")
        time.sleep(min(2 ** attempt, 60) * random.uniform(0.5, 1.5))


def load_keys():
    """name -> key of every Etherscan key of .env and the environment, duplicates dropped."""
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    names = sorted((n for n in os.environ if KEY_RE.match(n)), key=lambda n: (len(n), n))
    keys = {}
    for name in names + ["ETHERSCAN_KEY"]:
        key = os.environ.get(name, "").strip()
        if key and key not in keys.values():
            keys[name] = key
    if not keys:
        raise SystemExit("No Etherscan key: set ETHERSCAN_API_KEY in .env")
    return keys


def fetch_block(session, limiter, keys, action, user, block):
    """All rows of one address in one block, using pagination (capped at MAX_WINDOW rows)."""
    rows = []
    for page in range(1, MAX_WINDOW // MAX_ROWS + 1):
        result = call(session, limiter, keys, {
            "module": "account", "action": action, "address": user,
            "startblock": block, "endblock": block,
            "page": page, "offset": MAX_ROWS, "sort": "asc",
        })
        rows += result
        if len(result) < MAX_ROWS:
            return rows
    print(f"Warning: {user} has >= {MAX_WINDOW} {action} rows in block {block}, some are missing",
          flush=True)
    return rows


def fetch_kind(session, limiter, keys, kind, user, end_block):
    """Rows of one kind for one address between START_BLOCK and end_block, and whether it was cut."""
    action = ACTIONS[kind]
    rows = []
    start = START_BLOCK
    for _ in range(MAX_CALLS_PER_USER):
        result = call(session, limiter, keys, {
            "module": "account", "action": action, "address": user,
            "startblock": start, "endblock": end_block,
            "page": 1, "offset": MAX_ROWS, "sort": "asc",
        })
        if len(result) < MAX_ROWS:
            rows += result
            truncated = False
            break
        # Truncated: keep the complete blocks, start again from the last one
        last = int(result[-1]["blockNumber"])
        complete = [t for t in result if int(t["blockNumber"]) < last]
        if not complete:  # a single block with MAX_ROWS rows: page through that block alone
            rows += fetch_block(session, limiter, keys, action, user, last)
            start = last + 1
        else:
            rows += complete
            start = last
    else:
        truncated = True
        print(f"Warning: {user} {kind} cut at block {start:,} ({len(rows):,} rows kept)", flush=True)
    rows = [{"user": user, "kind": kind, **{col: t.get(field) for field, col in FIELDS.items()}}
            for t in rows]
    return rows, (user, kind, len(rows), truncated)


def fetch_user(session, limiter, keys, user, end_block):
    """Normal then internal rows of one address, and their two status rows."""
    rows, status = [], []
    for kind in ACTIONS:
        r, s = fetch_kind(session, limiter, keys, kind, user, end_block)
        rows += r
        status.append(s)
    return rows, status


def to_table(rows):
    df = pd.DataFrame(rows, columns=SCHEMA.names)
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # "" -> NA
    for col in ["method_id", "gas_price", "trace_id", "call_type", "contract_address"]:
        df[col] = df[col].replace("", None)  # fields that do not apply to the row's kind
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def done_batches(fs):
    """Indexes of the batches already saved in OUT_DIR."""
    fs.invalidate_cache(OUT_DIR)
    return {int(p.rsplit("_", 1)[1].split(".")[0]) for p in fs.glob(f"{OUT_DIR}/batch_*.parquet")}


def merge_batches(fs, n_batches):
    """Concatenate the batches into OUT_FILE one at a time, to keep memory low."""
    n_rows = 0
    tmp_file = f"{OUT_FILE}.tmp"
    with fs.open(tmp_file, "wb") as f, pq.ParquetWriter(f, SCHEMA) as writer:
        for i in range(n_batches):
            with fs.open(f"{OUT_DIR}/batch_{i:05d}.parquet", "rb") as part:
                table = pq.read_table(part, schema=SCHEMA)
            writer.write_table(table)
            n_rows += table.num_rows
    fs.mv(tmp_file, OUT_FILE)
    return n_rows


def last_block(fs, path):
    """Largest block_number of a Parquet file, from its row group statistics."""
    with fs.open(path, "rb") as f:
        meta = pq.ParquetFile(f).metadata
    col = meta.schema.names.index("block_number")
    return max(meta.row_group(i).column(col).statistics.max for i in range(meta.num_row_groups))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--test", type=int, metavar="N", help="fetch only N users and save nothing")
    args = parser.parse_args()

    keys = KeyPool(load_keys())
    print(f"Etherscan keys: {', '.join(keys.names)}")

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    end_block = last_block(fs, TRANSFERS_FILE)
    with fs.open(WALLETS_EVENTS_FILE, "rb") as f:
        users = sorted(pd.read_parquet(f, columns=["user"])["user"].dropna().unique())
    print(f"{len(users):,} users, blocks {START_BLOCK:,}-{end_block:,}")

    session = requests.Session()
    limiter = RateLimiter(CALLS_PER_SEC)

    def fetch(user):
        return fetch_user(session, limiter, keys, user, end_block)

    if args.test:
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            results = list(executor.map(fetch, users[:args.test]))
        elapsed = time.time() - t0
        df = to_table([r for rows, _ in results for r in rows]).to_pandas()
        status = pd.DataFrame([s for _, st in results for s in st], columns=STATUS_SCHEMA.names)
        print(status.groupby("kind").n_rows.describe().to_string())
        print(f"cut: {status.truncated.sum()} (user, kind)")
        moved = df[(df.value != "0") & (df.is_error == 0)]
        print(f"rows moving ETH: {len(moved):,} of {len(df):,} "
              f"({moved.kind.value_counts().to_dict()})")
        print(df.head().to_string())
        print(f"{len(df):,} rows for {args.test} users in {elapsed:.1f} s -> full run "
              f"{elapsed / args.test * len(users) / 3600:.1f} h without the daily quota")
        return

    batches = [users[i:i + BATCH_SIZE] for i in range(0, len(users), BATCH_SIZE)]
    done = done_batches(fs)
    todo = [i for i in range(len(batches)) if i not in done]
    if done:
        print(f"Resuming: {len(done)} of {len(batches)} batches already saved")

    n_total = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for k, i in enumerate(todo, 1):
            results = list(executor.map(fetch, batches[i]))
            table = to_table([r for rows, _ in results for r in rows])
            status = pa.Table.from_pylist(
                [dict(zip(STATUS_SCHEMA.names, s)) for _, st in results for s in st],
                schema=STATUS_SCHEMA)
            # status first: a saved batch always has its status
            with fs.open(f"{STATUS_DIR}/batch_{i:05d}.parquet", "wb") as f:
                pq.write_table(status, f)
            with fs.open(f"{OUT_DIR}/batch_{i:05d}.parquet", "wb") as f:
                pq.write_table(table, f)

            n_total += table.num_rows
            elapsed = time.time() - t0
            eta_h = elapsed / k * (len(todo) - k) / 3600
            print(f"[{(len(done) + k) / len(batches):6.1%}] batch {i:05d} | "
                  f"{table.num_rows:8,d} rows (total {n_total:,}) | "
                  f"{sum(status.column('truncated').to_pylist())} cut | ETA {eta_h:.1f} h", flush=True)

    n_rows = merge_batches(fs, len(batches))
    print(f"Saved {n_rows:,} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
