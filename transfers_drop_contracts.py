"""
Drop the transfers of the users that are contracts, keeping wallets.

Run after drop_non_wallets.py, which removed tokens and burn addresses. The
code of every remaining user is read with eth_getCode (latest block):
  - eoa:      no code, a wallet controlled by a private key           -> kept
  - eoa_7702: 0xef0100 + address, an EOA that delegates to code since
              the Pectra upgrade (EIP-7702), still a wallet            -> kept
  - safe:     a Safe (Gnosis Safe) proxy, a multisig wallet; its code
              contains the masterCopy() selector a619486e              -> kept
  - contract: any other code (routers, bots, vaults, pools...)        -> dropped
Other smart-contract wallets (Argent, ERC-4337 accounts...) are not told apart
and are dropped as contracts.

Calls are sent as JSON-RPC batches of RPC_BATCH addresses. The code types are
cached in CODE_DIR as Parquet parts of up to PART_SIZE addresses, so an
interrupted run resumes without calling again for the addresses already read.

The batches of WALLETS_DIR are rewritten in place without the contracts, the
contracts are appended to DROPPED_FILE (reason "contract") and every batch is
then merged into OUT_FILE. Since the contracts are gone after one run, a
second run would find none: run drop_non_wallets.py again first.

Calls go to RPC_URL, a free public node that needs no key and accepts
batches (the Alchemy free plan ran out of monthly capacity). Usage:
    python3 transfers_drop_contracts.py
"""
import re
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs

WALLETS_DIR  = "s3://bastienlevyguinot/aave_v3/transfers_parts_wallets"
CODE_DIR     = "s3://bastienlevyguinot/aave_v3/code_types_parts"
DROPPED_FILE = "s3://bastienlevyguinot/aave_v3/dropped_addresses.csv"
OUT_FILE     = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"

RPC_URL   = "https://ethereum.publicnode.com"

RPC_BATCH = 50      # eth_getCode calls per HTTP request
PART_SIZE = 5_000   # addresses per cached Parquet part
KEPT      = {"eoa", "eoa_7702", "safe"}

SAFE_SELECTOR = "a619486e"  # masterCopy(), answered by every Safe proxy

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")
PART_RE  = re.compile(r"part_(\d+)\.parquet$")


def code_type(code):
    code = code.lower()
    if code in ("0x", ""):
        return "eoa"
    if code.startswith("0xef0100") and len(code) == 2 + 2 * 23:
        return "eoa_7702"
    if SAFE_SELECTOR in code and len(code) < 2 + 2 * 1_000:  # proxies are small
        return "safe"
    return "contract"


def get_codes(session, url, addresses, max_retries=10):
    """address -> code, retrying the calls of the batch that failed (rate limits)."""
    codes = {}
    todo = list(addresses)
    for attempt in range(max_retries):
        payload = [{"jsonrpc": "2.0", "id": i, "method": "eth_getCode", "params": [a, "latest"]}
                   for i, a in enumerate(todo)]
        try:
            r = session.post(url, json=payload, timeout=60)
            answers = r.json()
        except (requests.RequestException, ValueError) as e:
            answers, error = [], str(e)
        if isinstance(answers, dict):  # the whole batch was refused
            answers, error = [], str(answers.get("error"))
        for answer in answers:
            if "result" in answer:
                codes[todo[answer["id"]]] = answer["result"]
            else:
                error = str(answer.get("error"))
        todo = [a for a in todo if a not in codes]
        if not todo:
            return codes
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"eth_getCode failed for {len(todo)} addresses: {error}")


def load_code_types(fs):
    """Cached code types and the index of the next part to write."""
    fs.invalidate_cache(CODE_DIR)
    paths = [p for p in fs.glob(f"{CODE_DIR}/part_*.parquet") if PART_RE.search(p)]
    parts = []
    for path in paths:
        with fs.open(path, "rb") as f:
            parts.append(pd.read_parquet(f))
    cached = pd.concat(parts) if parts else pd.DataFrame(columns=["address", "code_type", "code_size"])
    next_part = max((int(PART_RE.search(p).group(1)) for p in paths), default=-1) + 1
    return cached, next_part


def fetch_code_types(fs, url, users):
    """address, code_type, code_size (bytes) of every user, using the cache in CODE_DIR."""
    cached, next_part = load_code_types(fs)
    todo = sorted(set(users) - set(cached["address"]))
    print(f"{len(users):,} users, {len(users) - len(todo):,} code types cached, {len(todo):,} to fetch")

    session = requests.Session()
    parts = [cached]
    t0 = time.time()
    for k, start in enumerate(range(0, len(todo), PART_SIZE)):
        chunk = todo[start:start + PART_SIZE]
        codes = {}
        for i in range(0, len(chunk), RPC_BATCH):
            codes.update(get_codes(session, url, chunk[i:i + RPC_BATCH]))
        part = pd.DataFrame({
            "address":   chunk,
            "code_type": [code_type(codes[a]) for a in chunk],
            "code_size": [(len(codes[a]) - 2) // 2 for a in chunk],
        })
        with fs.open(f"{CODE_DIR}/part_{next_part + k:05d}.parquet", "wb") as f:
            part.to_parquet(f, index=False)
        parts.append(part)
        done = start + len(chunk)
        eta_min = (time.time() - t0) / done * (len(todo) - done) / 60
        print(f"[{done / len(todo):6.1%}] {done:,} code types fetched | ETA {eta_min:.0f} min", flush=True)
    return pd.concat(parts, ignore_index=True)


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    with fs.open(DROPPED_FILE, "r") as f:
        dropped = pd.read_csv(f)
    if (dropped["reason"] == "contract").any():
        raise SystemExit(f"{DROPPED_FILE} already lists contracts: run drop_non_wallets.py first")

    fs.invalidate_cache(WALLETS_DIR)
    paths = sorted(p for p in fs.glob(f"{WALLETS_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    users = set()
    for path in paths:
        with fs.open(path, "rb") as f:
            users.update(pq.read_table(f, columns=["user"]).column("user").to_pylist())

    types = fetch_code_types(fs, RPC_URL, users)
    print(types["code_type"].value_counts().to_string())
    contracts = set(types.loc[~types["code_type"].isin(KEPT), "address"])

    # Rewrite the batches without the contracts, then merge them into OUT_FILE
    n_dropped = {}
    n_rows = 0
    for path in paths:
        with fs.open(path, "rb") as f:
            table = pq.read_table(f)
        users = pd.Series(table.column("user").to_pylist())
        is_contract = users.isin(contracts)
        n_dropped.update(users[is_contract].value_counts())
        table = table.filter(pa.array(~is_contract.to_numpy()))
        with fs.open(path, "wb") as f:
            pq.write_table(table, f)
        n_rows += table.num_rows

    with fs.open(OUT_FILE, "wb") as f, pq.ParquetWriter(f, table.schema) as writer:
        for path in paths:
            with fs.open(path, "rb") as part:
                writer.write_table(pq.read_table(part))

    new = types[types["address"].isin(contracts)].assign(
        reason="contract", n_transfers=lambda d: d["address"].map(n_dropped))
    dropped = pd.concat([dropped, new], ignore_index=True)
    dropped = dropped.sort_values("n_transfers", ascending=False)
    with fs.open(DROPPED_FILE, "w") as f:
        dropped.to_csv(f, index=False)

    print(f"{len(contracts):,} contracts dropped ({new['n_transfers'].sum():,} transfers), "
          f"added to {DROPPED_FILE}")
    print(f"{len(paths)} batches rewritten in {WALLETS_DIR}, {n_rows:,} transfers saved to {OUT_FILE}")


if __name__ == "__main__":
    main()
