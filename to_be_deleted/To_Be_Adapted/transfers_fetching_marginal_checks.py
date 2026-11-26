"""
Check the blocks that fetch_transfers_etherscan.py fetched over several pages
against the chain itself.

A (user, block) with MAX_ROWS (1,000) transfers or more was paged through by
Etherscan, which assumes a stable row order across pages: otherwise rows may be
duplicated or skipped. For each such pair, the ERC-20 Transfer logs of the user
in that block are fetched again with eth_getLogs (once with the user as sender,
once as receiver, no row cap) and compared with the rows already saved.

Etherscan does not return the log index, so rows are compared as a multiset of
(tx_hash, token_address, from, to, value): two identical transfers in one
transaction count twice on both sides.

The RPC is Alchemy, with the key read from ALCHEMY_API_KEY (environment or .env
file), as in fetch_events_web3.py. S3 credentials and endpoint are read from
the AWS_* environment variables that Onyxia injects into the service. Usage:
    python3 transfers_fetching_marginal_checks.py
"""
import os
import time
from collections import Counter

import pandas as pd
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv
from web3 import Web3

from transfers_fetching_checks import BATCH_RE, MAX_ROWS, OUT_DIR

# keccak("Transfer(address,address,uint256)"), shared by ERC-20 and ERC-721
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

KEY_COLUMNS = ["tx_hash", "token_address", "from", "to", "value"]


def paged_rows(fs):
    """Saved rows of every (user, block) with MAX_ROWS+ transfers of the user."""
    fs.invalidate_cache(OUT_DIR)
    paths = sorted(p for p in fs.glob(f"{OUT_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    parts = []
    for path in paths:
        with fs.open(path, "rb") as f:
            df = pq.read_table(f, columns=["user", "block_number", *KEY_COLUMNS]).to_pandas()
        n = df.groupby(["user", "block_number"])["tx_hash"].transform("size")
        parts.append(df[n >= MAX_ROWS])
    print(f"{len(paths)} batches read")
    return pd.concat(parts, ignore_index=True)


def get_logs(w3, block, topics, max_retries=8):
    """eth_getLogs for one block with backoff on rate limits and errors."""
    for attempt in range(max_retries):
        try:
            return w3.eth.get_logs({"fromBlock": block, "toBlock": block, "topics": topics})
        except Exception as e:
            if attempt == max_retries - 1:
                raise RuntimeError(f"eth_getLogs failed for block {block}: {e}") from e
            time.sleep(min(2 ** attempt, 60))


def chain_transfers(w3, user, block):
    """ERC-20 transfers of user in block, as (tx_hash, token_address, from, to, value) tuples."""
    user_topic = "0x" + user.lower()[2:].rjust(64, "0")
    logs = (get_logs(w3, block, [TRANSFER_TOPIC, user_topic])
            + get_logs(w3, block, [TRANSFER_TOPIC, None, user_topic]))
    unique = {(Web3.to_hex(log["transactionHash"]), log["logIndex"]): log for log in logs}  # self-transfers match both
    keys = []
    for log in unique.values():
        if len(log["topics"]) != 3:  # ERC-721 transfers index the token id as a 4th topic
            continue
        keys.append((
            Web3.to_hex(log["transactionHash"]).lower(),
            log["address"].lower(),
            "0x" + Web3.to_hex(log["topics"][1])[-40:].lower(),
            "0x" + Web3.to_hex(log["topics"][2])[-40:].lower(),
            str(int.from_bytes(log["data"], "big")),
        ))
    return keys


def main():
    load_dotenv()
    api_key = os.environ["ALCHEMY_API_KEY"].strip()
    w3 = Web3(Web3.HTTPProvider(f"https://eth-mainnet.g.alchemy.com/v2/{api_key}",
                                request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        raise SystemExit("Could not connect to Alchemy")

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    saved = paged_rows(fs)
    for col in KEY_COLUMNS:
        saved[col] = saved[col].str.lower()
    pairs = saved.groupby(["user", "block_number"])
    print(f"{pairs.ngroups} (user, block) pairs to check")

    results = []
    for (user, block), rows in pairs:
        etherscan = Counter(rows[KEY_COLUMNS].itertuples(index=False, name=None))
        chain = Counter(chain_transfers(w3, user, int(block)))
        results.append({
            "user": user, "block_number": block,
            "n_etherscan": sum(etherscan.values()), "n_chain": sum(chain.values()),
            "missing": sum((chain - etherscan).values()),     # on chain, not saved
            "duplicated": sum((etherscan - chain).values()),  # saved, not on chain
        })
    results = pd.DataFrame(results)

    print(results.to_string(index=False))
    bad = results[(results["missing"] > 0) | (results["duplicated"] > 0)]
    print(f"\n{len(bad)} of {len(results)} pairs differ: "
          f"{results['missing'].sum():,} transfers missing, {results['duplicated'].sum():,} duplicated")
    return results


if __name__ == "__main__":
    main()
