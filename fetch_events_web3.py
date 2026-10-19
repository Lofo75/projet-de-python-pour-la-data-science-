"""
Fetch Aave V3 (Ethereum main market) events between two blocks with web3.py and
save them to Parquet on the Onyxia S3 (MinIO) storage:
  - user actions (Supply, Withdraw, Borrow, Repay, LiquidationCall) -> OUT_FILE
  - reserve state updates (ReserveDataUpdated: rates and indexes)   -> RESERVE_FILE

The Alchemy free plan caps eth_getLogs at 10 blocks per request, so the range
is split into small chunks fetched in parallel. Results are written in segments
under OUT_DIR and RESERVE_DIR, so an interrupted run resumes where it stopped.
Once every segment is fetched they are merged into OUT_FILE and RESERVE_FILE.

S3 credentials and endpoint are read from the AWS_* environment variables that
Onyxia injects into the service, so no extra configuration is needed.

All settings are the constants below. Usage:
    python3 fetch_borrow_repay_w3.py
"""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import s3fs
from dotenv import load_dotenv
from web3 import Web3

POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"  # Aave V3 Pool (proxy)
START_BLOCK  = 0xF89537   # 16,291,127: Pool proxy deployment
END_BLOCK    = 0x180CEED  # 25,218,797

S3_ROOT      = "s3://bastienlevyguinot/aave_v3"
OUT_DIR      = f"{S3_ROOT}/aave_v3_events_parts"
OUT_FILE     = f"{S3_ROOT}/aave_v3_events.parquet"
RESERVE_DIR  = f"{S3_ROOT}/aave_v3_reserve_parts"
RESERVE_FILE = f"{S3_ROOT}/aave_v3_reserve_data.parquet"

CHUNK_SIZE   = 10      # blocks per eth_getLogs call (Alchemy free plan max: 10)
SEGMENT_SIZE = 20_000  # blocks per saved Parquet part
WORKERS      = 4       # parallel eth_getLogs calls

POOL_EVENTS_ABI = [
    {
        "anonymous": False, "type": "event", "name": "Supply",
        "inputs": [
            {"indexed": True,  "name": "reserve",      "type": "address"},
            {"indexed": False, "name": "user",         "type": "address"},
            {"indexed": True,  "name": "onBehalfOf",   "type": "address"},
            {"indexed": False, "name": "amount",       "type": "uint256"},
            {"indexed": True,  "name": "referralCode", "type": "uint16"},
        ],
    },
    {
        "anonymous": False, "type": "event", "name": "Withdraw",
        "inputs": [
            {"indexed": True,  "name": "reserve", "type": "address"},
            {"indexed": True,  "name": "user",    "type": "address"},
            {"indexed": True,  "name": "to",      "type": "address"},
            {"indexed": False, "name": "amount",  "type": "uint256"},
        ],
    },
    {
        "anonymous": False, "type": "event", "name": "Borrow",
        "inputs": [
            {"indexed": True,  "name": "reserve",          "type": "address"},
            {"indexed": False, "name": "user",             "type": "address"},
            {"indexed": True,  "name": "onBehalfOf",       "type": "address"},
            {"indexed": False, "name": "amount",           "type": "uint256"},
            {"indexed": False, "name": "interestRateMode", "type": "uint8"},
            {"indexed": False, "name": "borrowRate",       "type": "uint256"},
            {"indexed": True,  "name": "referralCode",     "type": "uint16"},
        ],
    },
    {
        "anonymous": False, "type": "event", "name": "Repay",
        "inputs": [
            {"indexed": True,  "name": "reserve",    "type": "address"},
            {"indexed": True,  "name": "user",       "type": "address"},
            {"indexed": True,  "name": "repayer",    "type": "address"},
            {"indexed": False, "name": "amount",     "type": "uint256"},
            {"indexed": False, "name": "useATokens", "type": "bool"},
        ],
    },
    {
        "anonymous": False, "type": "event", "name": "LiquidationCall",
        "inputs": [
            {"indexed": True,  "name": "collateralAsset",            "type": "address"},
            {"indexed": True,  "name": "debtAsset",                  "type": "address"},
            {"indexed": True,  "name": "user",                       "type": "address"},
            {"indexed": False, "name": "debtToCover",                "type": "uint256"},
            {"indexed": False, "name": "liquidatedCollateralAmount", "type": "uint256"},
            {"indexed": False, "name": "liquidator",                 "type": "address"},
            {"indexed": False, "name": "receiveAToken",              "type": "bool"},
        ],
    },
    {
        "anonymous": False, "type": "event", "name": "ReserveDataUpdated",
        "inputs": [
            {"indexed": True,  "name": "reserve",             "type": "address"},
            {"indexed": False, "name": "liquidityRate",       "type": "uint256"},
            {"indexed": False, "name": "stableBorrowRate",    "type": "uint256"},
            {"indexed": False, "name": "variableBorrowRate",  "type": "uint256"},
            {"indexed": False, "name": "liquidityIndex",      "type": "uint256"},
            {"indexed": False, "name": "variableBorrowIndex", "type": "uint256"},
        ],
    },
]

# Common columns are filled for every event; the others only for their event type.
# "user" is the account whose position changes (onBehalfOf for Supply and Borrow).
# "reserve"/"amount_raw" are the debt asset/amount for liquidations (debtAsset/debtToCover).
# uint256 values are kept exact as strings in *_raw columns (convert with int()).
COLUMN_DTYPES = {
    "event_name":                   "string",
    "block_number":                 "Int64",
    "tx_hash":                      "string",
    "log_index":                    "Int64",
    "reserve":                      "string",
    "user":                         "string",
    "amount_raw":                   "string",
    # Supply / Borrow
    "caller":                       "string",
    "referral_code":                "Int64",
    # Withdraw
    "recipient":                    "string",
    # Borrow
    "interest_rate_mode":           "Int64",
    "borrow_rate_raw":              "string",
    # Repay
    "repayer":                      "string",
    "use_atokens":                  "boolean",
    # LiquidationCall
    "collateral_asset":             "string",
    "liquidated_collateral_raw":    "string",
    "liquidator":                   "string",
    "receive_atoken":               "boolean",
}

# ReserveDataUpdated: the reserve's state right after an action. Rates are yearly,
# indexes are cumulative interest factors; all are in ray units (1e27 = 1.0).
RESERVE_COLUMN_DTYPES = {
    "block_number":                 "Int64",
    "tx_hash":                      "string",
    "log_index":                    "Int64",
    "reserve":                      "string",
    "liquidity_rate_raw":           "string",
    "stable_borrow_rate_raw":       "string",
    "variable_borrow_rate_raw":     "string",
    "liquidity_index_raw":          "string",
    "variable_borrow_index_raw":    "string",
}

PART_RE = re.compile(r"part_(\d+)_(\d+)\.parquet$")


def get_logs(w3, topics, from_block, to_block, max_retries=8):
    """eth_getLogs with backoff on rate limits, splitting the range if the provider refuses it."""
    for attempt in range(max_retries):
        try:
            return w3.eth.get_logs({
                "address":   POOL_ADDRESS,
                "fromBlock": from_block,
                "toBlock":   to_block,
                "topics":    [topics],
            })
        except Exception as e:
            msg = str(e).lower()
            if ("block range" in msg or "response size" in msg) and to_block > from_block:
                mid = (from_block + to_block) // 2
                return (get_logs(w3, topics, from_block, mid)
                        + get_logs(w3, topics, mid + 1, to_block))
            if attempt == max_retries - 1:
                raise RuntimeError(f"eth_getLogs failed for blocks {from_block}-{to_block}: {e}") from e
            time.sleep(min(2 ** attempt, 60))


def decode(log, events_by_topic):
    event = events_by_topic[Web3.to_hex(log["topics"][0])]
    ev = event.process_log(log)
    a = ev["args"]
    row = {
        "event_name":   ev["event"],
        "block_number": ev["blockNumber"],
        "tx_hash":      Web3.to_hex(ev["transactionHash"]),
        "log_index":    ev["logIndex"],
    }
    if ev["event"] == "Supply":
        row.update(
            reserve=a["reserve"], user=a["onBehalfOf"], amount_raw=str(a["amount"]),
            caller=a["user"], referral_code=a["referralCode"],
        )
    elif ev["event"] == "Withdraw":
        row.update(
            reserve=a["reserve"], user=a["user"], amount_raw=str(a["amount"]),
            recipient=a["to"],
        )
    elif ev["event"] == "Borrow":
        row.update(
            reserve=a["reserve"], user=a["onBehalfOf"], amount_raw=str(a["amount"]),
            caller=a["user"], interest_rate_mode=a["interestRateMode"],
            borrow_rate_raw=str(a["borrowRate"]), referral_code=a["referralCode"],
        )
    elif ev["event"] == "Repay":
        row.update(
            reserve=a["reserve"], user=a["user"], amount_raw=str(a["amount"]),
            repayer=a["repayer"], use_atokens=a["useATokens"],
        )
    elif ev["event"] == "LiquidationCall":
        row.update(
            reserve=a["debtAsset"], user=a["user"], amount_raw=str(a["debtToCover"]),
            collateral_asset=a["collateralAsset"],
            liquidated_collateral_raw=str(a["liquidatedCollateralAmount"]),
            liquidator=a["liquidator"], receive_atoken=a["receiveAToken"],
        )
    else:  # ReserveDataUpdated
        row.update(
            reserve=a["reserve"],
            liquidity_rate_raw=str(a["liquidityRate"]),
            stable_borrow_rate_raw=str(a["stableBorrowRate"]),
            variable_borrow_rate_raw=str(a["variableBorrowRate"]),
            liquidity_index_raw=str(a["liquidityIndex"]),
            variable_borrow_index_raw=str(a["variableBorrowIndex"]),
        )
    return row


def to_frame(rows, dtypes):
    return pd.DataFrame(rows, columns=list(dtypes)).astype(dtypes)


def list_parts(fs, out_dir):
    """s3:// paths of the saved segments in out_dir."""
    fs.invalidate_cache(out_dir)
    return sorted(f"s3://{p}" for p in fs.glob(f"{out_dir}/part_*.parquet"))


def resume_block(fs, out_dir, start):
    """First block not yet covered by a saved segment."""
    ends = [int(m.group(2)) for p in list_parts(fs, out_dir) if (m := PART_RE.search(p))]
    return max([start] + [e + 1 for e in ends])


def merge_parts(fs, out_dir, start, end, out_file):
    parts = [
        p for p in list_parts(fs, out_dir)
        if (m := PART_RE.search(p)) and int(m.group(1)) >= start and int(m.group(2)) <= end
    ]
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    df = df.sort_values(["block_number", "log_index"], ignore_index=True)
    df.to_parquet(out_file, index=False)
    return df


def main():
    load_dotenv()
    api_key = os.environ["ALCHEMY_API_KEY"].strip()
    w3 = Web3(Web3.HTTPProvider(f"https://eth-mainnet.g.alchemy.com/v2/{api_key}",
                                request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise SystemExit("Could not connect to Alchemy")

    end = min(END_BLOCK, w3.eth.block_number)
    pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_EVENTS_ABI)
    events = [getattr(pool.events, abi["name"])() for abi in POOL_EVENTS_ABI]
    events_by_topic = {e.topic: e for e in events}
    topics = list(events_by_topic)

    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    # Resume from the folder that is furthest behind, so both tables cover the same blocks
    first = min(resume_block(fs, OUT_DIR, START_BLOCK), resume_block(fs, RESERVE_DIR, START_BLOCK))
    if first > START_BLOCK:
        print(f"Resuming from block {first:,}")

    total_blocks = end - first + 1
    n_events = n_updates = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for seg_start in range(first, end + 1, SEGMENT_SIZE):
            seg_end = min(seg_start + SEGMENT_SIZE - 1, end)
            chunks = [(a, min(a + CHUNK_SIZE - 1, seg_end))
                      for a in range(seg_start, seg_end + 1, CHUNK_SIZE)]
            logs = [log for batch in executor.map(lambda c: get_logs(w3, topics, *c), chunks)
                    for log in batch]

            rows = [decode(log, events_by_topic) for log in logs]
            events_df  = to_frame([r for r in rows if r["event_name"] != "ReserveDataUpdated"], COLUMN_DTYPES)
            reserve_df = to_frame([r for r in rows if r["event_name"] == "ReserveDataUpdated"], RESERVE_COLUMN_DTYPES)
            part_name = f"part_{seg_start:09d}_{seg_end:09d}.parquet"
            events_df.to_parquet(f"{OUT_DIR}/{part_name}", index=False)
            reserve_df.to_parquet(f"{RESERVE_DIR}/{part_name}", index=False)

            n_events  += len(events_df)
            n_updates += len(reserve_df)
            done = seg_end - first + 1
            elapsed = time.time() - t0
            eta_h = elapsed / done * (total_blocks - done) / 3600
            print(f"[{done / total_blocks:6.1%}] blocks {seg_start:,}-{seg_end:,} | "
                  f"{len(events_df):5d} events (total {n_events:,}) | "
                  f"{len(reserve_df):5d} reserve updates (total {n_updates:,}) | ETA {eta_h:.1f} h", flush=True)

    events_df = merge_parts(fs, OUT_DIR, START_BLOCK, end, OUT_FILE)
    print(f"Saved {len(events_df):,} events to {OUT_FILE}")
    print(events_df["event_name"].value_counts().to_string())

    reserve_df = merge_parts(fs, RESERVE_DIR, START_BLOCK, end, RESERVE_FILE)
    print(f"Saved {len(reserve_df):,} reserve updates to {RESERVE_FILE}")


if __name__ == "__main__":
    main()