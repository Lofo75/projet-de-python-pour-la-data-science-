"""
Keep the Aave V3 events and the ERC-20 transfers of wallets only.

Run after fetch_transfers_etherscan.py and before match_events_transfers.py.
Replaces drop_non_wallets.py, transfers_drop_contracts.py, preprocess_events.py,
drop_truncated.py and transfers_drop_incomplete.py.

Every user of EVENTS_FILE is checked (addresses are compared in lowercase).
A user is dropped, with all its events and transfers, if it is:
  1. Identity (free, on every user):
     - skipped_fetch: skipped by fetch_transfers_etherscan.py (too many
                      transfers, or 1,000+ transfers in a single block)
     - burn:          in BURN_ADDRESSES (null address, dead addresses, precompiles)
     - reserve:       an Aave V3 reserve (reserve or collateral_asset of an event)
     - token:         an ERC-20 contract (token_address of any transfer: aTokens,
                      debt tokens and every other token that moved)
  2. Consistency (free, on the users left after 1):
     - no_transfers:              no transfer fetched at all
     - event_after_last_transfer: an event in a later block than its last transfer
     Every Aave event moves an aToken or a debt token of its user, so these
     histories do not match the events (e.g. events fetched up to a later block
     than the transfers).
  3. Code (eth_getCode at the latest block, on the users left after 2):
     - contract: any code except
         eoa:      no code, a wallet controlled by a private key
         eoa_7702: 0xef0100 + address, an EOA delegating to code (EIP-7702)
         safe:     a Safe (Gnosis Safe) proxy, a multisig wallet: small code
                   containing the masterCopy() selector a619486e
       Other smart-contract wallets (Argent, ERC-4337 accounts...) are not told
       apart and are dropped as contracts.
Several reasons of group 1 are joined with "+" (e.g. "reserve+token").

Outputs, rebuilt from the raw inputs at every run (a second run gives the same
result; only the eth_getCode cache in CODE_DIR is reused):
  - OUT_DIR/batch_*.parquet: the batches of IN_DIR without the dropped users
  - TRANSFERS_OUT:           those batches merged
  - EVENTS_OUT:              the events of the users kept
  - SKIPPED_OUT:             one row per dropped user: address, reason,
                             n_transfers (in IN_DIR, 0 for skipped_fetch), n_events
Address columns are lowercased in every output.

eth_getCode calls go to RPC_URL, a free public node that needs no key, as
JSON-RPC batches of RPC_BATCH addresses. The code types are cached in CODE_DIR
as Parquet parts of up to PART_SIZE addresses, so an interrupted run resumes
without calling again for the addresses already read. S3 credentials and
endpoint are read from the AWS_* environment variables that Onyxia injects into
the service. Usage:
    python3 preprocess_events_and_transfers.py
"""
import re
import time

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import s3fs

EVENTS_FILE        = "s3://bastienlevyguinot/diffusion/aave_v3_events.parquet"
FETCH_SKIPPED_FILE = "s3://bastienlevyguinot/diffusion/skipped_users_during_transfers_fetching.parquet"
IN_DIR             = "s3://bastienlevyguinot/aave_v3/transfers_parts"
OUT_DIR            = "s3://bastienlevyguinot/aave_v3/transfers_parts_preprocessed"
CODE_DIR           = "s3://bastienlevyguinot/aave_v3/code_types_parts"
TRANSFERS_OUT      = "s3://bastienlevyguinot/diffusion/transfers_preprocessed.parquet"
EVENTS_OUT         = "s3://bastienlevyguinot/diffusion/aave_v3_events_preprocessed.parquet"
SKIPPED_OUT        = "s3://bastienlevyguinot/diffusion/skipped_addresses.parquet"

EVENT_ADDRESS_COLUMNS    = ["reserve", "user", "caller", "recipient", "repayer", "collateral_asset", "liquidator"]
TRANSFER_ADDRESS_COLUMNS = ["user", "from", "to", "token_address"]

BURN_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",  # null address (mint / burn)
    "0x000000000000000000000000000000000000dead",
    "0xdead000000000000000042069420694206942069",
    *(f"0x{i:040x}" for i in range(1, 11)),        # precompiles 0x01-0x0a
}

RPC_URL   = "https://ethereum.publicnode.com"
RPC_BATCH = 50      # eth_getCode calls per HTTP request
PART_SIZE = 5_000   # addresses per cached Parquet part
KEPT_CODE_TYPES = {"eoa", "eoa_7702", "safe"}
SAFE_SELECTOR = "a619486e"  # keccak256("masterCopy()")

SKIPPED_SCHEMA = pa.schema([
    ("address",     pa.string()),
    ("reason",      pa.string()),
    ("n_transfers", pa.int64()),
    ("n_events",    pa.int64()),
])

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")
PART_RE  = re.compile(r"part_(\d+)\.parquet$")


# ---------------------------------------------------------------- inputs

def load_events(fs):
    """Returns every event, with lowercased address columns."""
    with fs.open(EVENTS_FILE, "rb") as f:
        events = pd.read_parquet(f)
    for col in EVENT_ADDRESS_COLUMNS:
        if col in events:
            events[col] = events[col].str.lower()
    return events


def load_fetch_skipped(fs):
    """Returns a set of lowercased addresses skipped by fetch_transfers_etherscan.py."""
    if not fs.exists(FETCH_SKIPPED_FILE):
        raise SystemExit(f"{FETCH_SKIPPED_FILE} not found: run fetch_transfers_etherscan.py to the end first")
    with fs.open(FETCH_SKIPPED_FILE, "rb") as f:
        return set(pd.read_parquet(f, columns=["user"])["user"].str.lower())


def scan_transfers(fs, paths):
    """Lowercased token addresses, and user -> (n_transfers, last_transfer_block), one batch at a time."""
    tokens, stats = set(), []
    for path in paths:
        with fs.open(path, "rb") as f:
            df = pq.read_table(f, columns=["user", "token_address", "block_number"]).to_pandas()
        tokens.update(df["token_address"].dropna().str.lower())
        df["user"] = df["user"].str.lower()
        stats.append(df.groupby("user")["block_number"].agg(n_transfers="size", last_transfer_block="max"))
    if not stats:
        return tokens, pd.DataFrame(columns=["n_transfers", "last_transfer_block"])
    stats = pd.concat(stats)
    return tokens, stats.groupby(level=0).agg({"n_transfers": "sum", "last_transfer_block": "max"})


# ---------------------------------------------------------------- eth_getCode

def code_type(code):
    code = code.lower()
    if code in ("0x", ""):
        return "eoa"
    if code.startswith("0xef0100") and len(code) == 2 + 2 * 23:
        return "eoa_7702"
    if SAFE_SELECTOR in code and len(code) < 2 + 2 * 1_000:  # proxies are small
        return "safe"
    return "contract"


def get_codes(session, addresses, max_retries=10):
    """address -> code, retrying the calls of the batch that failed"""
    codes, todo, error = {}, list(addresses), None
    for attempt in range(max_retries):
        payload = [{"jsonrpc": "2.0", "id": i, "method": "eth_getCode", "params": [a, "latest"]}
                   for i, a in enumerate(todo)]
        try:
            answers = session.post(RPC_URL, json=payload, timeout=60).json()
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
            parts.append(pd.read_parquet(f, columns=["address", "code_type"]))
    cached = pd.concat(parts) if parts else pd.DataFrame(columns=["address", "code_type"])
    cached["address"] = cached["address"].str.lower()
    next_part = max((int(PART_RE.search(p).group(1)) for p in paths), default=-1) + 1
    return cached.drop_duplicates("address"), next_part


def fetch_code_types(fs, users):
    """address -> code_type for every user, using the cache in CODE_DIR."""
    cached, next_part = load_code_types(fs)
    todo = sorted(set(users) - set(cached["address"]))
    print(f"eth_getCode: {len(users):,} users, {len(users) - len(todo):,} cached, {len(todo):,} to fetch")

    session = requests.Session()
    parts = [cached]
    t0 = time.time()
    for k, start in enumerate(range(0, len(todo), PART_SIZE)):
        chunk = todo[start:start + PART_SIZE]
        codes = {}
        for i in range(0, len(chunk), RPC_BATCH):
            codes.update(get_codes(session, chunk[i:i + RPC_BATCH]))
        part = pd.DataFrame({
            "address":   chunk,
            "code_type": [code_type(codes[a]) for a in chunk],
            "code_size": [(len(codes[a]) - 2) // 2 for a in chunk],
        })
        with fs.open(f"{CODE_DIR}/part_{next_part + k:05d}.parquet", "wb") as f:
            part.to_parquet(f, index=False)
        parts.append(part[["address", "code_type"]])
        done = start + len(chunk)
        eta_min = (time.time() - t0) / done * (len(todo) - done) / 60
        print(f"[{done / len(todo):6.1%}] {done:,} code types fetched | ETA {eta_min:.0f} min", flush=True)
    types = pd.concat(parts, ignore_index=True).set_index("address")["code_type"]
    return types.reindex(sorted(users))


# ---------------------------------------------------------------- rules

def add_reasons(reason, rules):
    """Append each rule's name to reason where its mask is True, joined with "+" """
    for name, mask in rules:
        reason = reason.mask(mask, (reason + "+" + name).str.lstrip("+"))
    return reason


def classify_users(fs, events, fetch_skipped, tokens, stats):
    """One row per user of the events: n_events, n_transfers and reason ("" if kept)"""
    users = (events.dropna(subset=["user"]).groupby("user")["block_number"].agg(n_events="size", last_event_block="max"))
    users = users.join(stats, how="left")
    users["n_transfers"] = users["n_transfers"].fillna(0).astype("int64")
    address = users.index.to_series()

    # 1. Identity
    reserves = set(pd.concat([events["reserve"], events["collateral_asset"]]).dropna())
    reason = add_reasons(pd.Series("", index=users.index), [
        ("skipped_fetch", address.isin(fetch_skipped)),
        ("burn",          address.isin(BURN_ADDRESSES)),
        ("reserve",       address.isin(reserves)),
        ("token",         address.isin(tokens)),
    ])

    # 2. Consistency
    left = reason == ""
    no_transfers = left & (users["n_transfers"] == 0)
    after_last = (users["last_event_block"] > users["last_transfer_block"]).fillna(False).astype(bool)
    reason[no_transfers] = "no_transfers"
    reason[left & ~no_transfers & after_last] = "event_after_last_transfer"

    # 3. Code
    left = reason == ""
    types = fetch_code_types(fs, list(users.index[left]))
    print(types.value_counts().to_string())
    contracts = types.index[~types.isin(KEPT_CODE_TYPES)]
    reason.loc[contracts] = "contract"

    users["reason"] = reason
    return users


# ---------------------------------------------------------------- outputs

def lower_columns(table, cols):
    for col in cols:
        i = table.schema.get_field_index(col)
        if i >= 0:
            table = table.set_column(i, col, pc.utf8_lower(table.column(col)))
    return table


def write_transfers(fs, paths, kept):
    """Batches without the dropped users to OUT_DIR, merged into TRANSFERS_OUT; returns the rows written."""
    kept = pa.array(sorted(kept), pa.string())
    names, n_rows, writer = set(), 0, None
    with fs.open(TRANSFERS_OUT, "wb") as out:
        for path in paths:
            with fs.open(path, "rb") as f:
                table = lower_columns(pq.read_table(f), TRANSFER_ADDRESS_COLUMNS)
            table = table.filter(pc.is_in(table.column("user"), value_set=kept))
            name = path.rsplit("/", 1)[-1]
            with fs.open(f"{OUT_DIR}/{name}", "wb") as f:
                pq.write_table(table, f)
            if writer is None:
                writer = pq.ParquetWriter(out, table.schema)
            writer.write_table(table)
            names.add(name)
            n_rows += table.num_rows
        writer.close()

    # Batches left by an earlier run on more raw batches
    fs.invalidate_cache(OUT_DIR)
    for p in fs.glob(f"{OUT_DIR}/batch_*.parquet"):
        if p.rsplit("/", 1)[-1] not in names:
            fs.rm(p)
    return n_rows


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    fs.invalidate_cache(IN_DIR)
    paths = sorted(p for p in fs.glob(f"{IN_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    if not paths:
        raise SystemExit(f"No batch in {IN_DIR}: run fetch_transfers_etherscan.py first")

    events = load_events(fs)
    fetch_skipped = load_fetch_skipped(fs)
    tokens, stats = scan_transfers(fs, paths)
    print(f"{len(events):,} events, {len(paths)} transfer batches ({stats['n_transfers'].sum():,} transfers), "
          f"{len(tokens):,} tokens, {len(fetch_skipped):,} users skipped by the fetch")

    users = classify_users(fs, events, fetch_skipped, tokens, stats)
    kept = set(users.index[users["reason"] == ""])
    dropped = users[users["reason"] != ""]

    n_rows = write_transfers(fs, paths, kept)
    print(f"{len(paths)} batches written to {OUT_DIR}, {n_rows:,} transfers saved to {TRANSFERS_OUT}")

    is_kept = events["user"].isin(kept)
    with fs.open(EVENTS_OUT, "wb") as f:
        events[is_kept].to_parquet(f, index=False)
    print(f"{is_kept.sum():,} of {len(events):,} events saved to {EVENTS_OUT} "
          f"({events['user'].isna().sum():,} without user dropped)")
    print(pd.DataFrame({"kept": events.loc[is_kept, "event_name"].value_counts(),
                        "dropped": events.loc[~is_kept, "event_name"].value_counts()}).fillna(0).astype(int).to_string())

    skipped = (dropped.rename_axis("address").reset_index()[SKIPPED_SCHEMA.names]
               .sort_values(["n_transfers", "n_events"], ascending=False))
    with fs.open(SKIPPED_OUT, "wb") as f:
        pq.write_table(pa.Table.from_pandas(skipped, schema=SKIPPED_SCHEMA, preserve_index=False), f)
    print(f"{len(kept):,} users kept, {len(skipped):,} dropped, saved to {SKIPPED_OUT}")
    print(skipped.groupby("reason").agg(users=("address", "size"), n_transfers=("n_transfers", "sum"),
                                        n_events=("n_events", "sum")).to_string())


if __name__ == "__main__":
    main()
