import os
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import s3fs
from google.colab import userdata


S3_ENDPOINT = "https://minio.lab.sspcloud.fr"

def load_credentials():
    """Read the Onyxia S3 keys from Colab secrets (decimals() and symbol() use a public RPC: no API key needed)"""
    os.environ["AWS_ACCESS_KEY_ID"]     = userdata.get("AWS_ACCESS_KEY_ID").strip()
    os.environ["AWS_SECRET_ACCESS_KEY"] = userdata.get("AWS_SECRET_ACCESS_KEY").strip()
    os.environ["AWS_SESSION_TOKEN"]     = userdata.get("AWS_SESSION_TOKEN").strip()
    os.environ["AWS_ENDPOINT_URL"]      = S3_ENDPOINT
    os.environ["AWS_DEFAULT_REGION"]    = "us-east-1"

INPUT_FILE    = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
EVENTS_FILE   = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"
ETH_FILE      = "s3://bastienlevyguinot/diffusion/eth_transfers_wallets.parquet"
ETH_STATUS_DIR = "s3://bastienlevyguinot/aave_v3/eth_status_parts"
OUT_FILE      = "s3://bastienlevyguinot/diffusion/transfers_wallets_traced.parquet"
LEDGERS_FILE  = "s3://bastienlevyguinot/aave_v3/filtering_ledgers.parquet"
ORIGINAL_FILE = "s3://bastienlevyguinot/diffusion/original_portfolios.parquet"

RPC_URL = "https://ethereum.publicnode.com"

CHUNK_ROWS   = 1_000_000
MIN_FRACTION = 1e-6  # smaller traced fractions are float residue: treated as 0
DEBT_PREFIXES = ("variableDebt", "stableDebt")  # debt tokens of Aave and its forks: never own funds
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # native ETH rows use this token_address


def trace_user(user, tx_hash, src, dst, token, amount, tracked, supplies=None, debt_tokens=()):
    """traced_fraction of each transfer of one user, the final ledger and the contributions to the original portfolio."""
    supplies = supplies or {}
    fractions = [0.0] * len(tx_hash)
    ledger = {}
    contributions = []

    by_tx = defaultdict(list)  # insertion order = order of first appearance
    for i, h in enumerate(tx_hash):
        by_tx[h].append(i)

    for h, rows in by_tx.items():
        out, inc = defaultdict(float), defaultdict(float)
        for i in rows:
            if src[i] == user:
                out[token[i]] += amount[i]
            if dst[i] == user:
                inc[token[i]] += amount[i]

        if tracked[rows[0]]:  # Aave transaction
            f = 1.0
            for t, a in supplies.get(h, {}).items():
                q = a - min(ledger.get(t, 0.0), out.get(t, 0.0))
                if q > 0:
                    contributions.append((rows[0], t, q, "supply"))
        else:  # weakest link over the traced tokens sent (zero amounts move nothing: ignored)
            ratios = [min(1.0, ledger[t] / a) for t, a in out.items()
                      if a > 0 and ledger.get(t, 0.0) > 0]
            f = min(ratios, default=0.0)
            if f < MIN_FRACTION:  # nothing traced sent (pure receives included), or float dust
                continue
            # untraced tokens sent alongside traced ones: f * amount of own funds consumed
            for t, a in out.items():
                if ledger.get(t, 0.0) <= 0 and a > 0 and t not in debt_tokens:
                    contributions.append((rows[0], t, f * a, "complement"))

        for t, a in out.items():
            ledger[t] = max(0.0, ledger.get(t, 0.0) - a)
        for t, a in inc.items():
            ledger[t] = ledger.get(t, 0.0) + f * a
        for i in rows:
            fractions[i] = f

    return fractions, ledger, contributions


def reserve_info(reserves):
    """Lowercased reserve -> (decimals, symbol)"""
    calls = [(r, sel) for r in reserves for sel in ("0x313ce567", "0x95d89b41")]
    results = {}
    for k in range(0, len(calls), 50):  # the node accepts batches of 50 calls
        payload = [{"jsonrpc": "2.0", "id": i, "method": "eth_call",
                    "params": [{"to": r, "data": sel}, "latest"]}
                   for i, (r, sel) in enumerate(calls[k:k + 50], start=k)]
        for answer in requests.post(RPC_URL, json=payload, timeout=60).json():
            results[calls[answer["id"]]] = bytes.fromhex(answer["result"][2:])
    info = {}
    for r in reserves:
        raw = results[(r, "0x95d89b41")]
        if len(raw) > 32:
            symbol = raw[64:64 + int.from_bytes(raw[32:64], "big")].decode(errors="replace")
        else:  # bytes32 (e.g. MKR)
            symbol = raw.rstrip(b"\0").decode(errors="replace")
        info[r] = (int.from_bytes(results[(r, "0x313ce567")], "big"), symbol)
    return info


def load_events(fs):
    """ user -> tx_hash -> {reserve: amount supplied in token units}, 
        reserve -> symbol, 
        the last block of the events, 
        the (tx_hash, user) of every event"""
    with fs.open(EVENTS_FILE, "rb") as f:
        ev = pd.read_parquet(f, columns=["event_name", "block_number", "user", "tx_hash", "reserve",
                                         "amount_raw"])
    last_block = int(ev["block_number"].max())
    ev = ev.assign(user=ev["user"].str.lower(), tx_hash=ev["tx_hash"].str.lower(),
                   reserve=ev["reserve"].str.lower())
    aave_txs = set(zip(ev["tx_hash"], ev["user"]))
    ev = ev[ev["event_name"] == "Supply"]
    info = reserve_info(sorted(ev["reserve"].unique()))
    supplies = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for u, h, r, a in zip(ev["user"], ev["tx_hash"], ev["reserve"], ev["amount_raw"]):
        supplies[u][h][r] += int(a) / 10 ** info[r][0]  # several Supply events of one tx are summed
    return supplies, {r: sym for r, (_, sym) in info.items()}, last_block, aave_txs


def cut_eth_users(fs):
    """Lowercased users whose ETH history was cut by fetch_eth_etherscan.py."""
    cut = set()
    for path in fs.glob(f"{ETH_STATUS_DIR}/batch_*.parquet"):
        with fs.open(path, "rb") as f:
            status = pd.read_parquet(f)
        cut |= set(status.loc[status["truncated"], "user"].str.lower())
    return cut


def load_eth(fs, schema, last_block, aave_txs, skip):
    with fs.open(ETH_FILE, "rb") as f:
        t = pq.read_table(f, columns=["user", "kind", "block_number", "timestamp", "tx_hash",
                                      "from", "to", "value", "is_error"])
    weth = WETH.lower()
    keep = pc.and_(pc.and_(pc.not_equal(t["value"], "0"), pc.equal(t["is_error"], 0)),
                   pc.less_equal(t["block_number"], last_block))
    wrap = pc.or_(pc.and_(pc.equal(t["kind"], "normal"), pc.equal(pc.utf8_lower(t["to"]), weth)),
                  pc.and_(pc.equal(t["kind"], "internal"), pc.equal(pc.utf8_lower(t["from"]), weth)))
    t = t.filter(pc.and_(keep, pc.invert(wrap)))
    df = t.to_pandas()
    df["u"] = df["user"].str.lower()
    df = df[~df["u"].isin(skip)].sort_values("u", kind="stable")  # each user's order is kept
    df["tracked"] = [(h, u) in aave_txs for h, u in zip(df["tx_hash"], df["u"])]
    df = df.assign(token_address=WETH, token_symbol="ETH", token_name="Ether", token_decimal=18)
    bounds = df.reset_index(drop=True).groupby("u", sort=False).indices
    index = {u: (int(rows[0]), int(rows[-1]) + 1) for u, rows in bounds.items()}
    table = pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
    return table, index


def add_eth(table, eth, eth_index):
    """table with the ETH rows and ERC-20 transfers rows, chronological order."""
    users = pc.unique(pc.utf8_lower(table.column("user"))).to_pylist()  # in order of appearance
    parts = []
    for u in users:
        if u in eth_index:
            start, end = eth_index[u]
            parts.append(eth.slice(start, end - start))
    if not parts:
        return table
    table = pa.concat_tables([table, *parts])
    rank = pc.index_in(pc.utf8_lower(table.column("user")), pa.array(users))
    order = pc.sort_indices(pa.table({"r": rank, "b": table.column("block_number")}),
                            sort_keys=[("r", "ascending"), ("b", "ascending")])  # stable
    return table.take(order)


def trace_chunk(table, seen, supplies, reserve_symbols):
    """traced_fraction of every row of a table holding whole users, their ledgers and their
    original portfolio contributions as rows, and the number of Supply transactions not found."""
    users = [u.lower() for u in table.column("user").to_pylist()]
    tx_hash = table.column("tx_hash").to_pylist()
    block = table.column("block_number").to_pylist()
    timestamp = table.column("timestamp").to_pylist()
    src = [s.lower() for s in table.column("from").to_pylist()]
    dst = [d.lower() for d in table.column("to").to_pylist()]
    token = [t.lower() for t in table.column("token_address").to_pylist()]
    symbol = table.column("token_symbol").to_pylist()
    decimals = table.column("token_decimal").to_pylist()
    amount = [float(v) / 10 ** (d or 0) for v, d in zip(table.column("value").to_pylist(), decimals)]
    tracked = table.column("tracked").to_pylist()

    fractions, ledger_rows, original_rows = [], [], []
    n_missing = 0
    start = 0
    while start < len(users):
        end = start
        while end < len(users) and users[end] == users[start]:
            end += 1
        user = users[start]
        if user in seen:
            raise RuntimeError(f"rows of {user} are not contiguous in {INPUT_FILE}")
        seen.add(user)
        s = slice(start, end)
        user_supplies = supplies.get(user, {})
        symbols = {**reserve_symbols, **dict(zip(token[s], symbol[s]))}
        if WETH.lower() in symbols:
            symbols[WETH.lower()] = "WETH+ETH"  # one ledger entry for both
        debt = {t for t, sym in symbols.items() if (sym or "").startswith(DEBT_PREFIXES)}
        f, ledger, contributions = trace_user(user, tx_hash[s], src[s], dst[s], token[s], amount[s],
                                              tracked[s], user_supplies, debt)
        n_missing += len(set(user_supplies) - set(tx_hash[s]))
        fractions += f
        ledger_rows += [(user, t, symbols[t], q) for t, q in ledger.items()]
        original_rows += [(user, block[start + i], timestamp[start + i], tx_hash[start + i],
                           t, symbols[t], q, source) for i, t, q, source in contributions]
        start = end
    return fractions, ledger_rows, original_rows, n_missing


def main():
    load_credentials()
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": S3_ENDPOINT})  # keys from the AWS_* env vars set above
    supplies, reserve_symbols, last_block, aave_txs = load_events(fs)
    skip = cut_eth_users(fs)
    seen, ledger_rows, original_rows = set(), [], []
    n_rows = n_kept = n_forced = n_missing = n_eth_kept = 0
    tmp_file = f"{OUT_FILE}.tmp"  # OUT_FILE is replaced only once complete

    with fs.open(INPUT_FILE, "rb") as src_file, fs.open(tmp_file, "wb") as out_file:
        reader = pq.ParquetFile(src_file)
        eth, eth_index = load_eth(fs, reader.schema_arrow, last_block, aave_txs, skip)
        print(f"{eth.num_rows:,} ETH rows for {len(eth_index):,} users, "
              f"{len(skip)} users skipped (ETH history cut)", flush=True)
        writer = None
        pending = None

        def process(table):
            nonlocal writer, n_rows, n_kept, n_forced, n_missing, n_eth_kept
            table = add_eth(table, eth, eth_index)
            fractions, ledgers, originals, missing = trace_chunk(table, seen, supplies, reserve_symbols)
            ledger_rows.extend(ledgers)
            original_rows.extend(originals)
            n_missing += missing
            fractions = pa.array(fractions, pa.float64())
            table = table.append_column("traced_fraction", fractions)
            table = table.filter(pc.greater(fractions, 0.0))
            writer = writer or pq.ParquetWriter(out_file, table.schema)
            writer.write_table(table)
            n_rows += len(fractions)
            n_kept += table.num_rows
            n_forced += int(pc.sum(table.column("tracked")).as_py() or 0)
            native = pc.and_(pc.equal(table.column("token_address"), WETH),
                             pc.equal(table.column("token_symbol"), "ETH"))
            n_eth_kept += int(pc.sum(native).as_py() or 0)
            print(f"{n_rows:,} transfers read, {n_kept:,} kept", flush=True)

        for batch in reader.iter_batches(batch_size=CHUNK_ROWS):
            table = pa.Table.from_batches([batch])
            # after the last event, Aave transactions could not be recognised: dropped
            table = table.filter(pc.less_equal(table.column("block_number"), last_block))
            table = table.filter(pc.invert(pc.is_in(pc.utf8_lower(table.column("user")),
                                                     pa.array(sorted(skip), pa.string()))))
            if pending is not None:
                table = pa.concat_tables([pending, table])
            if not len(table):
                continue
            # Carry the last user over: its rows may continue in the next chunk
            users = table.column("user")
            last = users[-1].as_py()
            split = len(table)
            while split > 0 and users[split - 1].as_py() == last:
                split -= 1
            pending = table.slice(split)
            if split:
                process(table.slice(0, split))
        if pending is not None and len(pending):
            process(pending)
        writer.close()
    fs.mv(tmp_file, OUT_FILE)

    ledgers = pd.DataFrame(ledger_rows, columns=["user", "token_address", "token_symbol", "final_quantity"])
    with fs.open(LEDGERS_FILE, "wb") as f:
        ledgers.to_parquet(f, index=False)

    print(f"\nTransfers up to block {last_block:,} (last event)")
    print(f"{len(seen):,} users, {n_kept:,} of {n_rows:,} transfers kept ({n_kept / n_rows:.1%}), "
          f"{n_forced:,} of them forced, {n_eth_kept:,} of them ETH, saved to {OUT_FILE}")
    print(f"{len(ledgers):,} ledger entries ({(ledgers['final_quantity'] > 0).sum():,} positive), "
          f"saved to {LEDGERS_FILE}")

    original = pd.DataFrame(original_rows, columns=["user", "block_number", "timestamp", "tx_hash",
                                                    "token_address", "token_symbol", "quantity", "source"])
    with fs.open(ORIGINAL_FILE, "wb") as f:
        original.to_parquet(f, index=False)
    print(f"{len(original):,} original portfolio contributions "
          f"({original['source'].value_counts().to_dict()}), saved to {ORIGINAL_FILE}")
    if n_missing:
        print(f"WARNING: {n_missing:,} Supply transactions have no transfer of their user: ignored")


if __name__ == "__main__":
    main()
