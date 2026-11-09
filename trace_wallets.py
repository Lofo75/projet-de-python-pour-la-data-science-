"""
Keep the transfers of each wallet that can be traced back to its Aave V3
activity, with a per-user, chronological portfolio ledger.

Adapted from To_Be_Adapted/trace_wallets.py, with simpler rules. Each user
has one ledger, token_address -> traced quantity (in token units, >= 0):
the part of the wallet's balance that comes from Aave. The user's
transfers are grouped by transaction, in the order they were fetched
(ascending block), and every row of a transaction gets the same
traced_fraction f:

  - Aave transaction (tracked = True, i.e. the user has an Aave event in
    it, see match_events_transfers.py): f = 1.
  - Any other transaction: f = min(1, balance / amount) over the tokens sent
    by the user (amount > 0) that have a traced balance > 0 (weakest link;
    sent tokens with no traced balance are ignored, and so are zero-value
    sends, often address-poisoning spam). f = 0 if there are none, which
    includes every pure receive: tokens coming from outside (CEX
    withdrawals, payments, airdrops, spam) are never traced.

When f > 0, every token sent is debited from the ledger (floored at 0) and
every token received is credited with f * amount. When f = 0, the ledger is
untouched. So the ledger starts at the user's first Aave transaction and
only grows through Aave transactions and through what is obtained by
spending traced tokens (swaps, LP deposits, ...).

Transfers from the user to themselves count as both sent and received. Debt
tokens are traced like any other ERC-20 token. The data
has no native ETH, so unlike the original no ETH/WETH merge is needed.

ORIGINAL PORTFOLIO: the user's own funds that entered the traced flows, as
a log of contributions (every user is treated as supplying their own
collateral, and aTokens are assets like any other):
  - "supply": in an Aave transaction, each reserve supplied (Supply events
    of EVENTS_FILE, user = onBehalfOf), minus the traced tokens re-supplied:
    min(balance, amount sent by the user), which is debited from the
    ledger. Re-supplying borrowed or withdrawn tokens is not counted again;
    a Supply paid by someone else (or in native ETH) counts in full.
  - "complement": in another transaction with f > 0, each token sent with
    no traced balance, for f * amount: the own funds consumed alongside the
    traced ones (e.g. own ETH added to borrowed USDC in an LP deposit).
    Debt tokens (symbol starting with DEBT_PREFIXES) are never complements:
    burning one repays a loan, it does not spend own funds.

Transfers after the last block of EVENTS_FILE are dropped: there, Aave
transactions could not be recognised. The rows of INPUT_FILE are read in
chunks of CHUNK_ROWS (a user is split
across chunks at most at a chunk border: its rows are carried over to the
next chunk). Outputs:
  - OUT_FILE:     the transfers with traced_fraction > 0, plus that column;
  - LEDGERS_FILE: each user's final ledger, one row per token touched by a
    transaction with f > 0 (zeroed entries included);
  - ORIGINAL_FILE: the original portfolio contributions, one row per
    (transaction, token, source). Sum them by user and token for the
    portfolio, or cumulate them by block for its history.
The decimals and symbols of the reserves are read on RPC_URL.
Usage:
    python3 trace_wallets.py
"""
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import s3fs

INPUT_FILE    = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
EVENTS_FILE   = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"
OUT_FILE      = "s3://bastienlevyguinot/diffusion/transfers_wallets_traced.parquet"
LEDGERS_FILE  = "s3://bastienlevyguinot/aave_v3/filtering_ledgers.parquet"
ORIGINAL_FILE = "s3://bastienlevyguinot/diffusion/original_portfolios.parquet"

RPC_URL = "https://ethereum.publicnode.com"

CHUNK_ROWS   = 1_000_000
MIN_FRACTION = 1e-6  # smaller traced fractions are float residue: treated as 0
DEBT_PREFIXES = ("variableDebt", "stableDebt")  # debt tokens of Aave and its forks: never own funds


def trace_user(user, tx_hash, src, dst, token, amount, tracked, supplies=None, debt_tokens=()):
    """traced_fraction of each transfer of one user (lists in fetch order), the final ledger
    and the contributions to the original portfolio.

    user, src, dst and token are lowercased addresses; amount is in token units. supplies is
    tx_hash -> {reserve: amount supplied} from the user's Supply events. debt_tokens are
    traced like any token but never counted as complements. Each contribution is (index of the
    first row of its transaction, token, quantity, source)."""
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

        if tracked[rows[0]]:  # Aave transaction (tracked is the same for every row of it)
            f = 1.0
            # the traced tokens re-supplied are the part of the user's transfer that the ledger
            # covers (debited below); the rest of the amount supplied is own funds
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
    """Lowercased reserve -> (decimals, symbol), read with decimals() and symbol() on RPC_URL."""
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
        if len(raw) > 32:  # ABI string: offset, length, data
            symbol = raw[64:64 + int.from_bytes(raw[32:64], "big")].decode(errors="replace")
        else:  # bytes32 (e.g. MKR)
            symbol = raw.rstrip(b"\0").decode(errors="replace")
        info[r] = (int.from_bytes(results[(r, "0x313ce567")], "big"), symbol)
    return info


def load_supplies(fs):
    """user -> tx_hash -> {reserve: amount supplied in token units}, reserve -> symbol, and the
    last block of the events."""
    with fs.open(EVENTS_FILE, "rb") as f:
        ev = pd.read_parquet(f, columns=["event_name", "block_number", "user", "tx_hash", "reserve",
                                         "amount_raw"])
    last_block = int(ev["block_number"].max())
    ev = ev[ev["event_name"] == "Supply"]
    ev = ev.assign(user=ev["user"].str.lower(), tx_hash=ev["tx_hash"].str.lower(),
                   reserve=ev["reserve"].str.lower())
    info = reserve_info(sorted(ev["reserve"].unique()))
    supplies = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for u, h, r, a in zip(ev["user"], ev["tx_hash"], ev["reserve"], ev["amount_raw"]):
        supplies[u][h][r] += int(a) / 10 ** info[r][0]  # several Supply events of one tx are summed
    return supplies, {r: sym for r, (_, sym) in info.items()}, last_block


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
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    supplies, reserve_symbols, last_block = load_supplies(fs)
    seen, ledger_rows, original_rows = set(), [], []
    n_rows = n_kept = n_forced = n_missing = 0
    tmp_file = f"{OUT_FILE}.tmp"  # OUT_FILE is replaced only once complete

    with fs.open(INPUT_FILE, "rb") as src_file, fs.open(tmp_file, "wb") as out_file:
        reader = pq.ParquetFile(src_file)
        writer = None
        pending = None

        def process(table):
            nonlocal writer, n_rows, n_kept, n_forced, n_missing
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
            print(f"{n_rows:,} transfers read, {n_kept:,} kept", flush=True)

        for batch in reader.iter_batches(batch_size=CHUNK_ROWS):
            table = pa.Table.from_batches([batch])
            # after the last event, Aave transactions could not be recognised: dropped
            table = table.filter(pc.less_equal(table.column("block_number"), last_block))
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
          f"{n_forced:,} of them forced, saved to {OUT_FILE}")
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
