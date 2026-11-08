"""
Keep the transfers of each wallet that can be traced back to its Aave V3
activity, with a per-user, chronological portfolio ledger.

Adapted from To_Be_Adapted/trace_wallets.py. Each user has one ledger,
token_address -> traced quantity (in token units, can go negative, see
case 4). The user's transfers are grouped by transaction, in the order they
were fetched (ascending block), and every transaction gets a
traced_fraction:

  FORCED transactions (tracked = True, i.e. the user has an Aave event in
  the same transaction, see match_events_transfers.py): fraction = 1. Every
  token sent by the user is debited from the ledger, every token received is
  credited.

  NORMAL transactions (everything else), from the tokens sent ("out") and
  received ("in") by the user:
    1. Pure receive (nothing sent): every token received is credited in full,
       fraction = 1.
    2. Pure send of tracked tokens: their balance is depleted, floored at 0.
    3. Tracked tokens sent alongside tokens received (e.g. a swap): the
       weakest-link fraction min(balance / amount) over the tokens sent is
       credited for the tokens received.
    4. Tracked and untracked tokens sent together: the fraction comes from
       the tracked tokens only; each untracked token gets a negative entry of
       -(fraction * amount), the untraced complement consumed.
    5. Only untracked tokens (absent or at <= 0) sent: fraction = 0, ledger
       untouched.
  In cases 2-4 a sent token gets its own balance / amount ratio, every other
  row of the transaction gets the combined fraction.

A token is "tracked" when its ledger balance is > 0. Transfers from the user
to themselves count as both sent and received. The data has no native ETH,
so unlike the original no ETH/WETH merge is needed.

The rows of INPUT_FILE are read in chunks of CHUNK_ROWS (a user is split
across chunks at most at a chunk border: its rows are carried over to the
next chunk). Outputs:
  - OUT_FILE:     the transfers with traced_fraction > 0, plus that column;
  - LEDGERS_FILE: each user's final ledger, one row per token ever touched
    (zeroed and negative entries included).
Usage:
    python3 trace_wallets.py
"""
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import s3fs

INPUT_FILE   = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
OUT_FILE     = "s3://bastienlevyguinot/diffusion/transfers_wallets_traced.parquet"
LEDGERS_FILE = "s3://bastienlevyguinot/aave_v3/filtering_ledgers.parquet"

CHUNK_ROWS = 1_000_000


def trace_user(user, tx_hash, src, dst, token, amount, tracked):
    """traced_fraction of each transfer of one user (lists in fetch order) and the final ledger.

    user, src, dst and token are lowercased addresses; amount is in token units."""
    fractions = [0.0] * len(tx_hash)
    ledger = {}

    by_tx = defaultdict(list)  # insertion order = order of first appearance
    for i, h in enumerate(tx_hash):
        by_tx[h].append(i)

    for rows in by_tx.values():
        out, inc = defaultdict(float), defaultdict(float)
        for i in rows:
            if src[i] == user:
                out[token[i]] += amount[i]
            if dst[i] == user:
                inc[token[i]] += amount[i]

        if tracked[rows[0]]:  # forced (tracked is the same for every row of a transaction)
            for t, a in out.items():
                ledger[t] = ledger.get(t, 0.0) - a
            for t, a in inc.items():
                ledger[t] = ledger.get(t, 0.0) + a
            for i in rows:
                fractions[i] = 1.0
            continue

        if not out:  # case 1: pure receive
            for t, a in inc.items():
                ledger[t] = ledger.get(t, 0.0) + a
            for i in rows:
                fractions[i] = 1.0
            continue

        present = {t: a for t, a in out.items() if ledger.get(t, 0.0) > 0}
        if not present:  # case 5: nothing traceable, ledger untouched
            continue

        # cases 2/3/4 (a zero amount sent is fully covered)
        out_fracs = {t: min(1.0, ledger[t] / a) if a > 0 else 1.0 for t, a in present.items()}
        combined = min(out_fracs.values())
        for t, a in present.items():
            ledger[t] = max(0.0, ledger[t] - a)
        for t, a in out.items():
            if t not in present:
                ledger[t] = ledger.get(t, 0.0) - combined * a
        if combined > 0:
            for t, a in inc.items():
                ledger[t] = ledger.get(t, 0.0) + combined * a
        for i in rows:
            fractions[i] = out_fracs[token[i]] if src[i] == user and token[i] in out_fracs else combined

    return fractions, ledger


def trace_chunk(table, seen):
    """traced_fraction of every row of a table holding whole users, and their ledgers as rows."""
    users = [u.lower() for u in table.column("user").to_pylist()]
    tx_hash = table.column("tx_hash").to_pylist()
    src = [s.lower() for s in table.column("from").to_pylist()]
    dst = [d.lower() for d in table.column("to").to_pylist()]
    token = [t.lower() for t in table.column("token_address").to_pylist()]
    symbol = table.column("token_symbol").to_pylist()
    decimals = table.column("token_decimal").to_pylist()
    amount = [float(v) / 10 ** (d or 0) for v, d in zip(table.column("value").to_pylist(), decimals)]
    tracked = table.column("tracked").to_pylist()

    fractions, ledger_rows = [], []
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
        f, ledger = trace_user(user, tx_hash[s], src[s], dst[s], token[s], amount[s], tracked[s])
        fractions += f
        symbols = dict(zip(token[s], symbol[s]))
        ledger_rows += [(user, t, symbols[t], q) for t, q in ledger.items()]
        start = end
    return fractions, ledger_rows


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    seen, ledger_rows = set(), []
    n_rows = n_kept = n_forced = 0
    tmp_file = f"{OUT_FILE}.tmp"  # OUT_FILE is replaced only once complete

    with fs.open(INPUT_FILE, "rb") as src_file, fs.open(tmp_file, "wb") as out_file:
        reader = pq.ParquetFile(src_file)
        writer = None
        pending = None

        def process(table):
            nonlocal writer, n_rows, n_kept, n_forced
            fractions, ledgers = trace_chunk(table, seen)
            ledger_rows.extend(ledgers)
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
            if pending is not None:
                table = pa.concat_tables([pending, table])
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

    print(f"\n{len(seen):,} users, {n_kept:,} of {n_rows:,} transfers kept ({n_kept / n_rows:.1%}), "
          f"{n_forced:,} of them forced, saved to {OUT_FILE}")
    print(f"{len(ledgers):,} ledger entries ({(ledgers['final_quantity'] < 0).sum():,} negative), "
          f"saved to {LEDGERS_FILE}")


if __name__ == "__main__":
    main()
