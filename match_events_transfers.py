"""
Match the Aave V3 events of wallets to their ERC-20 transfers.

A transfer is "tracked" when an event of the same user happened in the same
transaction (same tx_hash and user). The transfers of WALLETS_DIR are written
to OUT_FILE with a boolean "tracked" column.

Each event is also checked against the transfers it must emit, seen from the
user's transfers (Z = null address, U = reserve, A = its aToken, D = its
variable debt token, u = user):
  Supply           s_mint        A: Z -> u
                   s_underlying  U: caller -> A, value = amount      (if caller = u)
  Withdraw         w_burn        A: u -> Z (or Z -> u when the interest
                                 accrued exceeds the amount withdrawn)
                   w_underlying  U: A -> recipient, value = amount   (if recipient = u)
  Borrow           b_mint        D: Z -> u                            (variable rate only)
                   b_underlying  U: A or Z (GHO) -> caller, value = amount (if caller = u)
  Repay            r_burn        D: u -> Z (or Z -> u)
                   r_underlying  U: repayer -> A, value = amount     (if repayer = u, not useATokens)
                   r_atoken      A: u -> Z (or Z -> u)              (if useATokens)
  LiquidationCall  l_debt_burn   D: u -> Z (or Z -> u)
                   l_collateral  A of the collateral: u -> anyone (or Z -> u)
A transfer can satisfy several events of the same transaction. An event is
"complete" when every expected transfer is found. Events after the last
transfer fetched for their user are flagged "after_last_transfer": those
users' histories were cut by MAX_CALLS_PER_USER, so missing transfers there
are expected. The checks are saved to CHECKS_FILE, one row per event.

The aToken and debt token of each reserve are read from the Pool
(getReserveData) on RPC_URL, a free public node. The batches are processed
one at a time (a user is in a single batch). Usage:
    python3 match_events_transfers.py
"""
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs

EVENTS_FILE = "s3://bastienlevyguinot/diffusion/aave_v3_events_wallets.parquet"
WALLETS_DIR = "s3://bastienlevyguinot/aave_v3/transfers_parts_wallets"
OUT_FILE    = "s3://bastienlevyguinot/diffusion/transfers_wallets.parquet"
CHECKS_FILE = "s3://bastienlevyguinot/aave_v3/events_transfers_checks.parquet"

RPC_URL      = "https://ethereum.publicnode.com"
POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"  # Aave V3 Pool (proxy)
GET_RESERVE_DATA = "0x35ea6a75"  # getReserveData(address)

NULL_ADDRESS = "0x0000000000000000000000000000000000000000"
Z = NULL_ADDRESS

BATCH_RE = re.compile(r"batch_(\d+)\.parquet$")


def reserve_tokens(reserves):
    """Lowercased reserve -> (aToken, variable debt token), words 8 and 10 of getReserveData."""
    payload = [{"jsonrpc": "2.0", "id": i, "method": "eth_call",
                "params": [{"to": POOL_ADDRESS, "data": GET_RESERVE_DATA + r[2:].rjust(64, "0")}, "latest"]}
               for i, r in enumerate(reserves)]
    answers = requests.post(RPC_URL, json=payload, timeout=60).json()
    tokens = {}
    for answer in answers:
        words = answer["result"][2:]
        word = lambda k: "0x" + words[64 * k + 24:64 * (k + 1)]
        tokens[reserves[answer["id"]]] = (word(8), word(10))
    return tokens


def load_events():
    """Events with lowercased addresses and the aToken / debt token of their reserves."""
    ev = pd.read_parquet(EVENTS_FILE)
    ev["event_id"] = range(len(ev))
    for col in ["tx_hash", "reserve", "caller", "recipient", "repayer", "liquidator", "collateral_asset"]:
        ev[col] = ev[col].str.lower()
    ev["u"] = ev["user"].str.lower()
    reserves = sorted(set(ev["reserve"].dropna()) | set(ev["collateral_asset"].dropna()))
    tokens = reserve_tokens(reserves)
    ev["a_token"] = ev["reserve"].map(lambda r: tokens[r][0])
    ev["d_token"] = ev["reserve"].map(lambda r: tokens[r][1])
    ev["a_collateral"] = ev["collateral_asset"].map(lambda r: tokens[r][0], na_action="ignore")
    return ev


def flow(m, token, src, dst):
    """Transfers of token from src to dst (columns of m or a constant address)."""
    col = lambda x: m[x] if x in m else x
    return (m["token"] == col(token)) & (m["src"] == col(src)) & (m["dst"] == col(dst))


def mint_or_burn(m, token):
    return flow(m, token, "u", Z) | flow(m, token, Z, "u")


# rule -> (event, is the transfer expected for this event, does a transfer row satisfy it)
RULES = {
    "s_mint":       ("Supply",   lambda e: True,
                     lambda m: flow(m, "a_token", Z, "u")),
    "s_underlying": ("Supply",   lambda e: e["caller"] == e["u"],
                     lambda m: flow(m, "reserve", "caller", "a_token") & (m["value"] == m["amount_raw"])),
    "w_burn":       ("Withdraw", lambda e: True,
                     lambda m: mint_or_burn(m, "a_token")),
    "w_underlying": ("Withdraw", lambda e: e["recipient"] == e["u"],
                     lambda m: flow(m, "reserve", "a_token", "recipient") & (m["value"] == m["amount_raw"])),
    "b_mint":       ("Borrow",   lambda e: e["interest_rate_mode"] == 2,
                     lambda m: flow(m, "d_token", Z, "u")),
    "b_underlying": ("Borrow",   lambda e: e["caller"] == e["u"],
                     lambda m: (flow(m, "reserve", "a_token", "caller") | flow(m, "reserve", Z, "caller"))
                     & (m["value"] == m["amount_raw"])),
    "r_burn":       ("Repay",    lambda e: True,
                     lambda m: mint_or_burn(m, "d_token")),
    "r_underlying": ("Repay",    lambda e: (e["repayer"] == e["u"]) & ~e["use_atokens"].fillna(False),
                     lambda m: flow(m, "reserve", "repayer", "a_token") & (m["value"] == m["amount_raw"])),
    "r_atoken":     ("Repay",    lambda e: e["use_atokens"].fillna(False),
                     lambda m: mint_or_burn(m, "a_token")),
    "l_debt_burn":  ("LiquidationCall", lambda e: True,
                     lambda m: mint_or_burn(m, "d_token")),
    "l_collateral": ("LiquidationCall", lambda e: True,
                     lambda m: ((m["token"] == m["a_collateral"]) & (m["src"] == m["u"]))
                     | flow(m, "a_collateral", Z, "u")),
}


def check_events(ev, tr):
    """One row per event: each rule's result (NA if not expected) and whether all were found."""
    t = pd.DataFrame({"tx_hash": tr["tx_hash"].str.lower(), "user": tr["user"],
                      "token": tr["token_address"].str.lower(), "src": tr["from"].str.lower(),
                      "dst": tr["to"].str.lower(), "value": tr["value"]})
    m = ev.merge(t, on=["tx_hash", "user"])
    out = ev[["event_id"]].copy()
    for rule, (name, expected, match) in RULES.items():
        found = pd.Series(False, index=ev.index)
        found_ids = set(m.loc[(m["event_name"] == name) & match(m).fillna(False), "event_id"])
        found[ev["event_id"].isin(found_ids)] = True
        is_expected = pd.Series(expected(ev), index=ev.index).fillna(False).astype(bool)
        is_expected &= ev["event_name"] == name
        out[rule] = found.astype("boolean").where(is_expected)
    out["complete"] = out[list(RULES)].fillna(True).all(axis=1)
    return out


def main():
    fs = s3fs.S3FileSystem()  # endpoint and credentials come from Onyxia's AWS_* env vars
    events = load_events()
    print(f"{len(events):,} events")

    fs.invalidate_cache(WALLETS_DIR)
    paths = sorted(p for p in fs.glob(f"{WALLETS_DIR}/batch_*.parquet") if BATCH_RE.search(p))
    checks, last_blocks = [], []
    n_rows = n_tracked = 0
    tmp_file = f"{OUT_FILE}.tmp"  # OUT_FILE is replaced only once complete
    with fs.open(tmp_file, "wb") as f:
        writer = None
        for path in paths:
            with fs.open(path, "rb") as part:
                table = pq.read_table(part)
            tr = table.to_pandas()
            ev = events[events["user"].isin(set(tr["user"]))]

            keys = pd.MultiIndex.from_frame(ev[["tx_hash", "user"]])
            tracked = pd.MultiIndex.from_arrays([tr["tx_hash"].str.lower(), tr["user"]]).isin(keys)
            table = table.append_column("tracked", pa.array(tracked, pa.bool_()))
            writer = writer or pq.ParquetWriter(f, table.schema)
            writer.write_table(table)
            n_rows += len(tr)
            n_tracked += int(tracked.sum())

            checks.append(check_events(ev, tr))
            last_blocks.append(tr.groupby("user")["block_number"].max())
        writer.close()
    fs.mv(tmp_file, OUT_FILE)
    print(f"{n_tracked:,} of {n_rows:,} transfers tracked ({n_tracked / n_rows:.1%}), saved to {OUT_FILE}")

    checks = events[["event_id", "event_name", "block_number", "tx_hash", "log_index", "user"]].merge(
        pd.concat(checks), on="event_id", how="left")
    last_block = checks["user"].map(pd.concat(last_blocks))
    checks["after_last_transfer"] = last_block.isna() | (checks["block_number"] > last_block)
    checks = checks.drop(columns="event_id")
    with fs.open(CHECKS_FILE, "wb") as f:
        checks.to_parquet(f, index=False)

    print("\nExpected transfers found, by rule")
    rows = []
    for rule, (name, _, _) in RULES.items():
        col = checks[rule].dropna()
        rows.append({"event": name, "rule": rule, "expected": len(col), "found": int(col.sum()),
                     "share": col.mean() if len(col) else float("nan")})
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nComplete events, by type (before / after the last transfer fetched for the user)")
    summary = checks.groupby(["event_name", "after_last_transfer"])["complete"].agg(["size", "sum", "mean"])
    print(summary.rename(columns={"size": "events", "sum": "complete", "mean": "share"}).to_string())
    print(f"\nChecks saved to {CHECKS_FILE}")


if __name__ == "__main__":
    main()
