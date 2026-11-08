# Context for the next Claude agent

Handover notes from the previous session (up to 2026-11-08). This file covers
the Aave V3 transfers pipeline, and mostly `trace_wallets.py`, the script
being worked on. The user (bastienlevyguinot) edits the scripts between
sessions: read a script before trusting what this file says about it.

## Status: what to do first

1. **Check that S3 works.** The last session ended with expired Onyxia S3
   credentials (`PermissionError: The Access Key Id you provided does not exist`).
   Test with `python -c "import s3fs; print(s3fs.S3FileSystem().ls('bastienlevyguinot/diffusion'))"`.
2. **`trace_wallets.py` has never run on the real data.** It was only tested
   on synthetic data (see "Testing" below). Run `python trace_wallets.py`,
   which reads about 47M rows in one pass, and look at the share of rows kept.
3. **One design decision is open: case 1 (pure receive).** It is described
   below and must be settled with the user before the output is used.

## Environment

- Onyxia (SSP Cloud) VS Code service. The data lives on the MinIO S3 bucket
  `s3://bastienlevyguinot/`. `s3fs.S3FileSystem()` with no arguments picks up
  the `AWS_*` environment variables that Onyxia injects. They expire: when
  they do, the user restarts the service or pastes new ones.
- **Etherscan:** the key is in the `ETHERSCAN_KEY` environment variable (an
  Onyxia secret). The free plan allows **3 calls/s** (the scripts use 2.8).
- **Alchemy:** `ALCHEMY_API_KEY` is in `.env`. Its **monthly capacity was
  exhausted on 2026-10-31.** The RPC scripts use the free public node
  `https://ethereum.publicnode.com` instead. It needs no key and accepts
  JSON-RPC batches of 50 calls. Most other public nodes refuse batches.
- `web3` and `python-dotenv` may be missing: `pip install web3 python-dotenv`.
- **`.env` is tracked by git** and holds API keys. The user was told to run
  `git rm --cached .env` and add it to `.gitignore`; check whether they did.
  Never commit it. `__pycache__/` must stay out of commits too.

## Data pipeline (in order)

| Step | Script | Output (on S3) |
|---|---|---|
| Aave V3 Pool events | `fetch_events_web3.py` | `aave_v3/aave_v3_events.parquet` |
| ERC-20 transfers of every event `user` (Etherscan `tokentx`) | `fetch_transfers_etherscan.py` | `aave_v3/transfers_parts/batch_XXXXX.parquet` (208 batches, raw, never modified) |
| Completeness checks | `transfers_fetching_checks.py`, `transfers_fetching_marginal_checks.py` | printed only |
| Drop reserves, tokens, burn addresses | `drop_non_wallets.py` | `aave_v3/transfers_parts_wallets/`, `aave_v3/dropped_addresses.csv` |
| Drop contracts (`eth_getCode`) | `transfers_drop_contracts.py` | batches rewritten in place, `diffusion/transfers_wallets.parquet`, `aave_v3/code_types_parts/` (cache) |
| Events of kept wallets | `preprocess_events.py` | `diffusion/aave_v3_events_wallets.parquet` |
| `tracked` column + structure checks | `match_events_transfers.py` | `diffusion/transfers_wallets.parquet` (rebuilt from the batches), `aave_v3/events_transfers_checks.parquet` |
| Drop users truncated by the fetch cap (events after their last transfer) | `drop_truncated.py` | filters every file above |
| Drop the other incomplete users (one Etherscan call each) | `transfers_drop_incomplete.py` | filters every file above |
| **Trace flows from Aave activity** | **`trace_wallets.py`** | `diffusion/transfers_wallets_traced.parquet`, `aave_v3/filtering_ledgers.parquet` |

`dropped_addresses.csv` lists every excluded address with a `reason`:
`contract` 31,826, `token` 368, `truncated` 83, `incomplete` 46,
`reserve+token` 2, `burn` 1.

**State after the last run:** `transfers_wallets.parquet` has 46,916,198
transfers of 175,453 users. `aave_v3_events_wallets.parquet` has 2,483,263
events, with exactly the same set of users. About 10% of the transfers are
`tracked`. Every remaining event passes every structure check.

### Facts about the data that matter

- **Transfer columns:** `user, block_number, timestamp, tx_hash, from, to,
  token_address, token_symbol, token_name, token_decimal, value, tracked`.
  - `value` is the raw integer as a **string**; divide by
    `10**token_decimal` to get token units.
  - Addresses are checksummed (mixed case): **always compare them lowercased.**
- **A transfer between two users appears twice,** once in each user's rows.
  That is why `tracked` matches on `(tx_hash, user)`, not on `tx_hash` alone.
- **Each user's rows are contiguous** in `transfers_wallets.parquet`, in
  ascending block order (Etherscan `sort=asc`). The batches hold whole users
  and are written in order.
- **There is no `logIndex` and no transaction index.** Two identical transfers
  in one transaction are legitimate, so never deduplicate. The order of a
  user's transactions within one block is Etherscan's order, which may not be
  the true order.
- **Only ERC-20 transfers are in the data.** There is no native ETH (neither
  `txlist` nor `txlistinternal` was fetched), and aToken interest emits no
  transfer.
- **`tracked = True`:** the user has an Aave event (Supply, Withdraw, Borrow,
  Repay or LiquidationCall) in the same transaction. It has the same value
  for every row of a `(tx_hash, user)`.
- **The event `user` column** is `onBehalfOf` for Supply and Borrow, and the
  `user` argument for the other events (see `decode` in `fetch_events_web3.py`).

## `trace_wallets.py`: how it works

It was adapted from `To_Be_Adapted/trace_wallets.py`, a script from another
project. That project processed one CSV per user, in which rows had already
been reduced to borrow-related flows. Keep the original for reference and do
not edit it.

**Goal:** keep only the transfers of each wallet that can be traced back to
its Aave activity. The script builds one **ledger** per user,
`token_address (lowercase) -> traced quantity in token units`, and computes a
`traced_fraction` in [0, 1] for each transfer. Rows with `traced_fraction > 0`
are written out.

### Per user (`trace_user`, a pure function)

The script groups the user's rows by `tx_hash`, in order of first appearance,
which is the fetch order. For each transaction, it sums by token:
- `out[token]`: the amounts where `from == user`;
- `inc[token]`: the amounts where `to == user`.

Then:

- **Forced transaction** (`tracked` is true): fraction 1 for every row. Every
  `out` is debited from the ledger and every `inc` is credited, with no floor.
  A token never seen before therefore goes negative. For example, a first
  Supply of USDC that never arrived in the wallet leaves USDC at a negative
  balance.
- **Normal transaction:** a token counts as "present" if its ledger balance
  is > 0.
  1. **Nothing sent:** every `inc` is credited in full, fraction 1.
     **This is the open issue; see below.**
  5. **Something sent, but no sent token is present:** fraction 0 for every
     row, ledger untouched (even the incoming tokens are not credited).
  2/3/4. **At least one sent token is present:**
     - each present token gets `out_frac = min(1, balance / amount)`, or 1 if
       the amount is 0;
     - `combined = min(out_frac)` over the present tokens only;
     - present tokens are depleted, floored at 0;
     - absent sent tokens get `-(combined * amount)` (case 4: "untraced
       complement consumed");
     - every `inc` is credited with `combined * amount`;
     - rows sending a present token get their own `out_frac`, every other row
       of the transaction gets `combined`.

The function returns `(fractions in row order, final ledger)`.

### Reading the data (`trace_chunk`, `main`)

- The input is read with `iter_batches(CHUNK_ROWS = 1,000,000)`. The rows of
  the last user of each chunk are carried over to the next chunk
  (`pending`), so a user is never split.
- A `seen` set raises an error if a user's rows are not contiguous.
- The output is written to `OUT_FILE.tmp` and moved to `OUT_FILE` only at
  the end, so a crash cannot leave a half-written file. `LEDGERS_FILE` is
  written afterwards: one row per `(user, token)` ever touched, with
  `token_symbol` and `final_quantity`, zero and negative entries included.
- The script writes only the filtered file. Unlike the original, there is no
  annotated full copy.

### Differences from the original

| Original | Here |
|---|---|
| forced = `kept_reason` in {disbursement, repayment} | forced = `tracked`, i.e. **every** Aave event type |
| forced and normal rows could share a transaction | impossible: `tracked` is per `(tx, user)`, so the whole transaction is forced or normal |
| `asset_key` merged ETH and WETH | `token_address` (no native ETH in the data) |
| `transfer_from_label == "user"` | `from.lower() == user.lower()` |
| ordered by `transfer_time` | fetch order (ascending block) |

## Edge cases and known weaknesses

**Open design issue: case 1 keeps almost everything.** The input is each
wallet's **entire** ERC-20 history, not just the borrow-related flows the
original script received. Case 1 credits every incoming transfer in full:
salaries, CEX withdrawals, swap outputs received alone, and **spam or
airdrop tokens**. All of them are kept, along with everything later paid
with them. In the synthetic test, 82% of the rows were kept. Two fixes were
proposed to the user, who has not chosen yet:
- (a) in case 1, credit only tokens that are already present in the ledger;
- (b) start each user's ledger at their first forced transaction, and give
  everything before it fraction 0.

Ask the user before changing it.

Other edge cases, and how the code currently handles them:

- **Self-transfer** (`from == to == user`): it counts in both `out` and `inc`.
  - Forced: the net change is 0.
  - Normal, token present: depleted then re-credited at `combined`, so the
    net change is 0 when `combined = 1`, and the balance is capped at the old
    balance otherwise.
  - Normal, token absent: case 5, ledger untouched.
- **Zero-value transfers** (address-poisoning spam often sends 0 tokens "from"
  the victim):
  - token present: `out_frac = 1`, the row is kept, the ledger is unchanged;
  - token absent: case 5, the row is dropped.
  Incoming spam falls under case 1 and is kept, which is part of the open
  issue.
- **Fake tokens emitting `Transfer(from=user)`** without the user acting:
  the token is never present, so case 5 drops them.
- **Debt tokens** (variableDebt…): they are minted to the user by Borrow and
  burned by Repay, both forced. They cannot be transferred, so they only sit
  in the ledger. They are harmless, but they show up in `filtering_ledgers`.
- **aToken interest:** it accrues without any transfer, so aToken balances
  in the ledger understate the real balance. Sending more aTokens than were
  minted gives `out_frac < 1`. Withdraw is forced, so withdrawals themselves
  are unaffected.
- **Mints to the user from `0x0`** outside Aave transactions (LP tokens,
  other protocols): they are a pure receive (case 1) when nothing is sent,
  otherwise case 3, credited at `combined`.
- **Floats:** amounts are `float(value) / 10**decimals`, so the precision is
  about 1e-16 relative. A balance meant to be 0 can stay at a tiny positive
  value and still count as present. An epsilon threshold may be worth
  adding.
- **`token_decimal` NA** is treated as 0 decimals. The integer columns
  contain no NA (checked earlier).
- **Multi-hop flows** go through other wallets (A borrows, sends to B, B
  swaps…). The ledger is per user, so B's copy of that transfer is a pure
  receive for B: B's ledger never learns that it came from A's borrow.

## Testing

There are no automated tests. What was done:
- `trace_user` on one synthetic user covering cases 1 to 5 and a forced
  borrow. The output was checked by hand:
  `{'USDC': 0.0, 'WETH': 0.5, 'PEPE': -10.0, 'LP': 2.5, 'DAI': 7.0}`, with
  fractions 1, 1, 0, 0.5…
- `main()` end to end on a local synthetic Parquet file of 50 users, with
  `CHUNK_ROWS = 37` to force carry-overs. The trick is to monkeypatch
  `trace_wallets.s3fs.S3FileSystem = lambda: fsspec.filesystem("file")` and
  the three path constants. The result was identical to running `trace_user`
  on each user in one go.

Before trusting a full run, compare the output against a few real users
(for example the first user, `0x7FA5195595EFE0dFbc79f03303448af3FbE4ea91`)
and read their ledgers.

## Conventions in this repo

- One script per step, with a module docstring that explains the method and
  ends with `Usage:`. Settings are aligned module-level constants. Comments
  are short.
- Paths are on S3. Files are written to a temporary file then moved, or
  rewritten in place only after the result is built.
- Steps that make API calls cache their results on S3 so they can resume.
- Write in English in the code. The user writes in English and sometimes
  French.
