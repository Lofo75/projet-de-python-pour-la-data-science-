# Context for the next Claude agent

Handover notes from the previous session (up to 2026-11-08). This file covers
the Aave V3 transfers pipeline, and mostly `trace_wallets.py`, the script
being worked on. The user (bastienlevyguinot) edits the scripts between
sessions: read a script before trusting what this file says about it.

## Status: what to do first

1. **`trace_wallets.py` has never run on the full data.** The rules were
   simplified on 2026-11-09 (see below) and checked on a sample of the first
   3.0M rows (11,569 users): **17.2% of rows kept**, 10% of them being the
   Aave transactions themselves. Run `python trace_wallets.py` (about 47M rows
   in one pass) and look at the share kept and at a few users' ledgers and
   original portfolios.
2. **Native ETH is now in the tracing** (2026-11-12, see "Native ETH in
   `trace_wallets.py`"). `fetch_eth_etherscan.py` finished on 2026-11-11:
   `diffusion/eth_transfers_wallets.parquet` has 36,356,546 rows for all
   175,453 users (14.3M of them move ETH). The full `trace_wallets.py` run
   has not been done yet.
3. **The events stop on 2026-05-31 (block 25,218,786), the transfers go on to
   2026-10-28.** 13.6% of the sample's transfers are after the last event, so
   their Aave transactions were not `tracked` (real Repay events missing from
   the events file). The user chose to cut: `trace_wallets.py` drops the
   transfers after the last event block. On the sample: 19.0% of rows kept.
4. **Open question: what counts as an "exit"** (idea 2, see "Exits" below).
   Not implemented yet.
5. If S3 fails with `PermissionError: The Access Key Id you provided does not
   exist`, the Onyxia credentials have expired (see "Environment").

## Environment

- Onyxia (SSP Cloud) VS Code service. The data lives on the MinIO S3 bucket
  `s3://bastienlevyguinot/`. `s3fs.S3FileSystem()` with no arguments picks up
  the `AWS_*` environment variables that Onyxia injects. They expire: when
  they do, the user restarts the service or pastes new ones.
- **Etherscan:** the keys are in `.env` as `ETHERSCAN_API_KEY` and
  `ETHERSCAN_API_KEY_2` (read by `fetch_eth_etherscan.py`, which switches
  key on rate limits and when a key reaches its 100,000 calls/day), or in the `ETHERSCAN_KEY` environment variable
  (an Onyxia secret, read by the older scripts). `.env` was untracked on
  2026-11-09 (`git rm --cached`, added to `.gitignore`), but a key had already
  been committed in 1f5f331 and pushed: the user was told to rotate it. The free plan allows **3 calls/s** (the scripts use 2.8).
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
| Native ETH of the wallets (`txlist` + `txlistinternal`) | `fetch_eth_etherscan.py` | `aave_v3/eth_parts/`, `aave_v3/eth_status_parts/` (cut users), `diffusion/eth_transfers_wallets.parquet` |
| **Trace flows from Aave activity** | **`trace_wallets.py`** | `diffusion/transfers_wallets_traced.parquet`, `aave_v3/filtering_ledgers.parquet`, `diffusion/original_portfolios.parquet` |

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
- **`transfers_wallets.parquet` holds ERC-20 transfers only.** Native ETH is
  in `eth_transfers_wallets.parquet` (`fetch_eth_etherscan.py`) and is merged
  in by `trace_wallets.py`. aToken interest emits no transfer.
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

Then one fraction `f` is computed for the whole transaction:
- **Aave transaction** (`tracked` is true): `f = 1`.
- **Any other transaction:** `f = min(1, balance / amount)` over the sent
  tokens with an amount > 0 and a ledger balance > 0. Sent tokens with no
  traced balance, and zero-value sends, are ignored. `f = 0` if no sent token has a traced
  balance, which includes every pure receive. `f < MIN_FRACTION = 1e-6` is
  treated as 0 (float dust).

If `f > 0`: every sent token is debited (floored at 0), then every received
token is credited with `f * amount`, and every row of the transaction gets
`f`. If `f = 0`, the ledger is untouched. **The ledger never goes negative**,
and it is empty until the user's first Aave transaction.

Debt tokens are traced like any other ERC-20 token (the user's choice), so
they appear in the ledgers. They are only excluded from complements (below).

The function returns `(fractions in row order, final ledger, contributions)`.

### Original portfolio (added 2026-11-09)

The user asked for a second dataset: the user's own funds that entered the
traced flows ("the portfolio before supplying anything to Aave"), to compare
with the wallet's exposure. Decisions by the user: every user is treated as
supplying their own collateral (even when the Supply was made on their
behalf), and aTokens are assets like any other. Two sources, both computed
inside `trace_user`:
- **`supply`:** in an Aave transaction, each reserve supplied, from the Supply
  events (`load_supplies`, several events of one transaction summed), minus
  the traced tokens re-supplied: `min(balance, amount sent by the user)`,
  which is exactly what the ledger is debited by (the user's rule: re-supplied
  traced tokens come out of the ledger, not into the contributions). A Supply
  paid by another wallet, or in native ETH through the gateway, counts in
  full and leaves the ledger untouched.
- **`complement`:** in another transaction with `f > 0`, each token sent with
  no traced balance, for `f * amount`. These are the tokens the traced rule
  ignores. This replaces the negative entries of the original script's case 4.
  Debt tokens (`DEBT_PREFIXES`: symbol starting with `variableDebt` or
  `stableDebt`, the naming of Aave and its forks such as Spark) are never
  complements: burning one repays a loan, it does not spend own funds.

Output `ORIGINAL_FILE`: one row per `(transaction, token, source)` with `user,
block_number, timestamp, tx_hash, token_address, token_symbol, quantity,
source`. Sum it for the portfolio, cumulate it by block for its history. The
reserves' decimals and symbols come from `decimals()` / `symbol()` on the RPC
node (`reserve_info`, bytes32 symbols such as MKR handled). The script warns
if a Supply transaction has no transfer of its user (0 on the sample).

On the sample, with the events cut-off: 51,295 `supply` rows and 1,359
`complement` rows (USDC, WBTC,
USDT, XAUt, WETH, Pendle YT/SY…). Not covered: own funds used to **repay**
(Repay is an Aave transaction, so nothing is recorded), and the interest on
supplied funds.

Before zero-value sends were ignored and debt tokens skipped, 10% of the
complements were debt-token burns (repaying loans on forks) and many were
look-alike spam tokens (`UЅDТ`, with a Cyrillic `Ѕ`): an address-poisoning
transaction sends 0 real USDC "from" the victim, which used to give `f = 1`.
That also kept the spam rows in the main output (17.7% -> 17.2% kept).

#### Why the rules were simplified (2026-11-09)

The first version had five cases taken from the original script. Measured on
the 3.0M-row sample:
- **Case 1 (a pure receive credited in full) kept 80% of all rows.** It made
  up 30% of the rows on its own, mostly USDC/USDT inflows (CEX withdrawals,
  payments), and almost every spend of those was then traced too. The
  original script only saw borrow-related flows, so the rule made sense there,
  but not on a wallet's whole history. Option (a), crediting only tokens that
  are already present, still credits outside USDC as soon as some traced USDC
  is held.
- **Negative balances swallowed borrows.** Supplying your own USDC put USDC
  negative, so a later USDC Borrow just filled the hole: with option (a),
  7,178 of 39,464 Borrow transactions (18%) were absorbed entirely, and the
  borrowed money was never traced. Case 4's negative entries were dropped too,
  for the same reason.
- Per-row fractions inside a transaction were dropped: every row gets `f`.

Other variants measured (rows kept): ledger credited only by Borrow and
Withdraw transactions 16.0%, only by Borrow 13.0%, against 17.7% with every
Aave transaction. Every Aave transaction is kept as the rule because it is
the simplest and because `tracked` does not say which event happened. The
consequence: withdrawn principal and the aTokens received on Supply count as
traced.

### Reading the data (`trace_chunk`, `main`)

- The input is read with `iter_batches(CHUNK_ROWS = 1,000,000)`. The rows of
  the last user of each chunk are carried over to the next chunk
  (`pending`), so a user is never split.
- A `seen` set raises an error if a user's rows are not contiguous.
- The output is written to `OUT_FILE.tmp` and moved to `OUT_FILE` only at
  the end, so a crash cannot leave a half-written file. `LEDGERS_FILE` is
  written afterwards: one row per `(user, token)` touched by a transaction
  with `f > 0`, with `token_symbol` and `final_quantity`, zeros included.
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
| 5 cases, pure receives credited, negative entries, per-row fractions | one rule, pure receives never credited, ledger >= 0, one fraction per transaction |

## Edge cases and known weaknesses

- **Self-transfer** (`from == to == user`): it counts in both `out` and `inc`.
  With a traced balance, it is debited then credited with `f * amount`, so the
  balance stays the same when `f = 1`. Without one, `f = 0`.
- **Zero-value transfers** (address-poisoning spam often sends 0 tokens "from"
  the victim): ignored when computing `f`, so a transaction that only sends
  zero amounts is dropped.
- **Fake tokens emitting `Transfer(from=user)`** without the user acting:
  the token never has a traced balance, so the row is dropped.
- **Spam and airdrops sent to the user:** pure receives, never traced.
- **Generous weakest link:** sending borrowed USDC together with untraced ETH
  (an LP deposit, say) gives `f` from the USDC alone, and the LP tokens are
  credited with that `f`.
- **Debt tokens** are recognised by their symbol only (for complements). A
  debt token of a protocol with another naming could be a complement.
- **aToken interest:** it accrues without any transfer, so aToken balances
  in the ledger understate the real balance. Sending more aTokens than were
  minted gives `f < 1`. Withdraw is an Aave transaction, so withdrawals
  themselves are unaffected.
- **Flows that leave and come back in another transaction** (bridges, CEX
  deposits, intent-based swaps settled later) lose their tracing: the return
  is a pure receive.

## Exits (idea 2, open)

The user's view: depositing into DeFi (vault, staking, LP…) always gives a
receipt token, so a send with nothing back ("pure send") and no DeFi
interaction means the funds leave Ethereum. The current code already
treats every pure send as an exit for the ledger (debited, never credited
back). The open question is whether to label exits in the output.

Measured on the sample: 52% of the traced non-Aave transactions are pure
sends (74,300). On 300 random ones, fetched with `eth_getTransactionByHash`:
- **51%** are a plain `transfer()` (selector `0xa9059cbb`) called by the user on
  the token contract: really no protocol (exchange deposit, own other wallet,
  payment);
- the rest call a protocol whose return the data cannot see: native ETH
  (swaps through the Uniswap Universal Router, 1inch, MetaMask Swaps, CoW
  Protocol settlements; native ETH is not fetched), NFTs (Uniswap V3
  positions through the NonfungiblePositionManager: `tokentx` is ERC-20
  only), bridges (LI.FI), the Aave Pool itself on behalf of another user;
- 26% of the transactions were not sent by the user (CoW solvers,
  relayers, Safe executions).
Also: 21% of the pure sends get the same token back later from the same
address, and 6% go to another user of the dataset.

Native ETH also matters for Aave itself: in the sample, 94% of the WETH
Supply events and 84% of the WETH Borrow events are called by another contract
(mostly 0xd01607c3…, 0x89341158…, 0xd322a490…, very likely Aave's
WrappedTokenGateway versions). There the user sends or receives native ETH,
so a native-ETH Borrow credits only the debt token. Withdrawals through the
gateway have the gateway as event `user`: for the wallet they are a pure send
of aWETH. The free public node refuses `trace_transaction` (archive access
needs a token), so native ETH has to come from Etherscan `txlist` (ETH sent)
and `txlistinternal` (ETH received from contracts), per user like
`fetch_transfers_etherscan.py`.

The recommended rule for an exit label is therefore the transaction itself
(`to == token`, selector `transfer`, `from == user`), not "nothing came
back". It costs one `eth_getTransactionByHash` per traced pure send (about
1.2M for the full data, batches of 50 on the public node).
- **Multi-hop flows** go through other wallets (A borrows, sends to B, B
  swaps…). The ledger is per user, so B's copy of that transfer is a pure
  receive for B and is not traced.
- **Floats:** amounts are `float(value) / 10**decimals`, about 1e-16 relative
  precision. `MIN_FRACTION` drops the dust fractions this creates (about 2% of
  the non-Aave rows kept on the sample had `f < 1e-6`).
- **Decimals:** `trace_chunk` uses Python ints from `to_pylist()`. Some
  tokens have 27 decimals, so do not compute `10**decimals` with numpy int64
  in analysis code: it overflows silently.
- **`token_decimal` NA** is treated as 0 decimals. The integer columns
  contain no NA (checked earlier).

## Native ETH in `trace_wallets.py` (2026-11-12)

- `load_eth` reads `ETH_FILE` once, in memory (the machine has ~1 TB of RAM):
  rows with `value > 0`, `is_error = 0`, block <= last event block. They become
  transfer rows with `token_address = WETH`, `token_symbol = "ETH"`,
  `token_name = "Ether"`, 18 decimals: ETH and WETH share one ledger entry
  (its symbol in the ledgers is `WETH+ETH`). `tracked` comes from the events:
  `(tx_hash, user)` has an event.
- **Wraps and unwraps are dropped** (ETH sent to the WETH contract by a normal
  transaction, ETH paid by it in an internal one, ~120k each): WETH emits no
  ERC-20 Transfer for them, so keeping the ETH leg alone would debit or
  credit the shared entry for nothing.
- `add_eth` adds each chunk's users' ETH rows and sorts each user's rows by
  block (stable: within a block, ERC-20 transactions first, since the
  transaction index was not fetched).
- The 15 users whose ETH history was cut (`ETH_STATUS_DIR`, bots with ~20k
  transactions) are skipped entirely, ERC-20 rows included.
- What it fixes: Aave's WrappedTokenGateway pays borrows and withdrawals in
  native ETH (an internal transaction), and swaps to ETH pay out native ETH.
  Gateway withdrawals have the gateway as event `user`, so for the wallet
  they are untracked: aWETH sent (traced) and ETH received, credited at `f`.

Measured on the 3.0M-row sample (cut at the last event): ERC-20 rows kept
19.0% -> 20.0% (26,948 newly kept, 737 no longer); ETH rows kept 9.4%; all
rows 17.6%. Original portfolio: `supply` 51,295 -> 47,241 rows (ETH supplied
through the gateway out of traced ETH is no longer counted as own funds);
`complement` 1,359 -> 5,269. Half of the new complements (2,674) are own ETH
with a median of 0.0001 ETH sent alongside traced tokens, mostly to bridges
and Uniswap V3 positions: fees (bridge messaging) rather than exposure. Not
filtered; the user may want a threshold.

## Testing

There are no automated tests. With native ETH (2026-11-12): a synthetic
user (ETH borrowed through the gateway, swapped for USDC, WETH sold from the
same entry); `main()` on 300 real users with their ETH, `CHUNK_ROWS = 937`
and one user marked as cut: identical to a one-pass run, the cut user absent,
every user's rows in block order. The paths `ETH_FILE` and `ETH_STATUS_DIR`
must be monkeypatched too.

What was done earlier (2026-11-09, simplified rules):
- `trace_user` on one synthetic user: own ETH supplied (`supply` 10),
  borrowed USDC re-supplied (no contribution), own ETH sent with traced USDC
  at `f = 1` and `f = 0.5` (`complement` 2 and 0.5). Checked by hand.
- `trace_chunk` on the first 3.0M rows of `transfers_wallets.parquet` and
  their events, saved locally: 17.2% of the rows kept, no negative balance
  (apart from the numpy overflow artifact above, which only existed in the
  analysis code).
- `main()` end to end on a local Parquet file of 300 real users, with
  `CHUNK_ROWS = 937` to force carry-overs: the output was identical to running
  `trace_user` on each user directly. To do this, monkeypatch
  `trace_wallets.s3fs.S3FileSystem = lambda: fsspec.filesystem("file")` and
  the five path constants. The original portfolio rows matched too.

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
