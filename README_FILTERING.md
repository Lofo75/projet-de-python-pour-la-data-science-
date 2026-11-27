# Filtering the transfers of Aave V3 users

This document explains how we built the dataset of token flows that can be
traced back to Aave, and why each choice was made. The question behind it:
**once money leaves Aave (a borrow, a withdrawal…), where does it go?** To
answer it, we keep, for every Aave V3 user, only the transfers that were
made possible by an earlier interaction with Aave.

All data is on the Onyxia S3 bucket `s3://bastienlevyguinot/`. Each step is
one Python script, listed in [Reproducing](#8-reproducing) at the end.

> **Provisional version.** The selection of users (section 2) has been
> rewritten as a single script, `preprocess_events_and_transfers.py`, which
> has been run. The tracing (sections 3 to 6) has not been run again since:
> its results come from the previous version of the selection, which
> applied the same rules in several scripts and kept 175,453 wallets
> instead of 175,446. Section 8 says which files each step reads.

## 1. Raw data

| Data | Source | Size |
|---|---|---|
| Aave V3 Pool events: Supply, Withdraw, Borrow, Repay, LiquidationCall | RPC node (`eth_getLogs`) on the Aave V3 Ethereum main market | 3,919,908 events, 207,780 distinct users |
| ERC-20 transfers sent or received by each user | Etherscan `tokentx` | 50,824,039 transfers, 48,083,055 after section 2 |
| Native ETH flows of each user | Etherscan `txlist` (the user's transactions) and `txlistinternal` (ETH paid by contracts) | 36,356,546 rows, 14,340,218 of them actually moving ETH |

**Period.** From the first Aave V3 event (block 16,496,817, 2023-01-27) to
the last event fetched (block 25,218,786, 2026-05-31). The transfers were
fetched up to 2026-10-28, but everything after the last event is dropped:
after that date we could not tell Aave transactions apart, so the filter
would have been wrong there.

**One row per user and transfer.** The transfers are fetched per user. A
transfer between two users of the dataset therefore appears twice, once in
each user's history, and each user is analysed separately.

## 2. Which users are kept: wallets only

An Aave "user" is any address that appears in the `user` field of an event.
Some of them are not people: token contracts, protocols, bots. Their
transfer histories would swamp the data (a token contract "sees" every
transfer of its token), and their flows are not decisions of a holder.
We keep **wallets**, i.e. addresses controlled by someone. This is done by
`preprocess_events_and_transfers.py`.

**A user is complete or absent, never partial.** The fetch stops at ~20,000
transfers per address (20 calls of 1,000), so that bots with millions of
transfers do not block it. An address still cut after that, or with more
than 1,000 transfers in a single block, is not saved at all and is listed
in `diffusion/skipped_users_during_transfers_fetching.parquet`. Tracing
needs the whole history of a user: a partial one would be wrong from the
first missing transfer.

Addresses are lowercased in both tables (Ethereum addresses are
case-insensitive, but the APIs return mixed-case checksums), then the rules
are applied in this order. An address can match several rules: its reasons
are joined with `+`.

| # | Reason | Rule | How it is detected |
|---|---|---|---|
| 1 | `skipped_fetch` | history not saved by the fetch | listed by the fetch (see above) |
| 1 | `burn` | burn address | fixed list (null address, `0x…dead`, precompiles) |
| 1 | `reserve` | Aave reserve | appears as a reserve or collateral in the events |
| 1 | `token` | ERC-20 token contract | appears as a token address in any transfer |
| 2 | `no_transfers` | no transfer at all | no row in the fetched transfers |
| 2 | `event_after_last_transfer` | inconsistent history | an event is later than the user's last transfer |
| 3 | `contract` | other smart contract (router, vault, bot…) | `eth_getCode` returns code that is not a wallet |

Kept as wallets: plain addresses with no code, addresses with EIP-7702
delegated code (still controlled by a private key) and Safe multisig
wallets (recognised by their `masterCopy()` function and small size). Other
smart-contract wallets cannot be told apart from protocols and are dropped
with them. The code types are cached in `aave_v3/code_types_parts/`.

| Reason | Addresses | Their transfers | Their events |
|---|---:|---:|---:|
| `contract` | 31,601 | 2,377,795 | 313,478 |
| `skipped_fetch` (18 of them also `token`, `reserve` or `burn`) | 379 | 0 | 1,065,065 |
| `token` | 353 | 363,189 | 58,542 |
| `burn` | 1 | 0 | 5 |
| **Total dropped** | **32,334** | **2,740,984** | **1,437,090** |

The rules of step 2 drop nobody: thanks to the complete-or-absent fetch, no
incomplete history is left. The 379 addresses skipped by the fetch are 0.2%
of the users but hold 27% of the events: very active bots.

**Result: 175,446 wallets**, with 2,482,818 events and 48,083,055 ERC-20
transfers. The dropped addresses and their reasons are in
`diffusion/skipped_addresses.parquet`.

*Previous version, used for the results of sections 3 to 6:* the fetch
saved cut histories, which were dropped afterwards (129 users), and the
same identity and code rules were applied in separate scripts. It kept
**175,453 wallets**, with 2,483,263 events and 46,916,198 ERC-20 transfers.

## 3. Linking transfers to Aave

A transfer is **`tracked`** when the user has an Aave event in the same
transaction (same transaction hash and same user). This marks the transfers
that *are* the Aave interaction: the borrowed USDC arriving, the collateral
leaving for the Pool, the aTokens minted…

Every event was also checked against the transfers it must produce (a
Supply mints aTokens to the user, a Borrow mints debt tokens…). After the
drops of section 2, every event passes these checks.

## 4. Tracing the flows

### The idea: a ledger of "traced" money per user

For each user we keep a **ledger**: for each token, the quantity in the
wallet that comes from Aave. We read the user's history in chronological
order, one transaction at a time, and give every transaction a
**traced fraction `f`** between 0 and 1: the share of it that was paid with
traced money. A transaction is kept in the output if `f > 0`.

### The rule

For each transaction we sum, per token, what the user sends (`out`) and what
the user receives (`in`). Then:

1. **Aave transaction** (`tracked`): `f = 1`.
2. **Any other transaction:** for each token the user sends that has a traced
   balance, compute `min(1, traced balance / amount sent)`. `f` is the
   smallest of these ratios ("weakest link"). Tokens sent with no traced
   balance are ignored. If no traced token is sent, `f = 0`.

When `f > 0`, the ledger is updated: every token sent is debited (never
below zero), and every token received is credited with `f × amount`. When
`f = 0`, the ledger does not change. All rows of a transaction get the same
`f`.

**Example.** Alice borrows 1,000 USDC on Aave (`f = 1`: her ledger now holds
1,000 traced USDC). She swaps 1,500 USDC for 0.5 ETH: only 1,000 of the
1,500 USDC are traced, so `f = 2/3`, her USDC ledger falls to 0 and she
gets 0.33 traced ETH. She then receives 2,000 USDC from an exchange: `f = 0`,
not kept, the ledger does not change.

### The choices and why

**Money that simply arrives is never traced.** A transaction where the user
only receives (an exchange withdrawal, a salary, an airdrop, spam) gets
`f = 0`. A first version credited every incoming transfer in full, as the
script we started from did. That made sense there, because its input only
held borrow-related flows. On whole wallet histories, however, it kept
**80% of all rows** (measured on a sample of 3.0M rows), mostly USDC and
USDT coming from outside, and everything later paid with them. As a
consequence, a user's ledger is empty until their first Aave transaction.

**Balances never go negative.** The first version debited the ledger even
below zero. A user who supplied their own USDC to Aave then had a negative
USDC balance, and a later USDC borrow just "filled the hole". Once incoming
money was no longer credited, this hid 18% of the borrows of the sample
entirely: the borrowed money was never traced. Balances are now floored at
zero.

**Weakest link, ignoring untraced tokens.** When a user sends borrowed USDC
together with their own ETH (to provide liquidity, for instance), `f` comes
from the USDC alone. The own ETH spent is recorded separately (section 5).

**Zero-value transfers do not count.** Scammers send fake transfers that
appear in the victim's history ("address poisoning"): 0 real USDC "from" the
victim, together with a fake token named like a real one (`UЅDТ`, with a
Cyrillic letter). Counting the 0-USDC send as fully covered gave `f = 1` and
kept these transactions. A send of 0 moves nothing, so it is now ignored.

**Debt tokens are tokens like the others.** Aave represents a loan with a
debt token minted to the borrower and burned on repayment. It is traced
like any token, but it is never counted as "own funds" (section 5): burning
it repays a loan, it does not spend anything.

**Native ETH is included, merged with WETH.** Token transfers do not include
native ETH, yet Aave's ETH gateway pays borrows and withdrawals in native
ETH, and many swaps pay out ETH. Without ETH, the borrowed ETH and the
proceeds of those swaps were invisible. ETH rows are added to each user's
history under the WETH address, so ETH and WETH share one ledger entry.
Wrapping and unwrapping (ETH ↔ WETH, 223,657 rows) are dropped: they change
nothing within one entry, and WETH emits no token transfer for them.
Failed transactions and zero-value calls are ignored. 15 users whose ETH
history was cut by the fetch (bots with ~20,000 transactions) are left out.

**Tiny fractions are rounding errors.** Amounts are floating-point numbers,
so a balance meant to be zero can stay at 1e-17. Fractions below 1e-6 are
treated as 0.

**One fraction per transaction.** The original script gave each row its own
fraction. Within one transaction, all rows now share `f`: simpler, and the
difference was marginal.

## 5. The original portfolio

A second dataset records **the user's own money that entered the traced
flows**, so that the exposure built with Aave money can be compared with
what the user brought in. Each row is a contribution:

- **`supply`**: an asset supplied to Aave, from the Supply events. All
  users are treated as supplying their own collateral, even when someone
  else supplied on their behalf, and even if we never saw the tokens arrive
  in the wallet. The part paid with already-traced tokens (for example
  borrowed USDC supplied back, as in leverage loops) is taken out of the
  ledger instead, so it is not counted twice.
- **`complement`**: untraced tokens sent together with traced ones, counted
  for `f × amount`. For example, own ETH added to borrowed USDC in a
  liquidity deposit. aTokens count as assets; debt tokens never do.

Summing the contributions by user and token gives the portfolio; adding
them up over time gives its history.

## 6. Results

| | Rows in | Rows kept | Share |
|---|---|---|---|
| ERC-20 transfers | 40,367,850 | 8,159,370 | 20.2% |
| Native ETH flows | 10,993,125 | 1,091,300 | 9.9% |
| **Total** | **51,360,975** | **9,250,670** | **18.0%** |

(Rows in: after the cut at the last event and without the 15 skipped users.)

- 5,086,571 of the kept rows belong to Aave transactions; 87.6% of the kept
  rows have `f = 1`.
- The ledgers hold 972,808 (user, token) entries, none negative.
- The original portfolios hold 816,581 contributions (739,573 `supply`,
  77,008 `complement`) for 172,733 users.
- Portfolio states (one per transaction that changes the portfolio), per
  user: median 7 in the traced transfers and 2 in the original portfolios.

## 7. Limitations

- **Money that goes through another wallet is lost.** Each user is traced
  separately: if Alice borrows and sends the money to Bob, Bob's copy of the
  transfer is a plain receive for him and is not traced.
- **Money that leaves and comes back later is lost.** Sending tokens to an
  exchange or a bridge and getting them back in another transaction looks
  like a new arrival.
- **Some deposits give nothing visible in return.** Uniswap V3 positions are
  NFTs, which are not fetched; bridges send the money to another chain. The
  tokens sent there simply leave the ledger.
- **Interest is not seen.** aTokens grow without any transfer, so traced
  aToken balances understate what the user holds.
- **Order within a block.** The order of a user's transactions inside one
  block is not known exactly (Etherscan's order is used, ERC-20 before ETH).
- **Debt tokens are recognised by their name** (`variableDebt…`,
  `stableDebt…`), the naming of Aave and its forks.
- **Small own-ETH complements.** About half of the ETH complements are tiny
  amounts (median 0.0001 ETH on the sample), most likely fees paid to
  bridges or protocols rather than exposure. They are kept.
- **Only the Aave V3 Ethereum main market** is covered: other markets and
  forks (Spark…) are not Aave events here.

## 8. Reproducing

Run in this order:

| Step | Script | Main output |
|---|---|---|
| Aave V3 events | `fetch_events_web3.py` | `diffusion/aave_v3_events.parquet` |
| ERC-20 transfers per user | `fetch_transfers_etherscan.py` | `aave_v3/transfers_parts/`, `diffusion/transfers.parquet` |
| Native ETH per user | `fetch_eth_etherscan.py` | `diffusion/eth_transfers_wallets.parquet` |
| Keep wallets (section 2) | `preprocess_events_and_transfers.py` | `aave_v3/transfers_parts_preprocessed/`, `diffusion/transfers_preprocessed.parquet`, `diffusion/aave_v3_events_preprocessed.parquet` |
| `tracked` column, event checks (section 3) | `match_events_transfers.py` | `diffusion/transfers_wallets.parquet`, `aave_v3/events_transfers_checks.parquet` |
| **Tracing** (sections 4-5) | **`trace_wallets.py`** | see below |

**Files not yet connected.** `match_events_transfers.py` and
`trace_wallets.py` still read the files of the previous version
(`aave_v3/transfers_parts_wallets/`,
`diffusion/aave_v3_events_wallets.parquet`), not the `*_preprocessed`
outputs. To run them on the current selection, point their input
constants to those outputs (`trace_wallets.py` needs the `tracked` column
added by `match_events_transfers.py`). Likewise, the ETH file and
`aave_v3/eth_status_parts/` (read by `trace_wallets.py` to leave out the
users whose ETH history was cut) come from the previous version of
`fetch_eth_etherscan.py`.

Versions of these scripts runnable in Google Colab are in `notebook_copies/`.

Outputs of `trace_wallets.py`:

- `diffusion/transfers_wallets_traced.parquet`: the kept rows, with the
  columns of the transfers (`user, block_number, timestamp, tx_hash, from,
  to, token_address, token_symbol, token_name, token_decimal, value,
  tracked`) plus `traced_fraction`. Native ETH rows have the WETH address and
  the symbol `ETH`. `value` is the raw integer as a string: divide by
  `10**token_decimal`.
- `aave_v3/filtering_ledgers.parquet`: each user's final ledger
  (`user, token_address, token_symbol, final_quantity`).
- `diffusion/original_portfolios.parquet`: the contributions
  (`user, block_number, timestamp, tx_hash, token_address, token_symbol,
  quantity, source`).

Etherscan keys are read from `.env` (`ETHERSCAN_API_KEY`,
`ETHERSCAN_API_KEY_2`…) or from the `ETHERSCAN_KEY` environment variable;
S3 access from the `AWS_*` variables that Onyxia provides.
