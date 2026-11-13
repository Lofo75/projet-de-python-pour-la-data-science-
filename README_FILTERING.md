# Filtering the transfers of Aave V3 users

This document explains how we built the dataset of token flows that can be
traced back to Aave, and why each choice was made. The question behind it:
**once money leaves Aave (a borrow, a withdrawal…), where does it go?** To
answer it, we keep, for every Aave V3 user, only the transfers that were
made possible by an earlier interaction with Aave.

All data is on the Onyxia S3 bucket `s3://bastienlevyguinot/`. Each step is
one Python script, listed in [Reproducing](#8-reproducing) at the end.

## 1. Raw data

| Data | Source | Size |
|---|---|---|
| Aave V3 Pool events: Supply, Withdraw, Borrow, Repay, LiquidationCall | RPC node (`eth_getLogs`) on the Aave V3 Ethereum main market | 3,919,908 events, 207,780 distinct users |
| ERC-20 transfers sent or received by each user | Etherscan `tokentx` | 46,916,198 transfers (after section 2) |
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
We keep **wallets**, i.e. addresses controlled by someone.

| Reason for dropping an address | How it is detected | Addresses |
|---|---|---|
| Aave reserve or ERC-20 token contract | appears as a reserve in the events, or as a token address in any transfer | 370 |
| Burn address | fixed list | 1 |
| Other smart contract (router, vault, bot…) | `eth_getCode` returns code that is not a wallet | 31,826 |
| History cut by the fetch, with Aave activity after the cut | an event happens after the last transfer fetched | 83 |
| History cut by the fetch, without activity after the cut | one extra Etherscan call finds transfers after the last one saved | 46 |

Kept as wallets: plain addresses with no code, addresses with EIP-7702
delegated code (still controlled by a private key) and Safe multisig
wallets. Other smart-contract wallets cannot be told apart from protocols and
are dropped with them.

The fetch keeps at most ~20,000 transfers per address, so that bots with
millions of transfers do not block it. A user whose history was cut is
dropped entirely, rather than kept with a partial history.

**Result: 175,453 wallets**, with 2,483,263 events and 46,916,198 ERC-20
transfers. The list of dropped addresses and their reasons is in
`aave_v3/dropped_addresses.csv`.

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

Run in this order (each script's docstring gives its details):

| Step | Script | Main output |
|---|---|---|
| Aave V3 events | `fetch_events_web3.py` | `aave_v3/aave_v3_events.parquet` |
| ERC-20 transfers per user | `fetch_transfers_etherscan.py` | `aave_v3/transfers_parts/` |
| Completeness checks | `transfers_fetching_checks.py`, `transfers_fetching_marginal_checks.py` | printed |
| Drop reserves, tokens, burn addresses | `drop_non_wallets.py` | `aave_v3/transfers_parts_wallets/` |
| Drop contracts | `transfers_drop_contracts.py` | `diffusion/transfers_wallets.parquet` |
| Events of wallets | `preprocess_events.py` | `diffusion/aave_v3_events_wallets.parquet` |
| `tracked` column, event checks | `match_events_transfers.py` | `diffusion/transfers_wallets.parquet` |
| Drop cut histories | `drop_truncated.py`, `transfers_drop_incomplete.py` | all files above |
| Native ETH per user | `fetch_eth_etherscan.py` | `diffusion/eth_transfers_wallets.parquet` |
| **Tracing** | **`trace_wallets.py`** | see below |

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
