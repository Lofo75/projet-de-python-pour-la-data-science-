"""Recompute traced_fraction for out-unique/by_user using a single,
per-user, chronologically-ordered portfolio ledger.

## Why this replaces the earlier per-borrow ledger design

The original approach (v1) computed one independent ledger per borrow,
seeded only by that borrow's own borrow_amount. That broke down for
overlapping borrows of the same asset (e.g. a leverage loop: borrow ->
supply as collateral -> borrow more -> supply more -> ...), since the
borrowed asset is genuinely fungible once it lands in the wallet -- there
is no way to tell "whose" tokens get spent first. See
fix_disbursement_attribution.py for the concrete example (5 overlapping
USDe borrows) that exposed this: a later borrow's own supply-back leg
showed up as 0% traced simply because its isolated ledger never knew
about a sibling borrow's contribution to the same pooled aToken balance.

v2 abandons per-borrow attribution entirely and instead tracks one
pooled dict per user: asset_key -> currently-traced quantity (which can
go negative -- see case 4 below). Every disbursement across every borrow
simply adds to its asset's entry; nothing needs to track which specific
borrow "owns" a given unit of the pooled balance.

## Algorithm

Process one user's out-unique/by_user/<address>.csv file in a single
chronological pass across ALL of that user's borrows together (not
grouped per borrow -- out-unique is already deduplicated to one row per
transfer_unique_ID, so every transaction appears exactly once in the
file). Group rows by on-chain transaction (transfer_unique_ID's tx-hash
prefix), process transactions in ascending transfer_time order.

Within each transaction, rows split into two groups:

  FORCED rows (kept_reason in {disbursement, repayment}): these are
  objectively-known anchor events (reliable since
  fix_disbursement_attribution.py corrected their attribution), so they
  always get fraction = 1.0, regardless of ledger state. Every asset
  touched still updates the ledger (present assets deplete/credit
  normally, a first-ever asset identity still gets created), just with
  the transaction's combined fraction pinned to 1 instead of computed.

  NORMAL rows (everything else, i.e. traced_flow): fraction is computed
  from ledger state, following 5 cases based on the transaction's
  outgoing (from user) and incoming (to user) legs, keyed by asset_key:

    1. Pure receive (no outgoing legs at all): every received asset is
       credited in full (fraction = 1.0), whether or not it was already
       in the ledger.
    2. Pure send of one already-tracked asset (no incoming legs): the
       ledger is depleted by the amount sent, floored at 0. If the
       asset isn't in the ledger (or is already at exactly 0), the
       transaction is untraced (fraction = 0.0) and the ledger is left
       untouched -- this is really case 5 with a single asset.
    3. Send legs are ALL already-tracked assets, alongside a receive leg
       (e.g. a swap): the outflow's combined fraction -- the
       weakest-link min() across each sent asset's own bal/amt ratio --
       determines how much of what's received gets credited into the
       ledger. Each sent asset depletes by its own bal/amt ratio
       (mathematically identical to flooring it at 0).
    4. Send legs mix already-tracked and never-seen assets (e.g.
       borrowed USDT + already-held ETH, both spent providing Uniswap
       liquidity): combined fraction is computed from the PRESENT assets
       only (a missing asset never drags combined to 0 the way it would
       if it were included in the min()). Present assets deplete as in
       case 3. Each missing asset gets a NEW ledger entry, created at
       -(combined * amount_sent) -- i.e. it goes negative, recording
       "this much of an asset with no traced record was consumed as a
       complementary input to an otherwise-traced action". The receive
       leg is credited by combined * amount, same as case 3.
    5. Send legs are ALL never-seen (or already-zeroed) assets: nothing
       is traceable here. Every row in the transaction gets
       fraction = 0.0, and the ledger is left completely untouched (no
       entries created, unlike case 4, since there's no traced leg to
       anchor a deficit).

  ETH and WETH share one asset_key (same as split_by_user.py) --
  wrapping/unwrapping nets out to no real ledger change rather than
  being treated as a swap between two assets.

## Outputs

  1. out-unique/by_user/*.csv: traced_fraction recomputed in place
     (annotation only, same convention as v1).
  2. out-unique-filtered/by_user/*.csv: full copy with traced_fraction
     == 0 rows removed.
  3. filtering_ledgers/<shard>/<address>.csv: the FINAL state of each
     user's portfolio dict once every row has been processed -- one row
     per asset_key ever touched, including zeroed-out and negative
     entries. A new, standalone diagnostic artifact (sharded the same
     way as every other by_user output in this repo).
"""

import argparse
import csv
import os
from collections import defaultdict

INPUT_DIR = "out-unique/by_user"
FILTERED_OUTPUT_DIR = "out-unique-filtered/by_user"
LEDGERS_OUTPUT_DIR = "filtering_ledgers"

FORCED_REASONS = {"disbursement", "repayment"}


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def asset_key(asset, asset_id):
    if asset in ("ETH", "WETH"):
        return "ETH/WETH"
    if isinstance(asset_id, str) and asset_id:
        return asset_id
    return asset


def tx_hash_of(row):
    return row["transfer_unique_ID"].split(":", 1)[0]


def leg_totals(rows):
    out_by_asset = defaultdict(float)
    in_by_asset = defaultdict(float)
    for r in rows:
        ak = asset_key(r["transfer_asset"], r["transfer_asset_ID"])
        amt = safe_float(r["value"])
        if r["transfer_from_label"] == "user":
            out_by_asset[ak] += amt
        if r["transfer_to_label"] == "user":
            in_by_asset[ak] += amt
    return out_by_asset, in_by_asset


def process_forced(rows, ledger, fractions):
    out_by_asset, in_by_asset = leg_totals(rows)

    for ak, amt in out_by_asset.items():
        ledger[ak] = ledger.get(ak, 0.0) - amt
    for ak, amt in in_by_asset.items():
        ledger[ak] = ledger.get(ak, 0.0) + amt

    for r in rows:
        fractions[id(r)] = 1.0


def process_normal(rows, ledger, fractions):
    out_by_asset, in_by_asset = leg_totals(rows)

    if not out_by_asset:
        # case 1: pure receive
        for ak, amt in in_by_asset.items():
            ledger[ak] = ledger.get(ak, 0.0) + amt
        for r in rows:
            fractions[id(r)] = 1.0
        return

    present_out = {ak: amt for ak, amt in out_by_asset.items() if ledger.get(ak, 0.0) > 0}
    missing_out = {ak: amt for ak, amt in out_by_asset.items() if ak not in present_out}

    if not present_out:
        # case 5: nothing traceable, ledger untouched
        for r in rows:
            fractions[id(r)] = 0.0
        return

    # cases 2/3/4
    out_fracs = {ak: min(1.0, ledger[ak] / amt) for ak, amt in present_out.items()}
    combined = min(out_fracs.values())

    for ak, amt in present_out.items():
        ledger[ak] = max(0.0, ledger[ak] - amt)
    for ak, amt in missing_out.items():
        ledger[ak] = ledger.get(ak, 0.0) - combined * amt
    if combined > 0:
        for ak, amt in in_by_asset.items():
            ledger[ak] = ledger.get(ak, 0.0) + combined * amt

    for r in rows:
        ak = asset_key(r["transfer_asset"], r["transfer_asset_ID"])
        if r["transfer_from_label"] == "user" and ak in out_fracs:
            fractions[id(r)] = out_fracs[ak]
        else:
            fractions[id(r)] = combined


def compute_fractions_for_user(rows):
    """rows: every row of one user's out-unique CSV. Returns
    (dict id(row) -> traced_fraction, final ledger dict)."""
    fractions = {}
    ledger = {}

    by_tx = defaultdict(list)
    for r in rows:
        if not r["transfer_unique_ID"]:
            continue
        by_tx[tx_hash_of(r)].append(r)
    tx_order = sorted(by_tx.keys(), key=lambda tx: min(r["transfer_time"] for r in by_tx[tx]))

    for tx in tx_order:
        tx_rows = by_tx[tx]
        forced_rows = [r for r in tx_rows if r["kept_reason"] in FORCED_REASONS]
        normal_rows = [r for r in tx_rows if r["kept_reason"] not in FORCED_REASONS]
        if forced_rows:
            process_forced(forced_rows, ledger, fractions)
        if normal_rows:
            process_normal(normal_rows, ledger, fractions)

    return fractions, ledger


def process_file(src_path, filtered_dst_path, ledger_dst_path):
    with open(src_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    fractions, ledger = compute_fractions_for_user(rows)
    for r in rows:
        r["traced_fraction"] = f"{fractions.get(id(r), 0.0):.6f}"

    out_fieldnames = fieldnames if "traced_fraction" in fieldnames else fieldnames + ["traced_fraction"]

    tmp_path = src_path + ".tmp"
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, src_path)

    os.makedirs(os.path.dirname(filtered_dst_path), exist_ok=True)
    with open(filtered_dst_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(r for r in rows if float(r["traced_fraction"]) > 0.0)

    os.makedirs(os.path.dirname(ledger_dst_path), exist_ok=True)
    with open(ledger_dst_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["asset_key", "final_quantity"])
        for ak, qty in sorted(ledger.items()):
            writer.writerow([ak, f"{qty:.6f}"])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", default=INPUT_DIR)
    parser.add_argument("--filtered-output-dir", default=FILTERED_OUTPUT_DIR)
    parser.add_argument("--ledgers-output-dir", default=LEDGERS_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="only process the first N files (dry run)")
    parser.add_argument("--progress-every", type=int, default=5000)
    args = parser.parse_args()

    n_files = 0
    for shard in sorted(os.listdir(args.input_dir)):
        shard_dir = os.path.join(args.input_dir, shard)
        if not os.path.isdir(shard_dir):
            continue
        for fname in sorted(os.listdir(shard_dir)):
            if not fname.endswith(".csv"):
                continue
            if args.limit is not None and n_files >= args.limit:
                print(f"Reached --limit {args.limit}, stopping.")
                print(f"Processed {n_files} files")
                return
            src_path = os.path.join(shard_dir, fname)
            filtered_dst_path = os.path.join(args.filtered_output_dir, shard, fname)
            ledger_dst_path = os.path.join(args.ledgers_output_dir, shard, fname)
            process_file(src_path, filtered_dst_path, ledger_dst_path)
            n_files += 1
            if n_files % args.progress_every == 0:
                print(f"  {n_files} files processed...")

    print(f"Done. {n_files} files processed")


if __name__ == "__main__":
    main()
