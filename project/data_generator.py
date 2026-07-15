#!/usr/bin/env python3
"""
Synthetic daily ledger feed generator.

Mimics the daily file a bank's upstream source system drops for the ledger team:
a ZIP containing a journal-lines CSV plus a control (manifest) file with row
counts and control totals. Entries are double-entry balanced (sum of debits ==
sum of credits per journal), and realistic data-quality problems are injected
so the pipeline's dedup / quarantine / reconciliation stages have real work to
do — while the CLEANED data still passes the debits==credits DQ gate.

Usage:
    python data_generator.py --date 2026-07-14 --rows 5000 --out ./landing
    python data_generator.py --date 2026-07-14 --clean          # no injected issues

Output:
    landing/journal_lines_<YYYYMMDD>.zip
        ├── journal_lines_<YYYYMMDD>.csv
        └── manifest_<YYYYMMDD>.json

Stdlib only — no dependencies.
"""

import argparse
import csv
import io
import json
import random
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

ACCOUNTS = [
    ("100100", "Cash and Equivalents", "ASSET"),
    ("110200", "Accounts Receivable", "ASSET"),
    ("120300", "Intercompany Receivable", "ASSET"),
    ("200100", "Accounts Payable", "LIABILITY"),
    ("210200", "Accrued Expenses", "LIABILITY"),
    ("300100", "Retained Earnings", "EQUITY"),
    ("400100", "Interest Income", "REVENUE"),
    ("400200", "Fee Income", "REVENUE"),
    ("500100", "Operating Expense", "EXPENSE"),
    ("500200", "FX Revaluation", "EXPENSE"),
]

COST_CENTERS = ["CC-GBM-01", "CC-WPB-02", "CC-CMB-03", "CC-OPS-04", "CC-TREAS-05"]
CURRENCIES = ["USD", "GBP", "INR", "HKD", "EUR"]
SOURCE_SYSTEMS = ["CORE_BANKING", "TREASURY", "CARDS", "PAYMENTS"]

HEADER = [
    "journal_id", "line_no", "business_date", "posting_ts", "account_id",
    "account_name", "cost_center", "currency", "debit_amount", "credit_amount",
    "description", "source_system",
]


def make_journal(rng: random.Random, journal_seq: int, business_date: date) -> list[list]:
    """One balanced journal entry: 1..3 debit lines mirrored by credit lines."""
    journal_id = f"JRN{business_date:%Y%m%d}{journal_seq:06d}"
    src = rng.choice(SOURCE_SYSTEMS)
    ccy = rng.choice(CURRENCIES)
    posting_ts = datetime.combine(business_date, datetime.min.time()) + timedelta(
        seconds=rng.randint(0, 86399)
    )
    n_pairs = rng.randint(1, 3)
    lines, line_no = [], 0
    for _ in range(n_pairs):
        amount = round(rng.uniform(10, 250_000), 2)
        debit_acct = rng.choice(ACCOUNTS)
        credit_acct = rng.choice([a for a in ACCOUNTS if a[0] != debit_acct[0]])
        for acct, debit, credit in ((debit_acct, amount, 0.0), (credit_acct, 0.0, amount)):
            line_no += 1
            lines.append([
                journal_id, line_no, business_date.isoformat(),
                posting_ts.strftime("%Y-%m-%d %H:%M:%S"),
                acct[0], acct[1], rng.choice(COST_CENTERS), ccy,
                f"{debit:.2f}", f"{credit:.2f}",
                f"{src.title().replace('_', ' ')} posting {journal_seq}", src,
            ])
    return lines


def inject_issues(rng: random.Random, rows: list[list], business_date: date) -> list[list]:
    """Realistic feed problems the pipeline must catch.

    - exact duplicate lines (upstream resend)            -> staging dedup
    - NULL cost_center                                   -> DQ null check
    - thousands separator in an amount ("12,345.67")     -> cleansing (REPLACE) before cast
    - one whole journal with unparseable amounts (#REF!) -> SAFE_CAST quarantine
    - late journals stamped with yesterday's date        -> late-data handling

    Every issue is injected so the CLEANED data still satisfies the ledger
    invariant (per-journal debits == credits): the separator is lossless, the
    bad journal is quarantined whole, and late journals move as complete units.
    That is what lets the DQ gate legitimately pass on a normal day.
    """
    # Exact duplicates: sample returns the same list objects, so later
    # mutations apply to both copies and the duplicates stay exact.
    dup_count = max(1, len(rows) // 100)
    rows.extend(rng.sample(rows, dup_count))

    for row in rng.sample(rows, max(1, len(rows) // 200)):
        row[6] = ""  # cost_center

    # Thousands separator sneaks into ONE row's own amount (lossless after
    # the pipeline strips commas, so the journal still balances).
    candidates = [r for r in rows if float(r[8]) >= 1000]
    comma_row = rng.choice(candidates)
    comma_row[8] = f"{float(comma_row[8]):,.2f}"

    # One WHOLE journal gets spreadsheet-corrupted amounts -> every line fails
    # SAFE_CAST and the journal is quarantined in full (a partially loaded
    # journal could never balance).
    bad_jid = rng.choice([r[0] for r in rows if r[0] != comma_row[0]])
    for row in rows:
        if row[0] == bad_jid:
            col = 8 if float(row[8] if row[8] != "#REF!" else 0) > 0 else 9
            row[col] = "#REF!"

    # Late data: a few COMPLETE journals in today's file carry yesterday's
    # business date (an adjustment posted after yesterday's cut-off).
    yesterday = (business_date - timedelta(days=1)).isoformat()
    late_jids = set(rng.sample(sorted({r[0] for r in rows if r[0] != bad_jid}), 3))
    for row in rows:
        if row[0] in late_jids:
            row[2] = yesterday

    rng.shuffle(rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat(), help="business date YYYY-MM-DD")
    parser.add_argument("--rows", type=int, default=5000, help="approx journal LINE count")
    parser.add_argument("--out", default="./landing", help="output directory (local landing zone)")
    parser.add_argument("--clean", action="store_true", help="skip data-quality issue injection")
    args = parser.parse_args()

    business_date = date.fromisoformat(args.date)
    # Seeded by date -> reruns for the same day produce the same file (idempotent upstream).
    rng = random.Random(business_date.toordinal())

    rows: list[list] = []
    seq = 0
    while len(rows) < args.rows:
        seq += 1
        rows.extend(make_journal(rng, seq, business_date))
    if not args.clean:
        rows = inject_issues(rng, rows, business_date)

    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(HEADER)
    writer.writerows(rows)
    csv_bytes = csv_buf.getvalue().encode()

    # Two kinds of control values, exactly like a real feed:
    #   * row_count  = PHYSICAL lines in the file (incl. duplicates) — the
    #     receiver uses it pre-load to detect a truncated transfer.
    #   * totals     = BUSINESS totals for this business date, from the
    #     sender's ledger (unique lines, commas ignored, unparseable = 0) —
    #     the receiver reconciles these AFTER dedup/quarantine; they should
    #     tie out exactly.
    unique = {(r[0], r[1]): r for r in rows}.values()
    on_date = [r for r in unique if r[2] == business_date.isoformat()]
    manifest = {
        "feed_name": "journal_lines",
        "business_date": business_date.isoformat(),
        "row_count": len(rows),
        "total_debit": f"{sum(_safe_float(r[8]) for r in on_date):.2f}",
        "total_credit": f"{sum(_safe_float(r[9]) for r in on_date):.2f}",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = f"{business_date:%Y%m%d}"
    zip_path = out_dir / f"journal_lines_{stamp}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"journal_lines_{stamp}.csv", csv_bytes)
        zf.writestr(f"manifest_{stamp}.json", json.dumps(manifest, indent=2))

    print(f"wrote {zip_path}  ({manifest['row_count']} lines, "
          f"debits {manifest['total_debit']} / credits {manifest['total_credit']})")


def _safe_float(value: str) -> float:
    try:
        return float(value.replace(",", ""))  # the sender knows "12,345.67" means 12345.67
    except ValueError:
        return 0.0


if __name__ == "__main__":
    main()
