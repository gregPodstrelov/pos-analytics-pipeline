#!/usr/bin/env python3
"""
Load real RORC TLOG files into the analytics database.

This replaces the synthetic generator. Point it at a folder of TLOG_*.DAT
files and it parses every transaction, joins product names and costs from
item_master.csv, and writes the same schema the MCP server already queries.

    python3 make_sample_catalogue.py
    python3 load_real_tlogs.py ~/Downloads/archive

Options:
    --since 2025-01-01     only load files dated on or after this
    --db demo_pos.db       output database

Notes on this export's configuration
------------------------------------
The files carry 36 fields: the 29 fixed ones plus TransactionCode, UnitCost,
UnitDeal, CustomerInfo, StoreCode, PointsEarned and PointsRedeemed. That means
APPENDTRNCD, APPENDCOSTDEAL, APPENDCUSTID, APPENDSTORENUM and APPENDLYLPOINTS
are switched on, but APPENDDESCRIPTIONS is not - so the log has barcodes and no
product names. Names, departments, categories and costs are joined in from the
price book, which is exactly what the Glue job will do in production.

UnitCost is written as 0 throughout, so margin comes from the price book too.
Turning on APPENDCOSTDEAL properly would make margin exact.
"""

import os
import re
import csv
import sys
import glob
import sqlite3
import argparse
import collections
from datetime import datetime

import tlog_format as tf

ITEM_MASTER = "item_master.csv"
DEFAULT_DB  = "demo_pos.db"

# The field layout, taken from the shared module rather than restated here.
#
# This used to be its own copy of the dict, and it drifted from the writer's:
# the reader expected CustomerInfo and loyalty points, the writer emitted
# descriptions instead. Same field count either way, so nothing errored - the
# store code just arrived holding a product description. Importing the one
# definition removes the possibility.
CONFIG = tf.DEFAULT_CONFIG

# Store codes seen in the logs, mapped to readable names. Extend as more
# locations arrive - unknown codes fall back to "Store <code>".
STORE_NAMES = {
    "0000000001": "Northline Riverside",
    "0000000003": "Northline Store 3",
}

SCHEMA = """
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_item;
DROP TABLE IF EXISTS dim_department;

CREATE TABLE dim_store (
    store_code TEXT PRIMARY KEY,
    store_name TEXT,
    region     TEXT
);

CREATE TABLE dim_department (
    department_code TEXT PRIMARY KEY,
    department_name TEXT
);

CREATE TABLE dim_item (
    item_id         TEXT PRIMARY KEY,
    item_desc       TEXT,
    product_key     TEXT,
    department_code TEXT,
    department_name TEXT,
    category_name   TEXT,
    vendor          TEXT,
    unit_price      REAL,
    unit_cost       REAL,
    margin_pct      REAL
);

CREATE TABLE fact_transactions (
    transaction_id   TEXT,
    store_id         TEXT,
    store_name       TEXT,
    transaction_date TEXT,
    terminal_code    TEXT,
    cashier_id       TEXT,
    sequence         INTEGER,
    key_function     TEXT,
    item_id          TEXT,
    item_desc        TEXT,
    product_key      TEXT,
    department_code  TEXT,
    department_name  TEXT,
    category_name    TEXT,
    transaction_type TEXT,
    transaction_type_name TEXT,
    is_loss_event    INTEGER,
    quantity         REAL,
    unit_price       REAL,
    unit_cost        REAL,
    extended_price   REAL,
    gross_margin     REAL,
    retail_type      TEXT,
    attribute_flag   TEXT,
    is_taxable       INTEGER,
    is_food_stamp    INTEGER,
    is_wic           INTEGER,
    loyalty_discount REAL,
    premium_discount REAL,
    loyalty_code     TEXT,
    tender_amount    REAL,
    tender_type      TEXT
);

CREATE INDEX idx_date  ON fact_transactions(transaction_date);
CREATE INDEX idx_store ON fact_transactions(store_id);
CREATE INDEX idx_item  ON fact_transactions(item_id);
CREATE INDEX idx_dept  ON fact_transactions(department_code);
CREATE INDEX idx_type  ON fact_transactions(transaction_type);
"""

INSERT_SQL = "INSERT INTO fact_transactions VALUES (" + ",".join("?" * 32) + ")"

TYPE_NAMES = {"SALE": "Sale", "VOID": "Void", "RETURN": "Return",
              "TENDER": "Tender", "COUPON": "Coupon"}


def load_item_master(path=ITEM_MASTER):
    """
    Index the price book by barcode. Padding differs between the price book
    and the transaction log, so every item is indexed both padded and stripped.
    """
    if not os.path.exists(path):
        print(f"Missing {path} - run build_item_master.py first.")
        sys.exit(1)

    by_id, rows = {}, []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
            by_id[r["item_id"]] = r
            by_id[r["item_id"].lstrip("0")] = r
    return by_id, rows


def date_from_filename(name):
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", name)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
    except ValueError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", nargs="?", default="archive",
                    help="folder containing TLOG_*.DAT files")
    ap.add_argument("--since", help="only load files dated on/after YYYY-MM-DD")
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()

    if not os.path.isdir(args.archive):
        print(f"Not a folder: {args.archive}")
        sys.exit(1)

    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").date()

    # The POS archive ships .DAT; the demo generator writes .txt. Same format
    # either way, so accept both rather than making the documented demo path
    # depend on which one you happen to have.
    files = sorted(set(
        f for ext in ("DAT", "dat", "TXT", "txt")
        for f in glob.glob(os.path.join(args.archive, f"*.{ext}"))))
    if since:
        files = [f for f in files
                 if (date_from_filename(os.path.basename(f)) or since) >= since]
    files = [f for f in files if os.path.getsize(f) > 0]

    if not files:
        print(f"No non-empty TLOG files found in {args.archive}")
        sys.exit(1)

    print(f"Loading real TLOG data from {args.archive}")
    print(f"  files: {len(files):,}")

    by_id, master_rows = load_item_master()
    print(f"  catalogue: {len(master_rows):,} items")

    dept_names = {}
    for r in master_rows:
        if r["department_code"]:
            dept_names[r["department_code"]] = r["department_name"]

    # Start from a clean file. Bulk loading runs with journalling off for
    # speed, which means an interrupted run leaves the database unrecoverable -
    # so it is replaced rather than reopened.
    for leftover in (args.db, args.db + "-journal", args.db + "-wal",
                     args.db + "-shm"):
        if os.path.exists(leftover):
            os.remove(leftover)

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -60000")
    conn.executescript(SCHEMA)

    conn.executemany("INSERT INTO dim_item VALUES (?,?,?,?,?,?,?,?,?,?)",
                     [(r["item_id"], r["item_desc"], r["product_key"],
                       r["department_code"], r["department_name"],
                       r["category_name"], r.get("vendor", ""),
                       float(r["unit_price"] or 0), float(r["unit_cost"] or 0),
                       float(r["margin_pct"] or 0)) for r in master_rows])

    batch, total_rows, unmatched = [], 0, collections.Counter()
    stores_seen, min_d, max_d = set(), None, None
    bad_lines = 0

    def flush_transaction(group):
        """
        Turn one transaction's lines into warehouse rows.

        Payment method lives on the tender line, but the useful question is
        "what do EBT shoppers buy", so the basket's tender is stamped onto
        every item line before the tender lines are dropped.
        """
        nonlocal total_rows, bad_lines, min_d, max_d
        if not group:
            return

        try:
            parsed, _fin = tf.parse_transaction_lines(group, CONFIG)
        except Exception:
            bad_lines += len(group)
            return

        tender = next((r["tender_type"] for r in parsed
                       if r["transaction_type"] == "TENDER" and r["tender_type"]),
                      "")

        for r in parsed:
            if r["transaction_type"] == "TENDER":
                continue

            store = r["store_code"] or "0000000000"
            stores_seen.add(store)
            d = r["transaction_date"].date()
            min_d = d if min_d is None or d < min_d else min_d
            max_d = d if max_d is None or d > max_d else max_d

            # Join the catalogue for name, department, category and cost
            item = by_id.get(r["item_id"]) or \
                by_id.get(r["item_id"].lstrip("0"))

            kf_upper = (r["key_function"] or "").upper()

            if item:
                desc = item["item_desc"]
                pkey = item["product_key"]
                dept = item["department_code"] or r["department_code"]
                dname = item["department_name"]
                cname = item["category_name"]
                cost = float(item["unit_cost"] or 0)

            elif kf_upper.startswith("OPEN DEPT TOL"):
                # Offer / discount departments. Per the spec these are the
                # promotional lines, written with a negative amount - not a
                # product, so they should not surface as an unknown item.
                code = kf_upper.replace("OPEN DEPT", "").strip()
                desc = f"Promotional discount ({code})"
                pkey = "PROMOTIONAL DISCOUNT"
                dept = r["department_code"]
                dname = dept_names.get(dept, "Promotions")
                cname = "Promotional discount"
                cost = 0.0

            elif kf_upper.startswith("OPEN DEPT"):
                # Rung by department key rather than scanned. A real sale with
                # no barcode by design, so it gets the department's name.
                code = kf_upper.replace("OPEN DEPT", "").strip() \
                    or r["department_code"]
                dept = r["department_code"] or code
                dname = dept_names.get(dept, f"Dept {dept}")
                desc = f"{dname} - open department key"
                pkey = f"OPEN DEPT {dept}"
                cname = "Open department"
                cost = 0.0

            else:
                unmatched[r["item_id"]] += 1
                desc = f"Unknown barcode {r['item_id']}"
                pkey = desc.upper()
                dept = r["department_code"]
                dname = dept_names.get(dept, f"Dept {dept}")
                cname = "Unmatched"
                cost = 0.0

            # The log writes UnitCost as 0, so margin comes from the price
            # book. Voids and returns carry negative quantity.
            qty = r["quantity"]
            ext = r["extended_price"]
            margin = round(ext - (cost * qty), 2) if cost else 0.0

            batch.append((
                r["transaction_code"], store,
                STORE_NAMES.get(store, f"Store {store.lstrip('0') or store}"),
                r["transaction_date"].strftime("%Y-%m-%d %H:%M:%S"),
                r["terminal_code"], r["cashier_code"], r["sequence"],
                r["key_function"], r["item_id"], desc, pkey,
                dept, dname, cname,
                r["transaction_type"],
                TYPE_NAMES.get(r["transaction_type"], r["transaction_type"]),
                r["is_loss_event"], qty,
                r["unit_price"], cost, ext, margin,
                r["retail_type"], r["attribute_flag"],
                r["is_taxable"], r["is_food_stamp"], r["is_wic"],
                r["loyalty_discount"], r["premium_discount"],
                r["loyalty_code"], r["tender_amount"], tender,
            ))

            if len(batch) >= 50000:
                conn.executemany(INSERT_SQL, batch)
                total_rows += len(batch)
                batch.clear()

    # Lines arrive grouped by transaction, so the group is flushed whenever
    # the transaction code changes.
    for n, path in enumerate(files, 1):
        current_key, group = None, []
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = tf.parse_line(line, CONFIG)
                except Exception:
                    bad_lines += 1
                    continue

                key = (rec.get("TransactionCode") or "",
                       rec.get("TerminalCode") or "",
                       rec.get("Date") or "")
                if key != current_key:
                    flush_transaction(group)
                    group, current_key = [], key
                group.append(line)

        flush_transaction(group)

        if n % 50 == 0:
            conn.commit()
            print(f"  {n:,}/{len(files):,} files, {total_rows:,} rows...",
                  flush=True)

    if batch:
        conn.executemany(INSERT_SQL, batch)
        total_rows += len(batch)

    conn.executemany("INSERT OR REPLACE INTO dim_store VALUES (?,?,?)",
                     [(s, STORE_NAMES.get(s, f"Store {s.lstrip('0') or s}"), "")
                      for s in sorted(stores_seen)])
    conn.executemany("INSERT OR REPLACE INTO dim_department VALUES (?,?)",
                     sorted(dept_names.items()))
    conn.commit()

    # ---- summary ----
    cur = conn.cursor()
    rev = cur.execute("SELECT SUM(extended_price) FROM fact_transactions "
                      "WHERE transaction_type='SALE'").fetchone()[0] or 0
    mar = cur.execute("SELECT SUM(gross_margin) FROM fact_transactions "
                      "WHERE transaction_type='SALE'").fetchone()[0] or 0
    txns = cur.execute("SELECT COUNT(DISTINCT transaction_id) "
                       "FROM fact_transactions").fetchone()[0]
    items = cur.execute("SELECT COUNT(DISTINCT item_id) "
                        "FROM fact_transactions").fetchone()[0]
    matched = cur.execute("SELECT COUNT(*) FROM fact_transactions "
                          "WHERE category_name != 'Unmatched'").fetchone()[0]

    print()
    print("Done.")
    print(f"  line items:    {total_rows:,}")
    print(f"  transactions:  {txns:,}")
    print(f"  date range:    {min_d} to {max_d}")
    print(f"  stores:        {', '.join(sorted(stores_seen))}")
    print(f"  distinct SKUs: {items:,}")
    print(f"  net sales:     ${rev:,.2f}")
    if rev:
        print(f"  gross margin:  ${mar:,.2f}  ({mar / rev * 100:.1f}%) "
              f"- from price book costs")
    print(f"  catalogue match: {matched / total_rows * 100:.1f}% of lines")
    if unmatched:
        print(f"  unmatched barcodes: {len(unmatched):,} distinct "
              f"({sum(unmatched.values()):,} lines)")
        for upc, cnt in unmatched.most_common(5):
            print(f"    {upc}  {cnt:,} lines")
    if bad_lines:
        print(f"  unparseable lines: {bad_lines:,}")

    conn.close()


if __name__ == "__main__":
    main()
