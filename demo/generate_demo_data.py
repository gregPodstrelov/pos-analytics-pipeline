#!/usr/bin/env python3
"""
Generate POS transaction data in the RORC TLOG format, using the client's
real product catalogue.

Products, prices, costs, departments, categories, vendors, and WIC/food-stamp
flags all come from item_master.csv, which is built from their own RORC price
book export. Only the shopping behaviour is synthetic - what sold, when, and
in what basket.

Two outputs:

  1. tlog_files/   - real TLOG files for a recent sample window, one file per
                     transaction, named and formatted exactly as RORC writes
                     them. These are what the store upload agent ships to S3.

  2. demo_pos.db   - history built by running generated TLOG lines through the
                     SAME parser that will run in AWS Glue. Nothing is inserted
                     directly; every row came out of a parsed TLOG record.

Run:
    python3 make_sample_catalogue.py
    python3 generate_demo_data.py
"""

import os
import csv
import sys
import bisect
import sqlite3
import random
import shutil
from itertools import accumulate
from datetime import datetime, timedelta

import tlog_format as tf

random.seed(42)

DB_PATH      = "demo_pos.db"
TLOG_DIR     = "tlog_files"
ITEM_MASTER  = "item_master.csv"
SAMPLE_DAYS  = 2
TAX_RATE     = 0.0625          # Massachusetts
HISTORY_DAYS = int(os.environ.get("HISTORY_DAYS", "1130"))
# Baskets per store per day before store size, day-of-week, and seasonal
# multipliers. Set so that Riverside lands near the volume implied by the
# client's Item Movement Report for the Berry category.
BASE_BASKETS = int(os.environ.get("BASE_BASKETS", "280"))


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------
# STORE01 is the location the price book was exported from. The others are
# stand-ins so multi-store reporting has something to compare - replace them
# with the client's real locations once we have the list.

STORES = [
    ("STORE01", "Northline Riverside", "North Region", 1.35, 8),
    ("STORE02", "Northline Oakvale",     "North Region", 1.00, 6),
    ("STORE03", "Northline Fairhaven",  "Boston",     0.85, 5),
    ("STORE04", "Northline Westgate",     "South",      1.15, 7),
    ("STORE05", "Northline Summit",     "North",      0.70, 4),
]

TENDER_MEDIA = ["CASH", "CREDIT", "DEBIT", "FOODSTAMP", "CHECK"]

# Departments where sales tax applies in Massachusetts. Most grocery food is
# exempt; alcohol, non-food, and prepared items are not.
TAXABLE_DEPTS = {"10", "25", "12", "31", "32", "28", "40"}


# ---------------------------------------------------------------------------
# Load the client's catalogue
# ---------------------------------------------------------------------------

def load_items(path=ITEM_MASTER):
    if not os.path.exists(path):
        print(f"Missing {path}")
        print("Build it first:")
        print("  python3 make_sample_catalogue.py")
        sys.exit(1)

    items = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                price = float(r["unit_price"])
                cost  = float(r["unit_cost"])
                pop   = float(r["popularity"])
            except (ValueError, KeyError):
                continue
            if price <= 0 or pop <= 0:
                continue
            items.append({
                "item_id":   r["item_id"],
                "desc":      r["item_desc"],
                "product_key": r.get("product_key") or r["item_desc"].upper(),
                "pos_desc":  r["pos_desc"],
                "dept":      r["department_code"],
                "dept_name": r["department_name"],
                "cat":       r["category_code"],
                "cat_name":  r["category_name"],
                "price":     price,
                "cost":      cost,
                "fs":        r["food_stamp"] == "Y",
                "wic":       r["wic"] == "Y",
                "season":    r["season"],
                "pop":       pop,
                "observed":  float(r["observed_units_per_day"])
                             if r.get("observed_units_per_day") else 0.0,
            })
    return items


def pin_observed_items(items, store_multiplier, avg_items_per_basket,
                       avg_qty_per_line):
    """
    Replace modelled demand with measured demand where a movement report
    covers the item.

    An item's chance of being picked is its weight over the total weight, so
    to reproduce an observed rate of U units per day at the reporting store:

        U = daily_picks * (w / W) * avg_qty      =>      w = U * W / (daily_picks * avg_qty)

    Observed items are a tiny slice of a 65,000-item catalogue, so W is taken
    from the modelled items and the feedback from pinning is negligible.
    Seasonality is not applied on top - the measured rate already includes it.
    """
    obs = [it for it in items if it["observed"] > 0]
    if not obs:
        return 0

    w_modelled  = sum(it["pop"] for it in items if it["observed"] <= 0)
    daily_picks = BASE_BASKETS * store_multiplier * avg_items_per_basket

    for it in obs:
        it["pop"] = (it["observed"] * w_modelled) / (daily_picks * avg_qty_per_line)

    # One correction pass now that the observed weights are in the total
    w_total = w_modelled + sum(it["pop"] for it in obs)
    for it in obs:
        it["pop"] *= w_total / w_modelled

    return len(obs)


# ---------------------------------------------------------------------------
# Seasonality
# ---------------------------------------------------------------------------

SEASON_TABLE = {
    "summer":  {5:1.45, 6:1.95, 7:2.15, 8:1.85, 9:1.15, 4:0.75,
                10:0.55, 11:0.35, 12:0.30, 1:0.30, 2:0.35, 3:0.50},
    "winter":  {11:1.35, 12:1.75, 1:1.85, 2:1.65, 3:1.15, 10:0.90,
                4:0.75, 5:0.55, 6:0.35, 7:0.30, 8:0.30, 9:0.55},
    "fall":    {9:1.60, 10:2.20, 11:1.55, 8:0.55, 12:0.50, 1:0.30,
                2:0.30, 3:0.30, 4:0.30, 5:0.30, 6:0.30, 7:0.35},
    "spring":  {3:1.60, 4:2.00, 5:1.65, 6:0.95, 2:0.70, 7:0.45,
                8:0.35, 9:0.30, 10:0.30, 11:0.30, 12:0.30, 1:0.40},
    "holiday": {11:2.80, 12:3.10, 10:1.05, 1:0.35, 2:0.25, 3:0.25,
                4:0.30, 5:0.30, 6:0.25, 7:0.25, 8:0.25, 9:0.45},
    "flat":    {m: 1.0 for m in range(1, 13)},
}


def day_of_week_multiplier(d):
    return [0.85, 0.80, 0.85, 0.95, 1.25, 1.45, 1.30][d.weekday()]


def holiday_multiplier(d):
    if d.month == 12 and 18 <= d.day <= 31:
        return 2.1                      # Russian/Eastern European New Year peak
    if d.month == 11 and 20 <= d.day <= 27:
        return 1.9
    if d.month == 1 and 1 <= d.day <= 7:
        return 1.5
    if d.month == 4 and 10 <= d.day <= 20:
        return 1.4                      # Orthodox Easter window
    if (d.month, d.day) in [(7, 3), (7, 4)]:
        return 1.5
    return 1.0


def yoy_growth(d, start_year):
    return 1.0 + (0.065 * (d.year - start_year))


# ---------------------------------------------------------------------------
# Weighted sampling
# ---------------------------------------------------------------------------

class WeightedPicker:
    """
    Cumulative-weight sampler. The catalogue has tens of thousands of items,
    so weights are rebuilt once per simulated day rather than once per basket.

    Keeps a second, restricted pool of food-stamp-eligible items. SNAP cannot
    be used for alcohol, prepared food, or non-food goods, so an EBT basket has
    to be drawn from that pool only - otherwise the tender analysis shows
    customers buying vodka with food stamps, which is both wrong and the first
    thing a grocer would notice.
    """

    def __init__(self, items):
        self.items    = items
        self.fs_items = [it for it in items if it["fs"]]

    def _weight(self, it, month):
        # Items pinned to a movement report already reflect the season they
        # were measured in, so no seasonal multiplier is layered on top.
        if it["observed"] > 0:
            return it["pop"]
        return it["pop"] * SEASON_TABLE[it["season"]][month]

    def rebuild(self, date):
        month = date.month
        self.cum = list(accumulate(
            self._weight(it, month) for it in self.items))
        self.total = self.cum[-1]

        self.fs_cum = list(accumulate(
            self._weight(it, month) for it in self.fs_items))
        self.fs_total = self.fs_cum[-1] if self.fs_cum else 0

    def pick(self, food_stamp_only=False):
        if food_stamp_only and self.fs_total:
            i = bisect.bisect(self.fs_cum, random.random() * self.fs_total)
            return self.fs_items[min(i, len(self.fs_items) - 1)]
        i = bisect.bisect(self.cum, random.random() * self.total)
        return self.items[min(i, len(self.items) - 1)]


# ---------------------------------------------------------------------------
# Build one transaction as TLOG lines
# ---------------------------------------------------------------------------

def build_transaction(date, store, txn_seq, picker, growth):
    store_code, _name, _region, _vol, n_terminals = store
    store_num = store_code[-2:]

    terminal = f"{random.randint(1, n_terminals):02d}"
    cashier  = str(random.randint(1, 14))
    txn_code = f"{store_num}-{txn_seq}"

    hour = random.choices(
        range(7, 22),
        weights=[3, 5, 7, 9, 11, 12, 10, 9, 9, 11, 13, 12, 9, 6, 4]
    )[0]
    time_s = hour * 3600 + random.randint(0, 59) * 60 + random.randint(0, 59)
    date_s = date.strftime("%m/%d/%Y")

    loyalty = ""
    if random.random() < 0.22:
        loyalty = "41" + "".join(str(random.randint(0, 9)) for _ in range(9))

    # Basket size. A grocery run is bigger than a convenience stop, so the
    # distribution is weighted toward the middle rather than toward one or
    # two items.
    n_items = random.choices(
        [1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 18, 22, 26],
        weights=[4, 6, 8, 10, 11, 11, 12, 11, 9, 7, 5, 4, 2]
    )[0]

    base = {
        "Date": date_s,
        "TimeInSeconds": time_s,
        "TerminalCode": terminal,
        "CashierCode": cashier,
        "LoyaltyCode": loyalty,
        "TransactionCode": txn_code,
        "StoreCode": store_code,
    }

    # Decide how the basket will be paid before choosing items. An EBT basket
    # can only contain SNAP-eligible goods, so the tender constrains the
    # shopping rather than the other way round.
    ebt_basket = random.random() < 0.11

    lines, seq = [], 0
    subtotal = tax_total = discounts = 0.0
    has_fs = False

    for _ in range(n_items):
        it = picker.pick(food_stamp_only=ebt_basket)
        dept = it["dept"]

        qty = random.choices([1, 2, 3, 4], weights=[62, 24, 10, 4])[0] \
            if dept in ("11", "16", "31") else \
            random.choices([1, 2, 3], weights=[75, 20, 5])[0]

        price  = round(it["price"] * growth * random.uniform(0.98, 1.02), 2)
        cost   = round(it["cost"] * growth * random.uniform(0.98, 1.02), 4)
        margin = (price - cost) / price if price else 0.25

        taxable = dept in TAXABLE_DEPTS

        retail_type = random.choices(
            ["B", "T", "S", "L"],
            weights=[80, 10, 7, 3 if loyalty else 0]
        )[0]

        depth = max(margin, 0.05) * random.uniform(0.25, 0.70)
        loy_disc = prem_disc = 0.0
        if retail_type == "L" and loyalty:
            loy_disc = round(price * qty * depth, 2)
        elif retail_type in ("T", "S"):
            prem_disc = round(price * qty * depth, 2)

        ext = round(price * qty - loy_disc - prem_disc, 2)

        roll = random.random()
        is_void   = roll < 0.018
        is_refund = 0.018 <= roll < 0.029

        seq += 1
        lines.append(tf.build_line({
            **base,
            "Sequence": seq,
            "KeyFunction": "UPC",
            "KeyFunctionId": it["item_id"],
            "DepartmentCode": dept,
            "Multiple": 1,
            "Retail": price,
            "MovementCount": qty,
            "MovementWeight": 0,
            "ExtSales": ext,
            "AttributeFlag": tf.build_attribute_flag(
                taxable=taxable, food_stamp=it["fs"], wic=it["wic"],
                refund=is_refund),
            "Category": it["cat"],
            "SubCategory": it["cat"],
            "SubDepartment": dept,
            "RetailType": retail_type,
            "LoyaltyDiscount": loy_disc,
            "PremiumDiscount": prem_disc,
            "UnitCost": cost,
            "UnitDeal": 0,
            "POSDescription": it["pos_desc"],
            "ItemDescription": it["desc"],
        }))

        if is_void:
            seq += 1
            lines.append(tf.build_line({
                **base,
                "Sequence": seq,
                "KeyFunction": "VOID",
                "KeyFunctionId": it["item_id"],
                "DepartmentCode": dept,
                "Multiple": 1,
                "Retail": price,
                "MovementCount": -qty,
                "MovementWeight": 0,
                "ExtSales": -ext,
                "AttributeFlag": tf.build_attribute_flag(
                    taxable=taxable, food_stamp=it["fs"], void=True),
                "Category": it["cat"],
                "SubCategory": it["cat"],
                "SubDepartment": dept,
                "RetailType": "",
                "UnitCost": cost,
                "POSDescription": it["pos_desc"],
                "ItemDescription": it["desc"],
            }))
            continue

        if is_refund:
            subtotal -= ext
        else:
            subtotal += ext
            if taxable:
                tax_total += ext * TAX_RATE
            if it["fs"]:
                has_fs = True
        discounts += loy_disc + prem_disc

    subtotal  = round(subtotal, 2)
    tax_total = round(tax_total, 2)
    total     = round(subtotal + tax_total, 2)
    discounts = round(discounts, 2)
    points    = int(max(subtotal, 0)) if loyalty else 0

    media = "FOODSTAMP" if (ebt_basket and has_fs) else \
        random.choices(TENDER_MEDIA, weights=[22, 38, 28, 0, 3])[0]
    acct = "PCI #" if media in ("CREDIT", "DEBIT") else ""
    cash_back = float(random.choice([20, 40, 60])) \
        if (media == "DEBIT" and random.random() < 0.10) else 0.0

    seq += 1
    lines.append(tf.build_line({
        **base,
        "Sequence": seq,
        "KeyFunction": "TENDER",
        "KeyFunctionId": media,
        "DepartmentCode": 0,
        "TenderAmount": round(total + cash_back, 2),
        "TenderAccountNumber": acct,
        "SubTotal": subtotal,
        "TaxTotal": tax_total,
        "TotalAmount": total,
        "CashBackAmount": cash_back,
    }))

    lines.append(tf.build_line({
        **base,
        "Sequence": 0,
        "KeyFunction": "",
        "KeyFunctionId": "",
        "DepartmentCode": 0,
        "SubTotal": subtotal,
        "TaxTotal": tax_total,
        "TotalAmount": total,
        "TotalDiscount": discounts,
        "TotalPoints": points,
        "CashBackAmount": cash_back,
    }))

    return txn_code, lines


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

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


def main():
    print("Generating RORC TLOG data from the client's product catalogue")
    items = load_items()
    print(f"  catalogue:    {len(items):,} items")

    # Average basket size and line quantity implied by the distributions in
    # build_transaction, used to convert observed units/day into a weight.
    # Measured from a generated run rather than estimated - the qty figure in
    # particular came out lower than a naive read of the weights suggests,
    # because voids and returns remove lines from the sale total.
    AVG_ITEMS_PER_BASKET = 8.38
    AVG_QTY_PER_LINE     = 1.36
    REPORTING_STORE_MULT = STORES[0][3]   # movement reports come from STORE01

    pinned = pin_observed_items(items, REPORTING_STORE_MULT,
                                AVG_ITEMS_PER_BASKET, AVG_QTY_PER_LINE)
    if pinned:
        print(f"  observed:     {pinned:,} items pinned to measured "
              f"movement (not modelled)")

    store_names = {s[0]: s[1] for s in STORES}
    dept_names   = {}
    cat_names    = {}
    product_keys = {}
    for it in items:
        dept_names[it["dept"]] = it["dept_name"]
        cat_names[it["item_id"]] = it["cat_name"]
        product_keys[it["item_id"]] = it["product_key"]

    end_date    = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date  = (end_date - timedelta(days=HISTORY_DAYS)).replace(day=1)
    sample_from = end_date - timedelta(days=SAMPLE_DAYS)
    start_year  = start_date.year

    print(f"  stores:       {len(STORES)}")
    print(f"  departments:  {len(dept_names)}")
    print(f"  period:       {start_date.date()} to {end_date.date()}")
    print(f"  TLOG samples: {sample_from.date()} onward -> {TLOG_DIR}/")
    print()

    if os.path.isdir(TLOG_DIR):
        shutil.rmtree(TLOG_DIR)
    os.makedirs(TLOG_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -40000")
    conn.executescript(SCHEMA)

    conn.executemany("INSERT INTO dim_store VALUES (?,?,?)",
                     [(s[0], s[1], s[2]) for s in STORES])
    conn.executemany("INSERT INTO dim_department VALUES (?,?)",
                     sorted(dept_names.items()))
    conn.executemany("INSERT INTO dim_item VALUES (?,?,?,?,?,?,?,?,?,?)",
                     [(it["item_id"], it["desc"], it["product_key"], it["dept"],
                       it["dept_name"], it["cat_name"], "", it["price"],
                       it["cost"],
                       round((it["price"] - it["cost"]) / it["price"] * 100, 1))
                      for it in items])
    conn.commit()

    picker = WeightedPicker(items)
    txn_seq, batch, total_rows, files_out = 100000, [], 0, 0

    current = start_date
    while current <= end_date:
        picker.rebuild(current)
        dow    = day_of_week_multiplier(current)
        hol    = holiday_multiplier(current)
        growth = yoy_growth(current, start_year)
        write_files = current >= sample_from

        for store in STORES:
            n_baskets = int(BASE_BASKETS * store[3] * dow * hol * growth
                            * random.uniform(0.88, 1.12))

            for _ in range(n_baskets):
                txn_seq += 1
                _code, lines = build_transaction(
                    current, store, txn_seq, picker, growth)

                if write_files:
                    fname = tf.build_filename(current, txn_seq, store[0])
                    with open(os.path.join(TLOG_DIR, fname), "w") as f:
                        f.write("\n".join(lines) + "\n")
                    files_out += 1

                # Parse back through the production parser
                parsed, _fin = tf.parse_transaction_lines(lines)

                # How the basket was paid for lives on the tender line, but
                # the useful question is "what do EBT shoppers buy" - so the
                # basket's tender is stamped onto every item line. Glue does
                # the same thing when it flattens a transaction.
                basket_tender = next(
                    (r["tender_type"] for r in parsed
                     if r["transaction_type"] == "TENDER" and r["tender_type"]),
                    ""
                )

                for r in parsed:
                    if r["transaction_type"] == "TENDER":
                        continue
                    batch.append((
                        r["transaction_code"],
                        r["store_code"],
                        store_names.get(r["store_code"], r["store_code"]),
                        r["transaction_date"].strftime("%Y-%m-%d %H:%M:%S"),
                        r["terminal_code"], r["cashier_code"], r["sequence"],
                        r["key_function"], r["item_id"], r["item_desc"],
                        product_keys.get(r["item_id"], r["item_desc"].upper()),
                        r["department_code"],
                        dept_names.get(r["department_code"], "Unknown"),
                        cat_names.get(r["item_id"], ""),
                        r["transaction_type"],
                        TYPE_NAMES.get(r["transaction_type"], r["transaction_type"]),
                        r["is_loss_event"], r["quantity"], r["unit_price"],
                        r["unit_cost"], r["extended_price"], r["gross_margin"],
                        r["retail_type"], r["attribute_flag"], r["is_taxable"],
                        r["is_food_stamp"], r["is_wic"], r["loyalty_discount"],
                        r["premium_discount"], r["loyalty_code"],
                        r["tender_amount"], basket_tender,
                    ))

                if len(batch) >= 50000:
                    conn.executemany(INSERT_SQL, batch)
                    total_rows += len(batch)
                    batch = []

        if current.day == 1:
            conn.commit()
            print(f"  {current.strftime('%B %Y')}...", flush=True)
        current += timedelta(days=1)

    if batch:
        conn.executemany(INSERT_SQL, batch)
        total_rows += len(batch)
    conn.commit()

    cur = conn.cursor()
    rev = cur.execute("SELECT SUM(extended_price) FROM fact_transactions "
                      "WHERE transaction_type='SALE'").fetchone()[0] or 0
    mar = cur.execute("SELECT SUM(gross_margin) FROM fact_transactions "
                      "WHERE transaction_type='SALE'").fetchone()[0] or 0
    txns = cur.execute("SELECT COUNT(DISTINCT transaction_id) "
                       "FROM fact_transactions").fetchone()[0]
    sold = cur.execute("SELECT COUNT(DISTINCT item_id) "
                       "FROM fact_transactions").fetchone()[0]

    print()
    print("Done.")
    print(f"  {DB_PATH}")
    print(f"    line items:    {total_rows:,}")
    print(f"    transactions:  {txns:,}")
    print(f"    net sales:     ${rev:,.2f}")
    print(f"    gross margin:  ${mar:,.2f}  ({mar/rev*100:.1f}%)" if rev else "")
    print(f"    items sold:    {sold:,} of {len(items):,} in catalogue")
    print(f"  {TLOG_DIR}/")
    print(f"    TLOG files:    {files_out:,}")

    conn.close()


if __name__ == "__main__":
    main()
