#!/usr/bin/env python3
"""
Build the product dimension from the client's RORC price book export.

Input:  price_book_export.csv  - the 303-column label/price export out of RORC
Output: item_master.csv   - a clean product dimension the pipeline can use

The TLOG itself carries only numeric department codes and barcodes. This
export is where the readable names, costs, and category structure come from,
so in production it becomes a second feed into the warehouse alongside the
transaction logs.

Run:  python3 build_item_master.py [path-to-price_book_export.csv]
"""

import csv
import sys
import os
import re
import bisect
import random
import collections

random.seed(42)

DEFAULT_INPUT = "price_book_export.csv"
OUTPUT        = "item_master.csv"
MOVEMENT      = "movement_actuals.csv"   # produced by load_movement_report.py

# Department codes come from the export. Names are expanded from the
# three-letter Dept 3 abbreviation RORC carries alongside them.
DEPARTMENT_NAMES = {
    "10": "Alcohol",
    "11": "Produce",
    "12": "Kitchen",
    "13": "Meat",
    "14": "Deli",
    "15": "Caviar",
    "16": "Dairy",
    "17": "Bread",
    "18": "Pet",
    "19": "Seafood",
    "20": "Bakery",
    "21": "Frozen",
    "22": "Grocery",
    "25": "Non-Food",
    "26": "Dry Goods",
    "27": "Pickled",
    "28": "Garden",
    "30": "Reusable Bags",
    "31": "Soda",
    "32": "Beer",
    "33": "Bottle Deposit",
    "34": "Gift",
    "35": "Online Fees",
    "40": "Garden",
}

# Departments left out of the sales model. Kept deliberately small - the real
# transaction logs show reusable bags (dept 30) on more than 11,000 lines in a
# single quarter, so excluding them dropped genuine revenue and left a large
# unmatched bucket. Only online fees and deposits are dropped now.
EXCLUDE_DEPARTMENTS = {"33", "35"}

# Seasonal behaviour is inferred from the description and category text,
# since the price book has no seasonality field.
#
# Rules are evaluated in order, most specific first. Fresh produce is listed
# ahead of the general rules because it is the most seasonal thing in the
# store and the easiest for a merchandiser to spot if it is wrong - winter
# citrus topping the August chart is an immediate credibility failure.
SEASON_RULES = [
    # ---- fresh produce, most specific first ----
    ("summer",  ["STRAWBERR", "RASPBERR", "BLUEBERR", "BLACKBERR",
                 "CURRANT", "GOOSEBERR", "CHERRIES", "CHERRY LB",
                 "WATERMELON", "CANTALOUPE", "HONEYDEW", "MELON",
                 "PEACH", "NECTARIN", "APRICOT", "PLUM", "PLUOT",
                 "CORN ON", "SWEET CORN", "ZUCCHINI", "CUCUMBER",
                 "TOMATO", "BASIL", "FIG FRESH", "MANGO", "APRIUM"]),
    ("fall",    ["GRAPES", "CONCORD", "APPLE", "PEAR ", "PEARS",
                 "PUMPKIN", "SQUASH", "CRANBERR", "QUINCE",
                 "BRUSSELS", "CAULIFLOWER", "SWEET POTATO", "YAM"]),
    ("winter",  ["MANDARIN", "CLEMENTIN", "TANGERIN", "NAVEL", "ORANGE",
                 "GRAPEFRUIT", "PERSIMMON", "POMEGRAN", "KIWI",
                 "CITRUS", "LEMON", "LIME", "DATES", "CHESTNUT",
                 "CABBAGE", "TURNIP", "PARSNIP", "BEET", "LEEK"]),
    ("spring",  ["ASPARAGUS", "RAMP", "FIDDLEHEAD", "RHUBARB",
                 "ARTICHOKE", "PEAS SNAP", "EASTER", "KULICH", "PASKA"]),

    # ---- everything else ----
    ("summer",  ["ICE-CREAM", "ICE CREAM", "POPSICLE", "SODA", "LEMONADE",
                 "KVASS", "BEER", "GRILL", "CHARCOAL", "SUNSCREEN",
                 "BOTTLED WATER", "SPARKLING WATER"]),
    ("winter",  ["TEA ", "COFFEE", "SOUP", "BROTH", "HONEY", "PHARMACY",
                 "VITAMIN", "COCOA", "PORRIDGE", "OATMEAL", "COLD MED",
                 "COUGH", "LOTION"]),
    ("holiday", ["CHAMPAGNE", "CAVIAR", "GIFT", "TURKEY", "HOLIDAY",
                 "SOUVENIR", "COGNAC", "LIQUER", "LIQUEUR", "ADVENT",
                 "ORNAMENT", "CALENDAR"]),
    ("fall",    ["MUSHROOM", "PICKLED", "PRESERVE"]),
]

# Category-level fallback when the description gives nothing away
CATEGORY_SEASON = {
    "BERRIES":      "summer",
    "ICE-CREAM":    "summer",
    "BEER":         "summer",
    "TEA & COFFEE": "winter",
    "PHARMACY":     "winter",
    "WINE":         "holiday",
    "HARD LIQUER":  "holiday",
    "BLACK CAVIAR": "holiday",
    "RED CAVIAR":   "holiday",
}


def money(s):
    """Parse the various price shapes in the export into a float."""
    if s is None:
        return None
    s = str(s).strip()
    if not s or s in ("#NAME?", "-"):
        return None
    # Values like "99¢" appear with a mangled cent sign
    cents = s.endswith("�") or s.endswith("¢")
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s in (".", "-"):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    return v / 100.0 if cents and v > 5 else v


def clean_text(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def recover_upc(row):
    """
    The plain UPC column in this export is damaged - roughly half the values
    were written in scientific notation ("4.60702E+11"), which collapses
    hundreds of distinct barcodes onto the same string.

    The "Upc with Dashes" column survived intact, so the real barcode is
    recovered from there and only falls back to the numeric column when the
    dashed one is missing.
    """
    dashed = re.sub(r"[^0-9]", "", (row.get("Upc with Dashes") or ""))
    if len(dashed) >= 6:
        return dashed.lstrip("0") or dashed, False
    raw = (row.get("UPC") or "").strip()
    corrupted = bool(re.search(r"[Ee]\+?\d", raw))
    return (re.sub(r"[^0-9]", "", raw), corrupted)


def season_for(desc, category):
    blob = f"{desc} {category}".upper()
    for profile, keywords in SEASON_RULES:
        if any(k in blob for k in keywords):
            return profile
    return CATEGORY_SEASON.get((category or "").upper(), "flat")


def normalize_product(desc):
    """
    Collapse the same product sold under different barcodes onto one key.

    The catalogue carries a product like RASPBERRIES 6 OZ under four separate
    UPCs, one per supplier. Left alone that splits every ranked list and buries
    items that are actually selling well. Sizes and pack counts are kept,
    because a 6 oz and a 1 lb clamshell really are different products.
    """
    d = (desc or "").upper()
    d = re.sub(r"\bORGANIC\b", " ORGANIC ", d)          # keep organic distinct
    d = re.sub(r"[^A-Z0-9 ]", " ", d)
    d = re.sub(r"\b(EA|EACH|BY|PER|CT|PC|PCS)\b", " ", d)
    # Normalise size units so "1LB", "1 LB" and "1 POUND" collapse together
    d = re.sub(r"(\d)\s*(LB|LBS|POUND|POUNDS)\b", r"\1LB", d)
    d = re.sub(r"(\d)\s*(OZ|OUNCE|OUNCES)\b", r"\1OZ", d)
    d = re.sub(r"(\d)\s*(G|GR|GRAM|GRAMS)\b", r"\1G", d)
    d = re.sub(r"(\d)\s*(ML|L|LITER|LITRE)\b", r"\1ML", d)
    d = re.sub(r"(\d)\s*(QT|QUART)\b", r"\1QT", d)
    d = re.sub(r"\s+", " ", d).strip()
    return d


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    if not os.path.exists(src):
        print(f"Cannot find {src}")
        print("Pass the path to the RORC price book export, e.g.")
        print("  python3 build_item_master.py ~/Downloads/price_book_export.csv")
        sys.exit(1)

    print(f"Reading {src}")
    with open(src, encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    print(f"  {len(rows):,} rows in the export")

    def g(r, k):
        return (r.get(k) or "").strip()

    # ---- recover missing category names -----------------------------------
    # Roughly 29% of rows have a blank "Category Name", but nearly all of them
    # carry a "Dept 4" sub-department code. Rows that have both let us build a
    # code -> name lookup, which fills in the blanks. Without this, close to
    # half the revenue lands in an "Uncategorized" bucket and every category
    # ranking becomes meaningless.
    dept4_to_category = {}
    votes = collections.defaultdict(collections.Counter)
    for r in rows:
        d4, cn = g(r, "Dept 4"), g(r, "Category Name")
        if d4 and cn:
            votes[d4][cn] += 1
    for d4, counter in votes.items():
        dept4_to_category[d4] = counter.most_common(1)[0][0]

    recovered = 0

    kept, skipped = [], collections.Counter()
    seen_upc = set()
    corrupted_upcs = 0

    for r in rows:
        upc, was_corrupted = recover_upc(r)
        if was_corrupted:
            corrupted_upcs += 1
        desc = clean_text(g(r, "Description")) or clean_text(g(r, "POS Description"))
        dept = g(r, "Department")

        if not upc or not desc:
            skipped["missing upc or description"] += 1
            continue
        if dept in EXCLUDE_DEPARTMENTS:
            skipped["non-merchandise department"] += 1
            continue
        if upc in seen_upc:
            skipped["duplicate upc"] += 1
            continue

        price = money(g(r, "Retail w/o dollar sign")) or money(g(r, "Price w/o dollar sign"))
        cost  = money(g(r, "Cost")) or money(g(r, "Base Unit Cost"))

        if not price or price <= 0:
            skipped["no price"] += 1
            continue

        # Cost is missing or nonsensical on a large share of rows - the client's
        # own movement report shows Driscolls strawberries, one of the store's
        # best sellers, carrying a cost of 0. Dropping those rows silently
        # deleted real, high-volume products from the catalogue and made them
        # look like they had never sold.
        #
        # Instead the item is kept and its cost is imputed from the department's
        # median margin. Imputed rows are flagged so margin figures can be
        # qualified rather than quietly trusted.
        cost_source = "actual"
        if not cost or cost <= 0 or cost >= price or (price - cost) / price > 0.90:
            cost_source = "imputed"
            cost = None

        seen_upc.add(upc)

        category = clean_text(g(r, "Category Name"))
        if not category:
            category = dept4_to_category.get(g(r, "Dept 4"), "")
            if category:
                recovered += 1

        kept.append({
            "item_id":         upc.rjust(14, "0"),
            "upc":             upc,
            "item_desc":       desc[:60],
            "product_key":     normalize_product(desc),
            "pos_desc":        clean_text(g(r, "POS Description"))[:22] or desc[:22],
            "department_code": dept,
            "department_name": DEPARTMENT_NAMES.get(dept, f"Dept {dept}"),
            # The spec writes 0 rather than blank for numeric fields
            "category_code":   g(r, "Sub Category") or "0",
            "category_name":   category or "Uncategorized",
            "brand":           clean_text(g(r, "Brand")),
            "vendor":          clean_text(g(r, "Vendor Name")),
            "unit_price":      round(price, 2),
            "unit_cost":       cost,          # filled in below when imputed
            "cost_source":     cost_source,
            "food_stamp":      "Y" if g(r, "Food Stamp").upper().startswith("Y") else "N",
            "wic":             "Y" if g(r, "WIC").upper() == "WIC" else "N",
            "season":          season_for(desc, category),
        })

    print(f"  {len(kept):,} items kept")
    for reason, n in skipped.most_common():
        print(f"    skipped {n:>7,}  {reason}")
    if corrupted_upcs:
        print(f"    NOTE: {corrupted_upcs:,} rows had a scientific-notation UPC "
              f"and no usable dashed barcode")

    # ---- impute missing costs from department medians ----------------------
    dept_margins = collections.defaultdict(list)
    for it in kept:
        if it["cost_source"] == "actual":
            dept_margins[it["department_code"]].append(
                (it["unit_price"] - it["unit_cost"]) / it["unit_price"])

    def median(xs):
        s = sorted(xs)
        n = len(s)
        if not n:
            return None
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    dept_median = {d: median(v) for d, v in dept_margins.items()}
    overall = median([m for v in dept_margins.values() for m in v]) or 0.35

    imputed = 0
    for it in kept:
        if it["cost_source"] == "imputed":
            m = dept_median.get(it["department_code"]) or overall
            m = min(max(m, 0.05), 0.75)
            it["unit_cost"] = round(it["unit_price"] * (1 - m), 4)
            imputed += 1
        it["margin_pct"] = round(
            (it["unit_price"] - it["unit_cost"]) / it["unit_price"] * 100, 1)

    # ---- assign demand weights -------------------------------------------
    # The price book has no sales velocity (Avg Sal/Day is empty), so demand
    # is modelled with a power law: a small share of items drives most sales
    # and there is a long tail of slow movers. That mirrors how a real
    # catalogue of this size behaves and gives the slow-mover analysis
    # something meaningful to find.
    by_dept = collections.defaultdict(list)
    for it in kept:
        by_dept[it["department_code"]].append(it)

    # Zipf-Mandelbrot: weight = 1 / (rank + q) ** s
    #
    # A plain Zipf curve (q = 0) hands the top item roughly 15% of a whole
    # department, which produced a single gooseberry SKU outselling the rest
    # of produce combined. The q offset flattens the head without losing the
    # long tail.
    #
    # Tuned against the client's Item Movement Report for the Berry category
    # (8/7-8/14/2026, Riverside): 13 SKUs carried 325 units that week. These
    # values put the top item near 8% of its department, the top ten near 44%,
    # and the top fifty near 75% - in line with published grocery norms.
    ZIPF_Q = 6
    ZIPF_S = 1.6

    # Rank within a department is not random. Cheap staples turn over far
    # faster than expensive specialities - a store sells more bananas than
    # saffron - so price drives most of the ordering, with noise so the result
    # is not a strict price list. Without this, a random draw can hand rank 1
    # to something obscure and the August produce chart ends up led by
    # gooseberries.
    for dept_items in by_dept.values():
        prices = sorted(it["unit_price"] for it in dept_items)
        n = len(prices)

        def price_percentile(p):
            return bisect.bisect_left(prices, p) / n if n else 0.5

        for it in dept_items:
            cheapness = 1.0 - price_percentile(it["unit_price"])
            # 0.45 keeps staples at the top without pushing every mid-priced
            # item into the tail. At 0.7 the basket filled up with the
            # cheapest thing in each department and average basket value
            # collapsed to well under what a real grocery run looks like.
            it["_rank_score"] = 0.28 * cheapness + 0.72 * random.random()

        dept_items.sort(key=lambda x: -x["_rank_score"])
        for rank, it in enumerate(dept_items, 1):
            base = 1.0 / ((rank + ZIPF_Q) ** ZIPF_S)
            it["popularity"] = round(base * random.uniform(0.75, 1.35), 8)
            del it["_rank_score"]

    # ---- normalise departments to a target REVENUE mix ---------------------
    # The weights above control how often an item is picked, not how much
    # money it brings in. Scaling them directly gets the mix backwards:
    # Deli sells by the pound at $12 while Grocery sells $2 tins, so a raw
    # frequency scale left Deli at 15% of revenue and Grocery at 8%.
    #
    # Instead, each department's expected revenue is computed as
    # sum(weight * price), then scaled so the department lands on its target
    # share. Targets are typical supermarket proportions.
    TARGET_REVENUE_SHARE = {
        "22": 0.26,   # Grocery - center store
        "14": 0.12,   # Deli
        "11": 0.11,   # Produce
        "16": 0.09,   # Dairy
        "10": 0.08,   # Alcohol
        "13": 0.07,   # Meat
        "21": 0.07,   # Frozen
        "20": 0.06,   # Bakery
        "17": 0.04,   # Bread
        "19": 0.04,   # Seafood
        "25": 0.03,   # Non-Food
        "26": 0.015,  # Dry Goods
        "15": 0.010,  # Caviar - high ticket, low frequency
        "27": 0.008,  # Pickled
        "12": 0.007,  # Kitchen
        "31": 0.005,  # Soda
        "32": 0.005,  # Beer
        "18": 0.003,  # Pet
        "28": 0.001,  # Garden
        "40": 0.001,
    }

    expected = {
        code: sum(it["popularity"] * it["unit_price"] for it in dept_items)
        for code, dept_items in by_dept.items()
    }

    default_share = 0.002
    for code, dept_items in by_dept.items():
        target = TARGET_REVENUE_SHARE.get(code, default_share)
        exp = expected.get(code, 0)
        if exp <= 0:
            continue
        factor = target / exp
        for it in dept_items:
            it["popularity"] = it["popularity"] * factor

    # Rescale everything to comfortable magnitudes
    biggest = max(it["popularity"] for it in kept)
    for it in kept:
        it["popularity"] = round(it["popularity"] / biggest, 9)
        it["observed_units_per_day"] = ""
        it["demand_source"] = "modelled"

    # ---- overlay observed movement ----------------------------------------
    # Anything covered by an Item Movement Report stops being modelled. The
    # real units-per-day is carried through to the generator, which pins those
    # items to their measured rate instead of drawing them from the curve.
    observed = 0
    suppressed = 0
    if os.path.exists(MOVEMENT):
        by_id = {it["item_id"]: it for it in kept}
        # Also index without leading zeros, since padding varies by report
        by_stripped = {it["item_id"].lstrip("0"): it for it in kept}
        covered_categories = set()

        with open(MOVEMENT, newline="", encoding="utf-8") as f:
            for m in csv.DictReader(f):
                if m.get("category_name"):
                    covered_categories.add(m["category_name"].strip().upper())

        with open(MOVEMENT, newline="", encoding="utf-8") as f:
            for m in csv.DictReader(f):
                upd = m.get("units_per_day")
                if not upd:
                    continue
                try:
                    upd = float(upd)
                except ValueError:
                    continue
                if upd <= 0:
                    continue

                key = m["item_id"]
                it = by_id.get(key) or by_stripped.get(key.lstrip("0"))
                if not it:
                    continue

                it["observed_units_per_day"] = round(upd, 4)
                it["demand_source"] = "observed"

                # Trust the report's cost over the price book when present
                cost = m.get("unit_cost")
                if cost:
                    try:
                        c = float(cost)
                        if 0 < c < it["unit_price"]:
                            it["unit_cost"] = round(c, 4)
                            it["cost_source"] = "movement report"
                            it["margin_pct"] = round(
                                (it["unit_price"] - c) / it["unit_price"] * 100, 1)
                    except ValueError:
                        pass
                observed += 1

        # A movement report lists everything that moved. An item sitting in a
        # covered category that does not appear in the report did not sell in
        # that window, so its modelled demand is suppressed rather than left
        # to pile on top of the measured total.
        for it in kept:
            if (it["demand_source"] == "modelled"
                    and it["category_name"].strip().upper() in covered_categories):
                it["popularity"] *= 0.02
                it["demand_source"] = "not in report"
                suppressed += 1

        print(f"  observed movement applied to:     {observed:,} items "
              f"(from {MOVEMENT})")
        if suppressed:
            print(f"  suppressed in covered categories: {suppressed:,} items "
                  f"(absent from the report, so treated as not selling)")
    else:
        print(f"  no {MOVEMENT} found - all demand is modelled")
        print(f"    run load_movement_report.py on any Item Movement Report "
              f"to pin real items to real velocity")

    fields = ["item_id", "upc", "item_desc", "product_key", "pos_desc",
              "department_code", "department_name", "category_code",
              "category_name", "brand", "vendor", "unit_price", "unit_cost",
              "cost_source", "margin_pct", "food_stamp", "wic", "season",
              "popularity", "observed_units_per_day", "demand_source"]

    with open(OUTPUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(kept)

    print()
    print(f"Wrote {OUTPUT}  ({len(kept):,} items)")

    uncat = sum(1 for i in kept if i["category_name"] == "Uncategorized")
    print(f"  costs imputed (missing in export):{imputed:,} "
          f"({imputed / len(kept) * 100:.1f}%) - margin on these is estimated")
    print(f"  categories recovered from Dept 4: {recovered:,}")
    print(f"  still uncategorized:              {uncat:,} "
          f"({uncat / len(kept) * 100:.1f}%)")

    groups = collections.Counter(i["product_key"] for i in kept)
    multi  = sum(1 for _k, n in groups.items() if n > 1)
    print(f"  distinct products:                {len(groups):,} "
          f"({multi:,} sold under more than one barcode)")

    dept_counts = collections.Counter(i["department_name"] for i in kept)
    print()
    print("Items by department:")
    for d, n in dept_counts.most_common():
        print(f"  {d:<18} {n:>7,}")

    seasons = collections.Counter(i["season"] for i in kept)
    print()
    print("Seasonal profiles:", dict(seasons))


if __name__ == "__main__":
    main()
