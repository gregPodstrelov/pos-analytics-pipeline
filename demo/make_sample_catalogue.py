#!/usr/bin/env python3
"""
Build a synthetic product catalogue for the demo.

The real project reads a price book exported from the client's POS. That file
is their commercial data - supplier names, wholesale costs, margins - so it is
not in this repository and never will be. This script generates a stand-in
with the same schema and the same statistical shape, so every other script in
the repo runs unmodified.

What is preserved:
  - the column layout build_item_master.py produces
  - a long-tail demand curve, so a handful of items carry most of the volume
  - department and category structure of a specialty grocery
  - seasonal assignment, so year-over-year queries have something to find
  - realistic price and margin bands per department

What is not real: every barcode, product name, brand, vendor and price.

Barcodes use the GS1 prefix 2, which is reserved for in-store and
variable-weight use and is never issued to a manufacturer. Nothing generated
here can collide with a real product.

    python3 make_sample_catalogue.py                # 12,000 items
    python3 make_sample_catalogue.py --items 40000
"""

import csv
import random
import argparse

SEED = 20260817           # fixed, so the catalogue is reproducible

# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------
# (department_code, department_name, share_of_catalogue, margin_low, margin_high)

DEPARTMENTS = [
    ("10", "Alcohol",   0.06, 0.22, 0.34),
    ("11", "Produce",   0.07, 0.30, 0.48),
    ("12", "Kitchen",   0.04, 0.40, 0.58),
    ("13", "Meat",      0.06, 0.24, 0.38),
    ("14", "Deli",      0.06, 0.30, 0.44),
    ("15", "Caviar",    0.01, 0.28, 0.40),
    ("16", "Dairy",     0.07, 0.22, 0.34),
    ("17", "Bread",     0.03, 0.32, 0.46),
    ("18", "Pet",       0.02, 0.26, 0.38),
    ("19", "Seafood",   0.05, 0.26, 0.40),
    ("20", "Bakery",    0.05, 0.38, 0.55),
    ("21", "Frozen",    0.08, 0.28, 0.42),
    ("22", "Grocery",   0.20, 0.26, 0.42),
    ("25", "Non-Food",  0.06, 0.34, 0.52),
    ("26", "Dry Goods", 0.05, 0.24, 0.36),
    ("27", "Pickled",   0.03, 0.32, 0.46),
    ("31", "Soda",      0.04, 0.28, 0.44),
    ("32", "Beer",      0.02, 0.20, 0.32),
]

# department -> [(category_name, season, food_stamp, wic, price_low, price_high)]
CATEGORIES = {
    "10": [("WINE", "holiday", 0, 0, 8.99, 42.0), ("VODKA", "holiday", 0, 0, 12.99, 55.0),
           ("LIQUEUR", "holiday", 0, 0, 14.99, 38.0), ("BRANDY", "winter", 0, 0, 16.99, 60.0)],
    "11": [("FRUIT", "summer", 1, 1, 0.99, 8.99), ("VEGETABLES", "flat", 1, 1, 0.79, 6.49),
           ("BERRIES", "summer", 1, 1, 2.99, 9.99), ("HERBS", "spring", 1, 0, 1.29, 4.49)],
    "12": [("PREPARED MEALS", "flat", 1, 0, 4.99, 16.99), ("SOUPS", "winter", 1, 0, 3.99, 11.99),
           ("SALADS", "summer", 1, 0, 3.49, 12.99)],
    "13": [("BEEF", "flat", 1, 0, 6.99, 28.0), ("PORK", "flat", 1, 0, 4.99, 19.0),
           ("POULTRY", "flat", 1, 0, 3.99, 15.0), ("SAUSAGE", "winter", 1, 0, 5.49, 18.0),
           ("CURED MEATS", "holiday", 1, 0, 7.99, 32.0)],
    "14": [("CHEESE", "flat", 1, 1, 4.49, 26.0), ("SLICED MEATS", "flat", 1, 0, 5.99, 21.0),
           ("OLIVES", "flat", 1, 0, 3.49, 12.99)],
    "15": [("CAVIAR", "holiday", 0, 0, 24.99, 180.0), ("ROE", "holiday", 0, 0, 12.99, 65.0)],
    "16": [("MILK", "flat", 1, 1, 1.99, 6.49), ("YOGURT", "flat", 1, 1, 1.29, 7.99),
           ("KEFIR", "flat", 1, 1, 2.49, 6.99), ("BUTTER", "holiday", 1, 1, 3.99, 12.99),
           ("CREAM", "flat", 1, 0, 2.29, 8.49), ("FARMER CHEESE", "flat", 1, 1, 2.99, 9.99)],
    "17": [("RYE BREAD", "flat", 1, 1, 2.49, 7.99), ("WHITE BREAD", "flat", 1, 1, 1.99, 5.99),
           ("FLATBREAD", "flat", 1, 0, 2.29, 6.49)],
    "18": [("DOG FOOD", "flat", 0, 0, 4.99, 34.0), ("CAT FOOD", "flat", 0, 0, 3.99, 29.0)],
    "19": [("SMOKED FISH", "holiday", 1, 0, 7.99, 38.0), ("FRESH FISH", "flat", 1, 0, 6.99, 26.0),
           ("HERRING", "winter", 1, 0, 4.99, 16.99), ("CANNED FISH", "flat", 1, 0, 1.99, 9.99)],
    "20": [("PASTRY", "flat", 1, 0, 1.99, 9.99), ("CAKE", "holiday", 1, 0, 8.99, 42.0),
           ("COOKIES", "flat", 1, 0, 1.49, 8.99), ("WAFERS", "flat", 1, 0, 1.29, 6.49)],
    "21": [("DUMPLINGS", "winter", 1, 0, 3.99, 14.99), ("FROZEN VEGETABLES", "flat", 1, 1, 1.99, 7.49),
           ("ICE CREAM", "summer", 1, 0, 2.99, 11.99), ("FROZEN BERRIES", "winter", 1, 0, 3.49, 12.99),
           ("FROZEN FISH", "flat", 1, 0, 4.99, 18.99)],
    "22": [("CHOCOLATE BARS AND GUMS", "holiday", 1, 0, 0.99, 9.99), ("CANDY", "holiday", 1, 0, 1.29, 14.99),
           ("TEA", "winter", 1, 0, 2.49, 18.99), ("COFFEE", "flat", 1, 0, 4.99, 24.99),
           ("PRESERVES", "fall", 1, 0, 2.99, 12.99), ("HONEY", "fall", 1, 0, 4.99, 19.99),
           ("GRAINS", "flat", 1, 1, 1.49, 9.99), ("PASTA", "flat", 1, 1, 1.29, 7.99),
           ("OIL", "flat", 1, 0, 3.49, 22.99), ("CONDIMENTS", "summer", 1, 0, 1.99, 9.49)],
    "25": [("PHARMACY", "winter", 0, 0, 2.99, 24.99), ("HOUSEHOLD", "flat", 0, 0, 1.99, 18.99),
           ("PAPER GOODS", "flat", 0, 0, 2.49, 16.99), ("COSMETICS", "flat", 0, 0, 3.99, 32.0)],
    "26": [("FLOUR", "holiday", 1, 1, 1.99, 9.99), ("SUGAR", "holiday", 1, 0, 2.49, 8.99),
           ("RICE", "flat", 1, 1, 2.29, 14.99), ("BUCKWHEAT", "flat", 1, 1, 2.99, 11.99)],
    "27": [("PICKLES", "fall", 1, 0, 2.99, 11.99), ("SAUERKRAUT", "fall", 1, 0, 3.49, 10.99),
           ("MARINATED VEGETABLES", "fall", 1, 0, 3.29, 13.99)],
    "31": [("SODA", "summer", 0, 0, 0.99, 9.99), ("JUICE", "summer", 1, 1, 1.99, 11.99),
           ("WATER", "summer", 0, 0, 0.79, 8.99), ("KVASS", "summer", 0, 0, 1.99, 7.49)],
    "32": [("BEER", "summer", 0, 0, 1.99, 18.99), ("CIDER", "summer", 0, 0, 2.99, 14.99)],
}

# Invented. Any resemblance to a real brand is accidental.
BRANDS = ["Alenka", "Baltika Bay", "Cedar Hollow", "Dvina", "Ekran", "Falcon Ridge",
          "Golden Steppe", "Havlicek", "Ivanko", "Jantar", "Kolos", "Lastochka",
          "Morozko", "Nevsky", "Orlik", "Podolsk", "Radost", "Sever", "Tulip Farms",
          "Uralsk", "Vesna", "White Birch", "Yarilo", "Zorka", "Amber Field",
          "Blue Danube", "Cherna Gora", "Dunav", "Elbrus", "Fjordline"]

VENDORS = ["Anchor Foods Import", "Belmont Provisions", "Continental Wholesale",
           "Dockside Seafood Co", "Eastgate Distributors", "Fairline Produce",
           "Granary Trading", "Harbor Point Foods", "Ironwood Supply",
           "Junction Foods", "Keystone Grocers Supply", "Lakeshore Dairy Co",
           "Meridian Import Group", "Northfield Bakery Supply", "Overland Foods"]

# Category -> descriptive words used to build a product name. Generic on
# purpose; nothing here is lifted from a real catalogue.
WORDS = {
    "default": ["Classic", "Traditional", "Homestyle", "Village", "Farmhouse",
                "Premium", "Select", "Original", "Family", "Garden"],
    "FRUIT": ["Apple", "Pear", "Plum", "Peach", "Apricot", "Grape", "Melon",
              "Cherry", "Fig", "Pomegranate", "Persimmon", "Quince"],
    "BERRIES": ["Strawberry", "Raspberry", "Blueberry", "Blackberry",
                "Currant", "Gooseberry", "Cranberry", "Lingonberry"],
    "VEGETABLES": ["Cabbage", "Beet", "Carrot", "Potato", "Onion", "Cucumber",
                   "Tomato", "Pepper", "Radish", "Turnip", "Squash"],
    "HERBS": ["Dill", "Parsley", "Cilantro", "Scallion", "Sorrel", "Basil"],
    "CHEESE": ["Brined", "Smoked", "Aged", "Soft", "Semi Hard", "Braided",
               "Sheep Milk", "Goat Milk"],
    "SAUSAGE": ["Smoked", "Semi Smoked", "Boiled", "Dry Cured", "Hunter",
                "Liver", "Blood", "Garlic"],
    "SMOKED FISH": ["Mackerel", "Salmon", "Trout", "Halibut", "Sprat",
                    "Capelin", "Pollock", "Sturgeon"],
    "FRESH FISH": ["Carp", "Perch", "Pike", "Cod", "Flounder", "Bream"],
    "HERRING": ["Fillet", "Whole", "In Oil", "In Wine Sauce", "Matjes"],
    "DUMPLINGS": ["Potato", "Cherry", "Meat", "Cabbage", "Mushroom",
                  "Farmer Cheese", "Veal"],
    "TEA": ["Black", "Green", "Herbal", "Fruit", "Mint", "Linden", "Chamomile"],
    "CANDY": ["Caramel", "Toffee", "Praline", "Jelly", "Nougat", "Halva"],
    "CHOCOLATE BARS AND GUMS": ["Dark", "Milk", "Hazelnut", "Almond", "Porous",
                                "Wafer", "Truffle"],
    "PRESERVES": ["Raspberry", "Strawberry", "Apricot", "Plum", "Rose Petal",
                  "Sour Cherry", "Blackcurrant"],
    "PICKLES": ["Barrel", "Dill", "Spicy", "Half Sour", "Gherkin"],
    "WINE": ["Dry Red", "Dry White", "Semi Sweet Red", "Semi Sweet White",
             "Sparkling", "Rose"],
    "VODKA": ["Classic", "Wheat", "Rye", "Cranberry", "Pepper", "Honey"],
    "BEER": ["Lager", "Pilsner", "Dark", "Wheat", "Unfiltered", "Amber"],
    "MILK": ["Whole", "Reduced Fat", "Baked", "Ultra Pasteurized"],
    "YOGURT": ["Plain", "Strawberry", "Peach", "Drinkable", "Greek Style"],
    "CAVIAR": ["Red Salmon", "Pink Salmon", "Trout", "Pike", "Black Sturgeon"],
    "COOKIES": ["Butter", "Shortbread", "Oat", "Honey", "Poppy Seed", "Walnut"],
    "PASTRY": ["Cheese", "Cabbage", "Apple", "Poppy Seed", "Cherry", "Meat"],
    "GRAINS": ["Pearl Barley", "Millet", "Semolina", "Oat", "Corn"],
    "PHARMACY": ["Cough Syrup", "Pain Relief", "Vitamin C", "Bandage",
                 "Antiseptic", "Throat Lozenge", "Cold Remedy", "Eye Drops"],
    "HOUSEHOLD": ["Dish Soap", "Laundry Detergent", "Sponge", "Surface Cleaner",
                  "Bleach", "Air Freshener", "Trash Bag", "Scouring Pad"],
    "PAPER GOODS": ["Paper Towel", "Napkin", "Facial Tissue", "Toilet Paper",
                    "Aluminium Foil", "Parchment Paper", "Paper Plate"],
    "COSMETICS": ["Shampoo", "Hand Cream", "Soap Bar", "Toothpaste", "Lotion",
                  "Deodorant", "Conditioner", "Shaving Gel"],
    "DOG FOOD": ["Dry Chicken", "Wet Beef", "Lamb", "Puppy", "Senior",
                 "Grain Free", "Salmon"],
    "CAT FOOD": ["Dry Chicken", "Wet Tuna", "Kitten", "Senior", "Salmon",
                 "Indoor Formula", "Hairball"],
}

# Categories whose word list already names the product. Appending the category
# on top of these produces "GARDEN PHARMACY", which is nonsense.
SELF_NAMING = {
    "FRUIT", "BERRIES", "VEGETABLES", "HERBS", "PHARMACY", "HOUSEHOLD",
    "PAPER GOODS", "COSMETICS",
}

# Size units have to match what the thing actually is. Vodka sold in grams and
# cabbage sold in litres is the fastest way to make generated data look
# generated.
WEIGHT_SIZES = ["100 G", "200 G", "250 G", "400 G", "500 G", "1 KG",
                "6 OZ", "8 OZ", "12 OZ", "16 OZ", "1 LB", "2 LB"]
LIQUID_SIZES = ["250 ML", "330 ML", "500 ML", "750 ML", "1 L", "1.5 L", "2 L"]
COUNT_SIZES = ["EA", "PK", "6 PK", "12 PK", "BUNCH"]

LIQUID_CATEGORIES = {
    "WINE", "VODKA", "LIQUEUR", "BRANDY", "MILK", "KEFIR", "CREAM", "JUICE",
    "WATER", "SODA", "KVASS", "BEER", "CIDER", "OIL",
}
COUNT_CATEGORIES = {"FRUIT", "VEGETABLES", "HERBS", "CAKE", "PASTRY",
                    "RYE BREAD", "WHITE BREAD", "FLATBREAD",
                    "PAPER GOODS", "HOUSEHOLD", "PHARMACY", "COSMETICS"}

SEASONS = ["summer", "winter", "fall", "spring", "holiday", "flat"]


def size_for(rng, category):
    if category in LIQUID_CATEGORIES:
        return rng.choice(LIQUID_SIZES)
    if category in SELF_NAMING - {"FRUIT", "BERRIES", "VEGETABLES", "HERBS"}:
        return rng.choice(COUNT_SIZES)          # non-food is sold by the unit
    if category in COUNT_CATEGORIES:
        return rng.choice(COUNT_SIZES + WEIGHT_SIZES[:4])
    return rng.choice(WEIGHT_SIZES)


def make_name(rng, category):
    """
    Build a plausible product description from generic parts.

    Returns (description, product_key). The key is the description without the
    size, which is how the real pipeline groups one product sold in several
    pack sizes under a single line in a movers report.
    """
    words = WORDS.get(category, WORDS["default"])
    parts = [rng.choice(words)]
    if rng.random() < 0.45:
        parts.append(rng.choice(WORDS["default"]))

    noun = "" if category in SELF_NAMING else category.title().replace(" And ", " ")

    # Drop adjacent repeats - "ORIGINAL ORIGINAL BRANDY" reads as a bug.
    seen, ordered = set(), []
    for p in parts + [noun]:
        if p and p.upper() not in seen:
            seen.add(p.upper())
            ordered.append(p)

    key = " ".join(ordered).upper()
    return f"{key} {size_for(rng, category)}", key


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--items", type=int, default=12000)
    ap.add_argument("--out", default="item_master.csv")
    args = ap.parse_args()

    rng = random.Random(SEED)
    rows = []
    serial = 1

    for dept_code, dept_name, share, m_lo, m_hi in DEPARTMENTS:
        n = max(1, int(args.items * share))
        cats = CATEGORIES[dept_code]
        for _ in range(n):
            cat_name, season, fs, wic, p_lo, p_hi = rng.choice(cats)
            cat_code = f"{dept_code}{cats.index((cat_name, season, fs, wic, p_lo, p_hi)) + 1}"

            # Prefix 2 is GS1 in-store use - never issued to a manufacturer, so
            # these cannot collide with a real product.
            upc = f"2{serial:011d}"
            serial += 1

            desc, product_key = make_name(rng, cat_name)
            # Prices cluster low with a long tail, the way a grocery shelf does.
            price = round(p_lo + (p_hi - p_lo) * (rng.random() ** 2.2), 2)
            margin = rng.uniform(m_lo, m_hi)
            cost = round(price * (1 - margin), 4)

            # Seasonality mostly follows the category, with some spread so the
            # data is not perfectly tidy.
            s = season if rng.random() < 0.75 else rng.choice(SEASONS)

            rows.append({
                "item_id": upc.zfill(14),
                "upc": upc,
                "item_desc": desc,
                "product_key": product_key,
                "pos_desc": desc[:22],
                "department_code": dept_code,
                "department_name": dept_name,
                "category_code": cat_code,
                "category_name": cat_name,
                "brand": rng.choice(BRANDS) if rng.random() < 0.55 else "",
                "vendor": rng.choice(VENDORS) if rng.random() < 0.7 else "",
                "unit_price": price,
                "unit_cost": cost,
                "cost_source": "synthetic",
                "margin_pct": round(margin * 100, 1),
                "food_stamp": "Y" if fs else "N",
                "wic": "Y" if wic else "N",
                "season": s,
                "popularity": 0.0,          # filled in below
                "observed_units_per_day": "",
                "demand_source": "synthetic",
            })

    # Zipf-Mandelbrot demand curve: weight = 1 / (rank + q) ** s.
    # Real grocery demand is steeply long-tailed - a few hundred items carry
    # most of the volume and the rest barely move. Without this the demo shows
    # a flat catalogue where every product sells about the same amount, which
    # is the single most obvious tell that data is fake.
    q, sexp = 6.0, 1.6
    rng.shuffle(rows)
    # Cheap items skew popular, but only partly - price is a weak signal.
    order = sorted(range(len(rows)),
                   key=lambda i: 0.28 * (1.0 / (rows[i]["unit_price"] + 1))
                                 + 0.72 * rng.random(),
                   reverse=True)
    weights = [1.0 / ((rank + q) ** sexp) for rank in range(len(rows))]
    total = sum(weights)
    for rank, idx in enumerate(order):
        rows[idx]["popularity"] = round(weights[rank] / total, 10)

    rows.sort(key=lambda r: r["item_id"])
    cols = list(rows[0].keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    top = sorted(rows, key=lambda r: -r["popularity"])[:100]
    print(f"wrote {args.out}: {len(rows):,} items across "
          f"{len(DEPARTMENTS)} departments")
    print(f"  top 100 items carry "
          f"{sum(r['popularity'] for r in top) * 100:.1f}% of expected volume")
    print(f"  price range ${min(r['unit_price'] for r in rows):.2f} - "
          f"${max(r['unit_price'] for r in rows):.2f}")
    print(f"  average margin {sum(r['margin_pct'] for r in rows)/len(rows):.1f}%")
    print("\nEvery barcode, name, brand, vendor and price here is invented.")


if __name__ == "__main__":
    main()
