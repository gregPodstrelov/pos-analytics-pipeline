#!/usr/bin/env python3
"""
RORC TLOG format - writer and parser.

Single source of truth for the transaction log layout described in
"TLOG Guide V2". Both the demo data generator and the parser import
from here, so the files we produce and the files we read are guaranteed
to agree on the format.

FORMAT SUMMARY (Long format, "L")
---------------------------------
Comma-separated values, one line per order line.
29 fixed fields, plus optional fields from position 30 onward.
Optional fields appear in a fixed order, skipping any that are disabled.

One file per transaction:
    YYYY-MM-DD_TransactionId_StoreCode.txt   (APPENDSTORENUM = Y)
    YYYY-MM-DD_TransactionId.txt             (APPENDSTORENUM = N)

Each file contains:
    - one line per item          (Sequence 1..n, KeyFunction "UPC" or "OPEN DEPT nnn")
    - one line per tender        (KeyFunction "TENDER")
    - one finalize record        (Sequence 0, KeyFunction empty, carries the totals)
"""

import csv as _csv
from datetime import datetime


# ---------------------------------------------------------------------------
# Fixed fields, positions 1-29
# ---------------------------------------------------------------------------

FIXED_FIELDS = [
    "Date",                  # 1   MM/DD/YYYY, quoted
    "TimeInSeconds",         # 2   seconds since midnight
    "TerminalCode",          # 3
    "CashierCode",           # 4
    "Sequence",              # 5   0 on the finalize record
    "KeyFunction",           # 6   quoted - UPC / OPEN DEPT nnn / TENDER / VOID / ...
    "KeyFunctionId",         # 7   quoted - UPC barcode, or media description on tenders
    "DepartmentCode",        # 8   0 on tenders
    "Multiple",              # 9
    "Retail",                # 10  unit price
    "MovementCount",         # 11  quantity, negative on voids/returns
    "MovementWeight",        # 12  0 for non-weighted items
    "ExtSales",              # 13  extended amount, negative on voids/discounts
    "TenderAmount",          # 14  0 on non-tender lines
    "AttributeFlag",         # 15  quoted - combination of W F X O T, prefixed R or C
    "TenderAccountNumber",   # 16  quoted - "PCI #" for card, empty for cash
    "Category",              # 17
    "SubCategory",           # 18
    "SubDepartment",         # 19
    "RetailType",            # 20  quoted - B T R S L E
    "LoyaltyDiscount",       # 21
    "PremiumDiscount",       # 22
    "LoyaltyCode",           # 23  quoted
    "SubTotal",              # 24  finalize record
    "TaxTotal",              # 25  finalize record
    "TotalAmount",           # 26  finalize record
    "TotalDiscount",         # 27  finalize record
    "TotalPoints",           # 28  finalize record
    "CashBackAmount",        # 29  finalize record
]

# Which fixed fields are quoted in the output
QUOTED_FIXED = {
    "Date", "KeyFunction", "KeyFunctionId", "AttributeFlag",
    "TenderAccountNumber", "RetailType", "LoyaltyCode",
}

# ---------------------------------------------------------------------------
# Optional fields, position 30+
# They appear in this order, skipping any whose APPEND variable is disabled.
# ---------------------------------------------------------------------------

OPTIONAL_FIELDS = [
    # (field name, controlling system variable, quoted?)
    ("TransactionCode", "APPENDTRNCD",         True),
    ("UnitCost",        "APPENDCOSTDEAL",      False),
    ("UnitDeal",        "APPENDCOSTDEAL",      False),
    ("CustomerInfo",    "APPENDCUSTID",        True),
    ("StoreCode",       "APPENDSTORENUM",      True),
    ("AlternateId",     "APPENDALTID",         True),
    ("PointsEarned",    "APPENDLYLPOINTS",     False),
    ("PointsRedeemed",  "APPENDLYLPOINTS",     False),
    ("POSDescription",  "APPENDDESCRIPTIONS",  True),
    ("ItemDescription", "APPENDDESCRIPTIONS",  True),
    ("DiscountCode",    "APPENDISCOUNTCODE",   True),
    ("PriceModified",   "APPENDPRICEMODIFIED", True),
]

# The APPEND configuration every script in this project reads and writes.
#
# This has to be defined exactly once. The optional fields are positional -
# they are appended in OPTIONAL_FIELDS order, skipping any whose variable is
# off - so a writer and a reader that disagree by a single flag misalign every
# field after it. That failure is quiet: the file still parses, the column
# count still looks plausible, and store codes silently arrive as product
# descriptions.
#
# These settings mirror a real RORC export: 36 fields, with cost and loyalty
# points on but descriptions off, which is why the pipeline has to join a
# price book to get item names at all.
DEFAULT_CONFIG = {
    "APPENDTRNCD":         True,   # default Y in RORC
    "APPENDCOSTDEAL":      True,   # UnitCost + UnitDeal -> margin analysis
    "APPENDCUSTID":        True,
    "APPENDSTORENUM":      True,   # StoreCode -> multi-store reporting
    "APPENDALTID":         False,
    "APPENDLYLPOINTS":     True,   # PointsEarned + PointsRedeemed
    "APPENDDESCRIPTIONS":  False,  # no item names in the log - join the price book
    "APPENDISCOUNTCODE":   False,
    "APPENDPRICEMODIFIED": False,
}

# 29 fixed + 7 optional. Asserted at import so a config edit that changes the
# layout fails immediately rather than corrupting a load.
EXPECTED_FIELD_COUNT = 36


def active_optional_fields(config=None):
    """Return the optional field names actually present, in output order."""
    cfg = config or DEFAULT_CONFIG
    return [name for name, var, _q in OPTIONAL_FIELDS if cfg.get(var)]


def _assert_layout():
    n = len(FIXED_FIELDS) + len(active_optional_fields())
    if n != EXPECTED_FIELD_COUNT:
        raise AssertionError(
            f"TLOG layout is {n} fields, expected {EXPECTED_FIELD_COUNT}. "
            "DEFAULT_CONFIG was changed without updating EXPECTED_FIELD_COUNT. "
            "Readers and writers must agree or every field after the change "
            "silently shifts.")


_assert_layout()


def field_positions(config=None):
    """Full ordered field list for the given configuration."""
    return FIXED_FIELDS + active_optional_fields(config)


# ---------------------------------------------------------------------------
# Attribute flags (field 15)
# ---------------------------------------------------------------------------
#   W = WIC eligible
#   F = Food stamp eligible
#   X = FSA/HSA Rx
#   O = FSA/HSA OTC
#   T = Taxable
#   R = Refund   (prefix)
#   C = Void     (prefix)

def build_attribute_flag(taxable=False, food_stamp=False, wic=False,
                         refund=False, void=False):
    prefix = ""
    if refund:
        prefix += "R"
    if void:
        prefix += "C"
    body = ""
    if wic:
        body += "W"
    if food_stamp:
        body += "F"
    if taxable:
        body += "T"
    return prefix + body


def parse_attribute_flag(flag):
    f = (flag or "").upper()
    return {
        "is_refund":     "R" in f,
        "is_void":       "C" in f,
        "is_wic":        "W" in f,
        "is_food_stamp": "F" in f,
        "is_taxable":    "T" in f,
    }


# ---------------------------------------------------------------------------
# Retail type (field 20)
# ---------------------------------------------------------------------------

RETAIL_TYPES = {
    "B": "Base retail",
    "T": "TPR (Temporary Price Reduction)",
    "R": "SPR (Special Price Reduction)",
    "S": "Sale price",
    "L": "Loyalty price",
    "E": "Electronic coupon price",
}


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _fmt(value, quoted):
    if value is None:
        value = ""
    if quoted:
        return f'"{value}"'
    if isinstance(value, float):
        # RORC writes plain decimals, not scientific notation
        return f"{value:.2f}".rstrip("0").rstrip(".") or "0"
    return str(value)


def build_line(values, config=None):
    """
    Build one TLOG line. `values` is a dict keyed by field name;
    anything missing is written as 0 (numeric) or empty (quoted).
    """
    cfg = config or DEFAULT_CONFIG
    out = []

    for name in FIXED_FIELDS:
        quoted = name in QUOTED_FIXED
        v = values.get(name, "" if quoted else 0)
        out.append(_fmt(v, quoted))

    for name, var, quoted in OPTIONAL_FIELDS:
        if not cfg.get(var):
            continue
        v = values.get(name, "" if quoted else 0)
        out.append(_fmt(v, quoted))

    return ",".join(out)


def build_filename(date, transaction_id, store_code=None, config=None):
    """
    YYYY-MM-DD_TransactionId_StoreCode.txt  when APPENDSTORENUM = Y
    YYYY-MM-DD_TransactionId.txt            otherwise
    """
    cfg = config or DEFAULT_CONFIG
    d = date.strftime("%Y-%m-%d")
    if cfg.get("APPENDSTORENUM") and store_code:
        return f"{d}_{transaction_id}_{store_code}.txt"
    return f"{d}_{transaction_id}.txt"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def split_csv(line):
    """
    Split a TLOG line on commas, honouring double quotes.

    Uses the stdlib csv reader (C implementation) for speed, since a full
    day of chain-wide TLOGs is millions of lines.
    """
    return next(_csv.reader([line.rstrip("\n")], quotechar='"', skipinitialspace=False))


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def parse_line(line, config=None):
    """Parse one TLOG line into a dict keyed by field name."""
    names = field_positions(config)
    parts = split_csv(line.strip())
    rec = {}
    for i, name in enumerate(names):
        rec[name] = parts[i] if i < len(parts) else ""
    return rec


def seconds_to_time(date_str, seconds):
    """
    Field 1 is MM/DD/YYYY and field 2 is seconds since midnight.
    Combine them into a real timestamp.
    """
    d = datetime.strptime(date_str, "%m/%d/%Y")
    s = int(_num(seconds))
    return d.replace(
        hour=min(s // 3600, 23),
        minute=(s % 3600) // 60,
        second=s % 60,
    )


def parse_transaction_file(path, config=None):
    """
    Parse one TLOG file (one transaction) into normalized rows.

    This is the logic that AWS Glue will run in production. It turns raw
    TLOG lines into the flat analytical records the warehouse holds.

    Returns (item_rows, finalize_row_or_None).
    """
    with open(path, "r") as f:
        lines = [ln for ln in f if ln.strip()]
    return parse_transaction_lines(lines, config)


def parse_transaction_lines(lines, config=None):
    items, finalize = [], None

    for line in lines:
        rec = parse_line(line, config)

        seq          = int(_num(rec.get("Sequence")))
        key_function = (rec.get("KeyFunction") or "").strip()

        # Sequence 0 with no key function is the finalize record - it carries
        # the transaction totals, not a product.
        if seq == 0 and not key_function:
            finalize = {
                "subtotal":       _num(rec.get("SubTotal")),
                "tax_total":      _num(rec.get("TaxTotal")),
                "total_amount":   _num(rec.get("TotalAmount")),
                "total_discount": _num(rec.get("TotalDiscount")),
                "total_points":   _num(rec.get("TotalPoints")),
                "cash_back":      _num(rec.get("CashBackAmount")),
                "loyalty_code":   rec.get("LoyaltyCode") or "",
            }
            continue

        flags = parse_attribute_flag(rec.get("AttributeFlag"))
        ts    = seconds_to_time(rec.get("Date"), rec.get("TimeInSeconds"))

        is_tender = key_function.upper().startswith("TENDER")
        is_void   = key_function.upper() == "VOID" or flags["is_void"]
        is_coupon = "COUPON" in key_function.upper()

        # Classify the line into an analytical transaction type
        if is_tender:
            txn_type = "TENDER"
        elif is_void:
            txn_type = "VOID"
        elif flags["is_refund"]:
            txn_type = "RETURN"
        elif is_coupon:
            txn_type = "COUPON"
        else:
            txn_type = "SALE"

        qty  = _num(rec.get("MovementCount"))
        ext  = _num(rec.get("ExtSales"))
        cost = _num(rec.get("UnitCost"))

        items.append({
            "transaction_code": rec.get("TransactionCode") or "",
            "store_code":       rec.get("StoreCode") or "",
            "transaction_date": ts,
            "terminal_code":    rec.get("TerminalCode") or "",
            "cashier_code":     rec.get("CashierCode") or "",
            "sequence":         seq,
            "key_function":     key_function,
            "item_id":          rec.get("KeyFunctionId") or "",
            "item_desc":        rec.get("ItemDescription") or "",
            "pos_desc":         rec.get("POSDescription") or "",
            "department_code":  rec.get("DepartmentCode") or "",
            "category":         rec.get("Category") or "",
            "sub_category":     rec.get("SubCategory") or "",
            "sub_department":   rec.get("SubDepartment") or "",
            "transaction_type": txn_type,
            "quantity":         qty,
            "weight":           _num(rec.get("MovementWeight")),
            "unit_price":       _num(rec.get("Retail")),
            "unit_cost":        cost,
            "extended_price":   ext,
            "gross_margin":     round(ext - (cost * qty), 2) if cost else 0.0,
            "retail_type":      rec.get("RetailType") or "",
            "attribute_flag":   rec.get("AttributeFlag") or "",
            "is_taxable":       int(flags["is_taxable"]),
            "is_food_stamp":    int(flags["is_food_stamp"]),
            "is_wic":           int(flags["is_wic"]),
            "is_loss_event":    int(txn_type in ("VOID", "RETURN")),
            "loyalty_discount": _num(rec.get("LoyaltyDiscount")),
            "premium_discount": _num(rec.get("PremiumDiscount")),
            "loyalty_code":     rec.get("LoyaltyCode") or "",
            "tender_amount":    _num(rec.get("TenderAmount")),
            "tender_type":      (rec.get("KeyFunctionId") or "").upper() if is_tender else "",
        })

    return items, finalize
