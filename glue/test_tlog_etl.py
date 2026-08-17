#!/usr/bin/env python3
"""
Tests for the ETL's parsing and reconciliation logic.

Runs without AWS. The point of the reconciliation tests is not that they pass
on good data - that is easy - but that they FAIL on corrupted data. A check
that cannot fail is not a check, and the only way to know is to break the data
on purpose and confirm it gets caught.

    python3 glue/test_tlog_etl.py
"""

import os
import sys
import copy

sys.argv = ["test", "--bucket", "test-bucket"]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tlog_etl as etl


PASS = FAIL = 0


def check(label, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}")


# ---------------------------------------------------------------------------
# A tiny TLOG file: two baskets, one with a void, one with a return.
# 36 fields, matching the client's APPEND configuration.
# ---------------------------------------------------------------------------

def line(seq, kf, kfid, dept, qty, ext, flag="", ttype="T1", store="0000000001",
         retail="0.00"):
    f = [""] * 36
    f[0] = "07/01/2026"; f[1] = "43200"; f[2] = "001"; f[3] = "07"
    f[4] = str(seq); f[5] = kf; f[6] = kfid; f[7] = dept
    f[8] = "1"; f[9] = retail; f[10] = str(qty); f[11] = "0"
    f[12] = str(ext); f[13] = "0.00"; f[14] = flag
    f[19] = "B"; f[20] = "0.00"; f[21] = "0.00"
    f[29] = ttype; f[30] = "0.00"; f[31] = "0.00"
    f[33] = store
    return ",".join(f)


SAMPLE = "\n".join([
    # basket 1 - two sales, one void, paid cash
    line(1, "UPC", "0000000012345", "112", 1, "3.99", "T", "T1"),
    line(2, "UPC", "0000000067890", "112", 2, "7.50", "TF", "T1"),
    line(3, "UPC", "0000000012345", "112", -1, "-3.99", "C", "T1"),
    line(4, "TENDER", "CASH", "", 0, "0.00", "", "T1"),
    line(0, "", "", "", 0, "0.00", "", "T1"),
    # basket 2 - an open department ring and a return, paid EBT FOOD
    line(1, "OPEN DEPT 237", "", "237", 1, "12.00", "T", "T2"),
    line(2, "UPC", "0000000067890", "112", -1, "-3.75", "R", "T2"),
    line(3, "TENDER", "EBT FOOD", "", 0, "0.00", "", "T2"),
    line(0, "", "", "", 0, "0.00", "", "T2"),
])

ITEMS = {
    "0000000012345": {"item_desc": "Test Bread", "product_key": "TEST BREAD",
                      "department_code": "11", "department_name": "Bread",
                      "category_name": "Bread", "vendor": "V1",
                      "unit_cost": 1.50, "cost_source": "book"},
    "12345": {"item_desc": "Test Bread", "product_key": "TEST BREAD",
              "department_code": "11", "department_name": "Bread",
              "category_name": "Bread", "vendor": "V1",
              "unit_cost": 1.50, "cost_source": "book"},
    "0000000067890": {"item_desc": "Test Milk", "product_key": "TEST MILK",
                      "department_code": "12", "department_name": "Dairy",
                      "category_name": "Milk", "vendor": "V2",
                      "unit_cost": 2.00, "cost_source": "book"},
    "67890": {"item_desc": "Test Milk", "product_key": "TEST MILK",
              "department_code": "12", "department_name": "Dairy",
              "category_name": "Milk", "vendor": "V2",
              "unit_cost": 2.00, "cost_source": "book"},
}

SUBDEPT = {"237": "22", "112": "11"}
DEPT_NAMES = {"22": "Produce", "11": "Bread", "12": "Dairy"}


print("\nparsing")
rows = etl.parse_file(SAMPLE, ITEMS, SUBDEPT, DEPT_NAMES)

check("tender and finalize records dropped", len(rows) == 5)
check("void classified", sum(1 for r in rows if r["transaction_type"] == "VOID") == 1)
check("return classified", sum(1 for r in rows if r["transaction_type"] == "RETURN") == 1)
check("sales classified", sum(1 for r in rows if r["transaction_type"] == "SALE") == 3)

print("\nkey_function carried through")
check("column present on every row", all("key_function" in r for r in rows))
check("in the parquet schema", "key_function" in [f.name for f in etl.SCHEMA])
kfs = {r["key_function"] for r in rows}
check("UPC rings preserved", "UPC" in kfs)
check("open department ring preserved", "OPEN DEPT 237" in kfs)
opens = [r for r in rows if r["key_function"].startswith("OPEN DEPT")]
check("open dept mapped to a real department, not phantom 237",
      opens and opens[0]["department_name"] == "Produce")

print("\ntender stamped onto every line in its basket")
b1 = [r for r in rows if r["transaction_id"] == "T1"]
b2 = [r for r in rows if r["transaction_id"] == "T2"]
check("basket 1 is cash", all(r["tender_type"] == "CASH" for r in b1))
check("basket 2 is EBT FOOD", all(r["tender_type"] == "EBT FOOD" for r in b2))

print("\nflags")
check("food stamp flag read", sum(r["is_food_stamp"] for r in rows) == 1)
# Three lines carry T: the two basket-1 sales and the open department ring.
# The void ("C") and the return ("R") do not.
check("taxable flag read", sum(r["is_taxable"] for r in rows) == 3)
check("voids and returns marked as loss events",
      sum(r["is_loss_event"] for r in rows) == 2)

print("\nmargin")
bread = [r for r in rows if r["item_desc"] == "Test Bread"
         and r["transaction_type"] == "SALE"][0]
check("margin is revenue minus cost times quantity",
      abs(bread["gross_margin"] - (3.99 - 1.50 * 1)) < 0.005)

print("\nreconciliation passes on good data")
item_rows = etl.build_item_rollup(rows)
dept_rows = etl.build_dept_rollup(rows)
check("no problems reported",
      etl.check_partition(rows, item_rows, dept_rows, "test") == [])

print("\nreconciliation catches corruption")

bad = copy.deepcopy(item_rows)
bad[0]["revenue"] += 0.05
check("a five cent revenue drift is caught",
      etl.check_partition(rows, bad, dept_rows, "test") != [])

bad = copy.deepcopy(item_rows)
bad[0]["line_count"] += 1
check("a duplicated line is caught",
      etl.check_partition(rows, bad, dept_rows, "test") != [])

bad = copy.deepcopy(dept_rows)[:-1]
check("a dropped rollup group is caught",
      etl.check_partition(rows, item_rows, bad, "test") != [])

bad = copy.deepcopy(item_rows)
bad[0]["revenue"] += 0.001
check("rounding noise below a cent is tolerated",
      etl.check_partition(rows, bad, dept_rows, "test") == [])

print("\npartition filter for the Athena check")
parts = [("0000000001", "2026", "7", "1"), ("0000000001", "2026", "8", "3"),
         ("0000000003", "2026", "7", "9")]
months = etl.months_touched(parts)
check("months deduplicated across stores", months == [(2026, 7), (2026, 8)])
f = etl.month_filter(months)
check("filter names both months",
      "year = 2026 AND month = 7" in f and "year = 2026 AND month = 8" in f)
check("filter is a disjunction", f.startswith("(") and " OR " in f)

print("\nthe raw SALE predicate mirrors classify()")
p = etl.RAW_SALE_PREDICATE.upper()
check("excludes tender", "TENDER%" in p)
check("excludes the finalize record", "TRIM(KEY_FUNCTION) = ''" in p)
check("excludes voids by flag", "'%C%'" in p)
check("excludes returns by flag", "'%R%'" in p)

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
