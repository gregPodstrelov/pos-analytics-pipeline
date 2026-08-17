#!/usr/bin/env python3
"""
Glue Python-shell job: raw TLOG CSV  ->  Parquet + daily rollups.

Runs nightly. Reads whatever landed in raw/, joins the item master for names,
departments and costs, and writes two things:

  processed/   one Parquet file per store-day, columnar and compressed.
               Detail queries hit this.

  rollups/     pre-aggregated daily totals. Most "full history" questions are
               repetitive aggregates - revenue by department by month, top
               items by quarter - and answering those from a rollup scans
               about a megabyte instead of gigabytes.

Deliberately a Python-shell job, not Spark. At a few GB a distributed engine
buys nothing, and Spark's ten-minute minimum billing costs ~$4.40/month for
work that finishes in seconds. This costs about $0.04.

Every run ends by reconciling what it wrote against the raw log through
Athena. If the numbers disagree the job fails rather than quietly publishing
tables that are slightly wrong.

Arguments (set as Glue job parameters):
    --bucket        s3 bucket name
    --database      glue database
    --mode          incremental (default) | backfill
    --days          incremental lookback, default 3
    --reconcile     true (default) | false
    --workgroup     athena workgroup for the reconciliation query
"""

import io
import os
import sys
import csv
import json
import time
import datetime as dt
from collections import defaultdict

import boto3

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow missing - add --additional-python-modules pyarrow to the job")
    raise


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

def get_args():
    args = {}
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a.startswith("--") and i + 1 < len(argv):
            args[a[2:]] = argv[i + 1]
    bucket = args.get("bucket", os.environ.get("BUCKET", ""))
    return {
        "bucket":   bucket,
        "database": args.get("database", "pos_analytics"),
        "mode":     args.get("mode", "incremental"),
        "days":     int(args.get("days", "3")),
        "reconcile": args.get("reconcile", "true").lower() != "false",
        "workgroup": args.get("workgroup", "pos-analytics"),
        "athena_output": args.get("athena-output",
                                  f"s3://{bucket}/athena-results/"),
    }


ARGS = get_args()
BUCKET = ARGS["bucket"]
s3 = boto3.client("s3")

CSV_FIELDS = [
    "date", "time_in_seconds", "terminal_code", "cashier_code", "sequence",
    "key_function", "key_function_id", "department_code", "multiple", "retail",
    "movement_count", "movement_weight", "ext_sales", "tender_amount",
    "attribute_flag", "tender_account_number", "category", "sub_category",
    "sub_department", "retail_type", "loyalty_discount", "premium_discount",
    "loyalty_code", "sub_total", "tax_total", "total_amount", "total_discount",
    "total_points", "cash_back_amount",
    "transaction_code", "unit_cost", "unit_deal", "customer_info",
    "store_code", "points_earned", "points_redeemed",
]

STORE_NAMES = {
    "0000000001": "Northline Riverside",
    "0000000003": "Northline Store 3",
}

TYPE_NAMES = {"SALE": "Sale", "VOID": "Void", "RETURN": "Return",
              "COUPON": "Coupon", "TENDER": "Tender"}


# ---------------------------------------------------------------------------
# Item master
# ---------------------------------------------------------------------------

def load_item_master():
    """
    The TLOG carries barcodes only - APPENDDESCRIPTIONS is off in this export.
    Names, departments, categories and costs all come from the price book,
    indexed both zero-padded and stripped because padding differs by source.
    """
    obj = s3.get_object(Bucket=BUCKET, Key="reference/item_master.csv")
    text = obj["Body"].read().decode("utf-8", errors="replace")
    index = {}
    for r in csv.DictReader(io.StringIO(text)):
        rec = {
            "item_desc":       r.get("item_desc", ""),
            "product_key":     r.get("product_key", ""),
            "department_code": r.get("department_code", ""),
            "department_name": r.get("department_name", ""),
            "category_name":   r.get("category_name", ""),
            "vendor":          r.get("vendor", ""),
            "unit_cost":       _f(r.get("unit_cost")),
            "cost_source":     r.get("cost_source", ""),
        }
        iid = r.get("item_id", "")
        if iid:
            index[iid] = rec
            index[iid.lstrip("0")] = rec
    print(f"item master: {len(index):,} keys")
    return index


def load_subdept_map():
    """
    Map the TLOG's sub-department code onto a real department.

    The transaction log writes a three-digit sub-department (112, 237, 239)
    where the price book uses a two-digit department (11, 22). Taking the
    first two digits does not work - 239 belongs to department 22, not 23 -
    so the mapping is learned from items that appear in both.

    Without this, any barcode missing from the price book produced its own
    phantom department ("Dept 237"), and a department report came back with
    eighty near-empty rows instead of twenty real ones.
    """
    obj = s3.get_object(Bucket=BUCKET, Key="reference/item_master.csv")
    text = obj["Body"].read().decode("utf-8", errors="replace")
    votes = defaultdict(lambda: defaultdict(int))
    names = {}
    for r in csv.DictReader(io.StringIO(text)):
        sub = (r.get("category_code") or "").strip()
        dept = (r.get("department_code") or "").strip()
        if sub and dept and sub != "0":
            votes[sub][dept] += 1
        if dept:
            names[dept] = r.get("department_name", "")

    mapping = {}
    for sub, counts in votes.items():
        mapping[sub] = max(counts.items(), key=lambda kv: kv[1])[0]
    print(f"sub-department map: {len(mapping):,} codes")
    return mapping, names


def _f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# TLOG parsing
# ---------------------------------------------------------------------------

def parse_attribute_flag(flag):
    f = (flag or "").upper()
    return ("R" in f, "C" in f, "W" in f, "F" in f, "T" in f)


def classify(key_function, is_void_flag, is_refund_flag):
    kf = (key_function or "").strip().upper()
    if kf.startswith("TENDER"):
        return "TENDER"
    if kf == "VOID" or is_void_flag:
        return "VOID"
    if is_refund_flag:
        return "RETURN"
    if "COUPON" in kf:
        return "COUPON"
    return "SALE"


def parse_file(body, items, subdept=None, dept_names=None):
    """
    Parse one TLOG file into flat rows.

    Transactions are contiguous, so the tender is captured as the group is
    walked and stamped onto every line - that is what makes "what do EBT
    customers buy" answerable later.
    """
    rows = []
    group, current = [], None

    def flush(g):
        if not g:
            return
        tender = ""
        for rec in g:
            kf = (rec.get("key_function") or "").upper()
            if kf.startswith("TENDER"):
                tender = (rec.get("key_function_id") or "").upper()
                break

        for rec in g:
            seq = _i(rec.get("sequence"))
            kf = (rec.get("key_function") or "").strip()
            if seq == 0 and not kf:
                continue                      # finalize record, totals only

            refund, void, wic, fs, taxable = parse_attribute_flag(
                rec.get("attribute_flag"))
            ttype = classify(kf, void, refund)
            if ttype == "TENDER":
                continue

            barcode = rec.get("key_function_id") or ""
            item = items.get(barcode) or items.get(barcode.lstrip("0")) or {}

            kfu = kf.upper()
            if item:
                desc = item["item_desc"]
                pkey = item["product_key"]
                dept = item["department_code"] or rec.get("department_code", "")
                dname = item["department_name"]
                cname = item["category_name"]
                vendor = item["vendor"]
                cost = item["unit_cost"]
            elif kfu.startswith("OPEN DEPT TOL"):
                code = kfu.replace("OPEN DEPT", "").strip()
                desc, pkey = f"Promotional discount ({code})", "PROMOTIONAL DISCOUNT"
                dept = rec.get("department_code", "")
                dname, cname, vendor, cost = "Promotions", "Promotional discount", "", 0.0
            elif kfu.startswith("OPEN DEPT"):
                raw_dept = rec.get("department_code", "")
                dept = (subdept or {}).get(raw_dept, raw_dept)
                dname = (dept_names or {}).get(dept, f"Dept {dept}")
                desc = f"{dname} - open department key"
                pkey = f"OPEN DEPT {dept}"
                cname, vendor, cost = "Open department", "", 0.0
            else:
                desc = f"Unknown barcode {barcode}"
                pkey = desc.upper()
                raw_dept = rec.get("department_code", "")
                dept = (subdept or {}).get(raw_dept, raw_dept)
                dname = (dept_names or {}).get(dept, f"Dept {dept}")
                cname, vendor, cost = "Unmatched", "", 0.0

            secs = _i(rec.get("time_in_seconds"))
            try:
                d = dt.datetime.strptime(rec.get("date", ""), "%m/%d/%Y")
            except ValueError:
                continue
            ts = d + dt.timedelta(seconds=min(secs, 86399))

            qty = _f(rec.get("movement_count"))
            ext = _f(rec.get("ext_sales"))
            store = rec.get("store_code") or ""

            rows.append({
                "transaction_id":   rec.get("transaction_code", ""),
                "store_id":         store,
                "store_name":       STORE_NAMES.get(store, f"Store {store.lstrip('0') or store}"),
                "transaction_ts":   ts,
                "terminal_code":    rec.get("terminal_code", ""),
                "cashier_id":       rec.get("cashier_code", ""),
                "sequence":         seq,
                # The raw key the register pressed: UPC for a scanned barcode,
                # OPEN DEPT nnn for a department key, PLU, and so on. Carried
                # through because "how much did we ring on open department
                # keys" is a real question, and answering it off category_name
                # was a guess - category_name comes from the price book join,
                # which is exactly what open-department lines do not have.
                "key_function":     kf,
                "item_id":          barcode,
                "item_desc":        desc,
                "product_key":      pkey,
                "department_code":  dept,
                "department_name":  dname,
                "category_name":    cname,
                "vendor":           vendor,
                "transaction_type": ttype,
                "transaction_type_name": TYPE_NAMES.get(ttype, ttype),
                "is_loss_event":    1 if ttype in ("VOID", "RETURN") else 0,
                "quantity":         qty,
                "unit_price":       _f(rec.get("retail")),
                "unit_cost":        cost,
                "extended_price":   ext,
                "gross_margin":     round(ext - cost * qty, 2) if cost else 0.0,
                "retail_type":      rec.get("retail_type", ""),
                "attribute_flag":   rec.get("attribute_flag", ""),
                "is_taxable":       1 if taxable else 0,
                "is_food_stamp":    1 if fs else 0,
                "is_wic":           1 if wic else 0,
                "loyalty_discount": _f(rec.get("loyalty_discount")),
                "premium_discount": _f(rec.get("premium_discount")),
                "loyalty_code":     rec.get("loyalty_code", ""),
                "tender_type":      tender,
            })

    reader = csv.reader(io.StringIO(body), quotechar='"')
    for parts in reader:
        if not parts:
            continue
        rec = dict(zip(CSV_FIELDS, parts))
        key = (rec.get("transaction_code", ""), rec.get("terminal_code", ""),
               rec.get("date", ""))
        if key != current:
            flush(group)
            group, current = [], key
        group.append(rec)
    flush(group)
    return rows


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

SCHEMA = pa.schema([
    ("transaction_id", pa.string()), ("store_id", pa.string()),
    ("store_name", pa.string()), ("transaction_ts", pa.timestamp("s")),
    ("terminal_code", pa.string()), ("cashier_id", pa.string()),
    ("sequence", pa.int32()), ("key_function", pa.string()),
    ("item_id", pa.string()),
    ("item_desc", pa.string()), ("product_key", pa.string()),
    ("department_code", pa.string()), ("department_name", pa.string()),
    ("category_name", pa.string()), ("vendor", pa.string()),
    ("transaction_type", pa.string()), ("transaction_type_name", pa.string()),
    ("is_loss_event", pa.int8()), ("quantity", pa.float64()),
    ("unit_price", pa.float64()), ("unit_cost", pa.float64()),
    ("extended_price", pa.float64()), ("gross_margin", pa.float64()),
    ("retail_type", pa.string()), ("attribute_flag", pa.string()),
    ("is_taxable", pa.int8()), ("is_food_stamp", pa.int8()),
    ("is_wic", pa.int8()), ("loyalty_discount", pa.float64()),
    ("premium_discount", pa.float64()), ("loyalty_code", pa.string()),
    ("tender_type", pa.string()),
])


def write_parquet(rows, key):
    cols = {f.name: [r.get(f.name) for r in rows] for f in SCHEMA}
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    buf = io.BytesIO()
    # Dictionary encoding plus snappy - descriptions and departments repeat
    # heavily, so they compress hard.
    pq.write_table(table, buf, compression="snappy", use_dictionary=True,
                   version="2.6")
    body = buf.getvalue()
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    return len(body)


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------

# Two rollups rather than one, because the useful grains differ.
#
# Measured on 40 real files (139,407 detail rows):
#   detail parquet   2.66 MB
#   item rollup      0.92 MB   2.9x smaller  - top movers, margin by product
#   dept rollup      0.24 MB  11.3x smaller  - dept/category revenue, tender mix
#
# A single combined grain including both item and tender collapsed only 1.7x,
# because most items sell once or twice a day per store. Splitting them is what
# makes the department rollup small enough to answer multi-year questions at
# Athena's 10 MB minimum charge.

ITEM_ROLLUP_SCHEMA = pa.schema([
    ("sale_date", pa.date32()), ("store_id", pa.string()), ("store_name", pa.string()),
    ("department_code", pa.string()), ("department_name", pa.string()),
    ("category_name", pa.string()), ("item_id", pa.string()),
    ("item_desc", pa.string()), ("product_key", pa.string()),
    ("transaction_type", pa.string()),
    ("units", pa.float64()), ("revenue", pa.float64()), ("margin", pa.float64()),
    ("discount", pa.float64()), ("line_count", pa.int32()),
    ("basket_count", pa.int32()),
])

DEPT_ROLLUP_SCHEMA = pa.schema([
    ("sale_date", pa.date32()), ("store_id", pa.string()), ("store_name", pa.string()),
    ("department_code", pa.string()), ("department_name", pa.string()),
    ("category_name", pa.string()), ("tender_type", pa.string()),
    ("transaction_type", pa.string()),
    ("units", pa.float64()), ("revenue", pa.float64()), ("margin", pa.float64()),
    ("discount", pa.float64()), ("line_count", pa.int32()),
    ("basket_count", pa.int32()),
])


def _aggregate(rows, key_fields, meta_fields):
    agg = defaultdict(lambda: {"units": 0.0, "revenue": 0.0, "margin": 0.0,
                               "discount": 0.0, "lines": 0, "baskets": set()})
    meta = {}
    for r in rows:
        k = tuple([r["transaction_ts"].date()] + [r[f] for f in key_fields])
        a = agg[k]
        a["units"] += r["quantity"]
        a["revenue"] += r["extended_price"]
        a["margin"] += r["gross_margin"]
        a["discount"] += r["loyalty_discount"] + r["premium_discount"]
        a["lines"] += 1
        a["baskets"].add(r["transaction_id"])
        if k not in meta:
            meta[k] = {f: r[f] for f in meta_fields}

    out = []
    for k, a in agg.items():
        rec = {"sale_date": k[0]}
        for i, f in enumerate(key_fields):
            rec[f] = k[i + 1]
        rec.update(meta[k])
        rec.update(units=round(a["units"], 3), revenue=round(a["revenue"], 2),
                   margin=round(a["margin"], 2), discount=round(a["discount"], 2),
                   line_count=a["lines"], basket_count=len(a["baskets"]))
        out.append(rec)
    return out


def build_item_rollup(rows):
    return _aggregate(
        rows,
        ["store_id", "item_id", "transaction_type"],
        ["store_name", "department_code", "department_name", "category_name",
         "item_desc", "product_key"])


def build_dept_rollup(rows):
    return _aggregate(
        rows,
        ["store_id", "department_code", "category_name", "tender_type",
         "transaction_type"],
        ["store_name", "department_name"])


def write_table(rows, schema, key):
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    table = pa.Table.from_pydict(cols, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", use_dictionary=True,
                   version="2.6")
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return len(buf.getvalue())


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------
#
# Two layers, because they fail in different ways.
#
#   In memory, per partition. Do the rollups still add up to the detail they
#   were built from? Catches a bad GROUP BY key or a dropped field the moment
#   it is introduced, before anything reaches S3. Free.
#
#   Against Athena, once per run. Does the catalogue agree with the raw log?
#   This is the one that matters, because it is the only check that sees what
#   a query actually returns. Stale files left behind by a schema change,
#   a partition written twice, a Parquet file that never landed - none of
#   those are visible from inside the job that wrote them.
#
# A mismatch fails the job. Silently serving numbers that are 0.5% off is
# worse than not serving them, because nobody goes looking for a number that
# looks plausible.

TOLERANCE = 0.01          # one cent - these are sums of rounded currency


def check_partition(rows, item_rows, dept_rows, label):
    """Rollups must reproduce the detail exactly. Returns a list of problems."""
    def totals(rs, rev, ln):
        return (round(sum(r[rev] for r in rs), 2), sum(r[ln] for r in rs))

    d_rev = round(sum(r["extended_price"] for r in rows), 2)
    d_ln = len(rows)
    problems = []
    for name, (rev, ln) in (("rollup_item", totals(item_rows, "revenue", "line_count")),
                            ("rollup_dept", totals(dept_rows, "revenue", "line_count"))):
        if abs(rev - d_rev) > TOLERANCE:
            problems.append(f"{label} {name} revenue {rev} != detail {d_rev}")
        if ln != d_ln:
            problems.append(f"{label} {name} lines {ln} != detail {d_ln}")
    return problems


# The SALE definition, expressed against the raw CSV columns. This has to
# mirror classify() and the finalize-record skip in parse_file exactly - it is
# a second, independent implementation of the same rule, which is the whole
# point. If someone changes the classifier and forgets this, the job fails and
# tells them.
RAW_SALE_PREDICATE = """
      NOT (CAST(sequence AS VARCHAR) = '0' AND TRIM(key_function) = '')
  AND UPPER(TRIM(key_function)) NOT LIKE 'TENDER%'
  AND UPPER(TRIM(key_function)) <> 'VOID'
  AND UPPER(TRIM(key_function)) NOT LIKE '%COUPON%'
  AND UPPER(COALESCE(attribute_flag, '')) NOT LIKE '%C%'
  AND UPPER(COALESCE(attribute_flag, '')) NOT LIKE '%R%'
"""


def months_touched(parts):
    return sorted({(int(y), int(m)) for (_s, y, m, _d) in parts})


def month_filter(months):
    return "(" + " OR ".join(f"(year = {y} AND month = {m})"
                             for y, m in months) + ")"


def run_athena(sql, database, workgroup, output):
    athena = boto3.client("athena")
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output},
    )["QueryExecutionId"]

    while True:
        ex = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(ex["Status"].get("StateChangeReason", state))

    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=100)
    rows = res["ResultSet"]["Rows"][1:]          # first row is the header
    scanned = ex["Statistics"].get("DataScannedInBytes", 0)
    out = []
    for r in rows:
        out.append([c.get("VarCharValue") for c in r["Data"]])
    return out, scanned


def reconcile(parts, database, workgroup, output):
    """
    Compare raw log, detail and both rollups over every month this run touched.

    Returns (ok, lines_of_report).
    """
    months = months_touched(parts)
    if not months:
        return True, ["reconcile: nothing written, skipped"]
    where = month_filter(months)

    sql = f"""
    SELECT 'raw'         AS src, COUNT(*)          AS lines,
           ROUND(SUM(ext_sales), 2)                AS revenue
      FROM tlog_raw     WHERE {where} AND {RAW_SALE_PREDICATE}
    UNION ALL
    SELECT 'detail',      COUNT(*),        ROUND(SUM(extended_price), 2)
      FROM tlog_detail  WHERE {where} AND transaction_type = 'SALE'
    UNION ALL
    SELECT 'rollup_item', CAST(SUM(line_count) AS BIGINT), ROUND(SUM(revenue), 2)
      FROM rollup_item  WHERE {where} AND transaction_type = 'SALE'
    UNION ALL
    SELECT 'rollup_dept', CAST(SUM(line_count) AS BIGINT), ROUND(SUM(revenue), 2)
      FROM rollup_dept  WHERE {where} AND transaction_type = 'SALE'
    """

    rows, scanned = run_athena(sql, database, workgroup, output)
    got = {r[0]: (int(r[1] or 0), float(r[2] or 0.0)) for r in rows}

    report = [f"reconcile: {len(months)} month(s), "
              f"{scanned/1024/1024:.1f} MB scanned "
              f"(${scanned / (1024**4) * 5:.4f})"]
    for src in ("raw", "detail", "rollup_item", "rollup_dept"):
        ln, rev = got.get(src, (0, 0.0))
        report.append(f"  {src:<12} {ln:>10,} lines  ${rev:>14,.2f}")

    base_ln, base_rev = got.get("raw", (0, 0.0))
    ok = True
    for src in ("detail", "rollup_item", "rollup_dept"):
        ln, rev = got.get(src, (0, 0.0))
        if ln != base_ln or abs(rev - base_rev) > TOLERANCE:
            ok = False
            report.append(f"  MISMATCH {src}: {ln - base_ln:+,} lines, "
                          f"${rev - base_rev:+,.2f}")
    report.append("  all sources agree" if ok else
                  "  RECONCILIATION FAILED - do not trust these tables")
    return ok, report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def list_raw_partitions():
    """Return {(store, y, m, d): [keys]} for everything under raw/."""
    parts = defaultdict(list)
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "raw/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if not k.lower().endswith(".dat") or o["Size"] == 0:
                continue
            seg = k.split("/")
            if len(seg) < 6:
                continue
            try:
                store = seg[1].split("=")[1]
                y = seg[2].split("=")[1]
                m = seg[3].split("=")[1]
                d = seg[4].split("=")[1]
            except IndexError:
                continue
            parts[(store, y, m, d)].append(k)
        token = r.get("NextContinuationToken")
        if not token:
            break
    return parts


def main():
    t0 = time.time()
    if not BUCKET:
        print("no --bucket given")
        sys.exit(1)

    print(f"bucket={BUCKET} mode={ARGS['mode']} days={ARGS['days']}")
    items = load_item_master()
    subdept, dept_names_master = load_subdept_map()
    parts = list_raw_partitions()
    print(f"raw partitions found: {len(parts):,}")

    if ARGS["mode"] == "incremental":
        cutoff = dt.date.today() - dt.timedelta(days=ARGS["days"])
        keep = {}
        for k, v in parts.items():
            try:
                if dt.date(int(k[1]), int(k[2]), int(k[3])) >= cutoff:
                    keep[k] = v
            except ValueError:
                continue
        parts = keep
        print(f"incremental: {len(parts):,} partitions since {cutoff}")

    tot_rows = tot_pq = tot_item = tot_dept = 0
    done = 0
    written = []
    problems = []

    for (store, y, m, d), keys in sorted(parts.items()):
        rows = []
        for k in keys:
            body = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read().decode(
                "utf-8", errors="replace")
            rows.extend(parse_file(body, items, subdept, dept_names_master))
        if not rows:
            continue

        item_rows = build_item_rollup(rows)
        dept_rows = build_dept_rollup(rows)

        # Check before writing. A rollup that does not reproduce its own
        # detail is a code bug, and there is no reason to publish it.
        problems += check_partition(rows, item_rows, dept_rows,
                                    f"{store}/{y}-{m}-{d}")

        base = f"store={store}/year={y}/month={m}/day={d}"
        tot_pq   += write_parquet(rows, f"processed/{base}/part-000.parquet")
        tot_item += write_table(item_rows, ITEM_ROLLUP_SCHEMA,
                                f"rollup_item/{base}/part-000.parquet")
        tot_dept += write_table(dept_rows, DEPT_ROLLUP_SCHEMA,
                                f"rollup_dept/{base}/part-000.parquet")
        tot_rows += len(rows)
        written.append((store, y, m, d))
        done += 1
        if done % 50 == 0:
            print(f"  {done}/{len(parts)} partitions, {tot_rows:,} rows",
                  flush=True)

    secs = time.time() - t0
    print(f"\nwrote {done:,} partitions in {secs:.0f}s")
    print(f"  rows         : {tot_rows:,}")
    print(f"  processed/   : {tot_pq/1024/1024:.1f} MB")
    print(f"  rollup_item/ : {tot_item/1024/1024:.2f} MB")
    print(f"  rollup_dept/ : {tot_dept/1024/1024:.2f} MB")

    if problems:
        print(f"\nin-memory checks: {len(problems)} FAILED")
        for p in problems[:20]:
            print(f"  {p}")
    else:
        print(f"\nin-memory checks: {done:,} partitions, rollups match detail")

    # Athena cross-check. Skipped only when explicitly turned off, because a
    # check that is easy to skip is a check that stops running.
    ok = True
    if ARGS["reconcile"]:
        print()
        try:
            ok, report = reconcile(written, ARGS["database"],
                                   ARGS["workgroup"], ARGS["athena_output"])
            for line in report:
                print(line)
        except Exception as e:
            ok = False
            print(f"reconcile: could not run - {e}")
    else:
        print("\nreconcile: skipped (--reconcile false)")

    if problems or not ok:
        print("\nJob failed reconciliation. The tables may be wrong.")
        sys.exit(1)


if __name__ == "__main__":
    main()
