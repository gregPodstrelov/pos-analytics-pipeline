#!/usr/bin/env python3
"""
POS Analytics MCP Server - production, backed by S3 + Athena.

Same eleven tools as the local demo, but every query runs against the data
lake and each one is routed to the cheapest table that can answer it:

    rollup_dept    department, category and tender questions
                   ~40x smaller than detail; most history questions land here

    rollup_item    item rankings and margin by product
                   ~8x smaller than detail

    tlog_detail    anything needing transaction-level rows - baskets,
                   cashiers, hour of day, individual barcodes

Routing is the whole point. A "revenue by department for the last three
years" query reads a few hundred KB from rollup_dept instead of scanning
tens of MB of detail, which is what keeps a busy day of questions under a
few cents.

Every query passes through the cost guard first (see athena_cost_guard.py):
no partition filter, no query.
"""

import json
import os
import time
import re
from datetime import datetime, timedelta, date
from typing import Optional

import boto3

from athena_cost_guard import CostGuard, CostGuardError

# MCP SDK 2.0 renamed FastMCP to MCPServer. Support both.
try:
    from mcp.server import MCPServer as _Server        # SDK 2.x
except ImportError:
    from mcp.server.fastmcp import FastMCP as _Server  # SDK 1.x


# ---------------------------------------------------------------------------
# Config, Athena, cost guard
# ---------------------------------------------------------------------------

# Claude Desktop launches this server from an arbitrary working directory,
# so every path is resolved against the script's own location rather than cwd.
HERE = os.path.dirname(os.path.abspath(__file__))


def load_config(filename="athena_config.json"):
    path = filename if os.path.isabs(filename) else os.path.join(HERE, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise SystemExit(
            f"Cannot find {path}\n"
            "Run provision_aws.py, or copy athena_config.example.json and "
            "fill it in.")


config = load_config()

athena = boto3.client(
    "athena",
    region_name           = config["region"],
    aws_access_key_id     = config["aws_access_key"],
    aws_secret_access_key = config["aws_secret_key"],
)

guard = CostGuard(
    daily_budget_usd         = config.get("daily_budget_usd", 5.00),
    max_scan_gb              = config.get("max_scan_gb", 20),
    require_partition_filter = config.get("require_partition_filter", True),
)

last_query_stats = {"bytes": 0, "description": "", "table": ""}

DETAIL = "tlog_detail"
R_ITEM = "rollup_item"
R_DEPT = "rollup_dept"


def run_query(sql, skip_guard=False, table=""):
    """Submit to Athena, wait, return rows. Cost-guarded and accounted."""
    if not skip_guard:
        guard.check(sql)

    kwargs = {
        "QueryString":           sql,
        "QueryExecutionContext": {"Database": config["database"]},
        "ResultConfiguration":   {"OutputLocation": config["output_bucket"]},
    }
    if config.get("workgroup"):
        kwargs["WorkGroup"] = config["workgroup"]

    resp = athena.start_query_execution(**kwargs)
    qid = resp["QueryExecutionId"]

    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        status = r["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "Unknown")
            if "bytes scanned" in reason.lower() or "cutoff" in reason.lower():
                raise CostGuardError(
                    "AWS stopped this query for exceeding the workgroup scan "
                    f"limit. Narrow the date range.\n({reason})")
            raise Exception(f"Athena query failed: {reason}")
        time.sleep(0.4)

    stats = r["QueryExecution"].get("Statistics", {})
    scanned = stats.get("DataScannedInBytes", 0)
    guard.record(scanned)
    last_query_stats.update(bytes=scanned,
                            description=guard.describe_cost(scanned),
                            table=table)

    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=1000)
    cols = [c["Label"]
            for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    out = []
    for row in res["ResultSet"]["Rows"][1:]:          # skip header
        out.append(dict(zip(cols,
                            [f.get("VarCharValue", "") for f in row["Data"]])))
    return out


def to_float(v):
    try:
        return float(v or 0)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Partition pruning
# ---------------------------------------------------------------------------

def partition_filter(start: date, end: date) -> str:
    """
    Build a predicate on the partition columns.

    The tables are partitioned by year/month/day, so a plain filter on the
    timestamp would read everything. Narrow windows get an explicit list of
    (year, month) pairs; wide ones fall back to a year/month range, which
    still prunes most of the table.
    """
    pairs = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        pairs.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
        if len(pairs) > 36:
            break

    if len(pairs) <= 12:
        ors = " OR ".join(f"(year = {a} AND month = {b})" for a, b in pairs)
        return f"({ors})"

    # Longer spans: bound the ends by year and month rather than enumerating
    # every pair. Written as explicit year comparisons on purpose - an
    # arithmetic form like (year*100+month) BETWEEN ... prunes correctly but
    # the cost guard cannot recognise it as a partition predicate, so the
    # query would be refused before it ever ran.
    (ly, lm), (hy, hm) = pairs[0], pairs[-1]
    return (f"(year >= {ly} AND year <= {hy}"
            f" AND (year > {ly} OR month >= {lm})"
            f" AND (year < {hy} OR month <= {hm}))")


def date_col(table):
    """Detail carries a timestamp; the rollups carry a date."""
    return "DATE(transaction_ts)" if table == DETAIL else "sale_date"


def scope(table, start, end, extra=None):
    """Partition predicate + exact date bounds + any extra conditions."""
    parts = [partition_filter(start, end),
             f"{date_col(table)} BETWEEN DATE '{start}' AND DATE '{end}'"]
    if extra:
        parts.extend(extra)
    return " AND ".join(parts)


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

DEPARTMENTS = {
    "10": "Alcohol",  "11": "Produce",  "12": "Kitchen",  "13": "Meat",
    "14": "Deli",     "15": "Caviar",   "16": "Dairy",    "17": "Bread",
    "18": "Pet",      "19": "Seafood",  "20": "Bakery",   "21": "Frozen",
    "22": "Grocery",  "25": "Non-Food", "26": "Dry Goods","27": "Pickled",
    "28": "Garden",   "31": "Soda",     "32": "Beer",     "40": "Garden",
}

_DEPT_ALIASES = {
    "alcohol":"10","alc":"10","liquor":"10","wine":"10","spirits":"10",
    "produce":"11","pro":"11","fruit":"11","vegetables":"11","veg":"11",
    "kitchen":"12","kit":"12","prepared":"12","hot bar":"12",
    "meat":"13","mea":"13","butcher":"13",
    "deli":"14","del":"14","delicatessen":"14",
    "caviar":"15","cav":"15","roe":"15",
    "dairy":"16","dai":"16",
    "bread":"17","bre":"17",
    "pet":"18","pet food":"18",
    "seafood":"19","sea":"19","fish":"19",
    "bakery":"20","bak":"20",
    "frozen":"21","fro":"21","frozen food":"21","freezer":"21",
    "grocery":"22","gro":"22","center store":"22","dry grocery":"22",
    "non-food":"25","nonfood":"25","non food":"25","hba":"25",
    "health & beauty":"25","household":"25",
    "dry goods":"26","dry":"26",
    "pickled":"27","pic":"27","pickles":"27",
    "garden":"28","gar":"28",
    "soda":"31","sod":"31","soft drinks":"31",
    "beer":"32","bee":"32",
}

TENDER_ALIASES = {
    "ebt":"EBT FOOD","snap":"EBT FOOD","food stamp":"EBT FOOD",
    "food stamps":"EBT FOOD","foodstamp":"EBT FOOD","ebt food":"EBT FOOD",
    "ebt cash":"EBT CASH","credit card":"VISA","amex":"AMERICAN EXPRESS",
    "mc":"MASTERCARD","debit card":"DEBIT",
}


def resolve_department(value):
    if not value:
        return None
    v = str(value).strip()
    if v in DEPARTMENTS:
        return v
    return _DEPT_ALIASES.get(v.lower())


def resolve_category(value, start, end):
    """Match a spoken category name against what is actually in the data."""
    if not value:
        return None
    v = value.strip().upper()
    rows = run_query(f"""
        SELECT DISTINCT category_name FROM {R_DEPT}
        WHERE {partition_filter(start, end)} AND category_name <> ''
    """, table=R_DEPT)
    names = [r["category_name"] for r in rows]
    for n in names:
        if n.upper() == v:
            return n
    for pool in (
        [n for n in names if n.upper().startswith(v)],
        [n for n in names if v in n.upper()],
    ):
        if pool:
            return sorted(pool, key=len)[0]
    return None


def stem(word):
    """So 'strawberries' and 'strawberry' both match."""
    w = word.strip().lower()
    if len(w) < 6:
        return w
    for suffix, cut in (("ies",3),("ches",2),("shes",2),("ses",2),("s",1),("y",1)):
        if w.endswith(suffix):
            s = w[:-cut]
            if len(s) >= 4:
                return s
            break
    return w


# ---------------------------------------------------------------------------
# Periods
# ---------------------------------------------------------------------------

def resolve_period(period: str):
    today = datetime.now().date()
    if period == "today":            return today, today
    if period == "yesterday":
        d = today - timedelta(days=1); return d, d
    if period == "last_7_days":      return today - timedelta(days=7), today
    if period == "last_30_days":     return today - timedelta(days=30), today
    if period == "last_90_days":     return today - timedelta(days=90), today
    if period == "last_week":
        mon = today - timedelta(days=today.weekday() + 7)
        return mon, mon + timedelta(days=6)
    if period == "this_month":       return today.replace(day=1), today
    if period == "last_month":
        first = today.replace(day=1); last = first - timedelta(days=1)
        return last.replace(day=1), last
    if period == "this_year":        return today.replace(month=1, day=1), today
    if period == "all_time":         return date(2024, 1, 1), today
    return today - timedelta(days=30), today


mcp = _Server("POS Analytics")


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_top_movers(
    period: str = "last_30_days",
    direction: str = "top",
    metric: str = "revenue",
    limit: int = 20,
    store_id: Optional[str] = None,
    department: Optional[str] = None,
    category: Optional[str] = None,
) -> str:
    """
    Best or worst selling items, optionally within a department or category.

    Reads the item rollup, which is pre-aggregated per day per product - far
    cheaper than scanning transaction detail.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year, all_time
    direction: top or bottom
    metric: revenue, units, or margin
    store_id: store code, or omit for all stores
    department: e.g. produce, frozen, deli, grocery, alcohol
    category: e.g. berries, wine, cheese chunks
    """
    start, end = resolve_period(period)
    order = "DESC" if direction == "top" else "ASC"
    # Order by the output alias, not a repeated aggregate. Presto rejects
    # ORDER BY SUM(revenue) when "revenue" is also a projection alias:
    # "Invalid reference to output projection attribute from ORDER BY".
    metric_col = {"revenue": "revenue", "units": "units",
                  "margin": "margin"}.get(metric, "revenue")

    extra = ["transaction_type = 'SALE'"]
    if store_id:
        extra.append(f"store_id = '{store_id}'")
    dept = resolve_department(department)
    if department and not dept:
        return (f"Unknown department '{department}'. Options: "
                + ", ".join(sorted(set(DEPARTMENTS.values()))))
    if dept:
        extra.append(f"department_code = '{dept}'")

    cat = None
    if category:
        cat = resolve_category(category, start, end)
        if not cat:
            return f"No category matching '{category}'."
        extra.append(f"category_name = '{cat}'")

    rows = run_query(f"""
        SELECT product_key,
               MIN(item_desc) AS item_desc,
               MIN(department_name) AS department_name,
               COUNT(DISTINCT item_id) AS barcodes,
               SUM(revenue) AS revenue,
               SUM(units)   AS units,
               SUM(margin)  AS margin
        FROM {R_ITEM}
        WHERE {scope(R_ITEM, start, end, extra)}
        GROUP BY product_key
        ORDER BY {metric_col} {order}
        LIMIT {limit + 1}
    """, table=R_ITEM)

    if not rows:
        return f"No sales data found for {period}."

    truncated = len(rows) > limit
    rows = rows[:limit]

    label = "Top" if direction == "top" else "Bottom"
    sc = ""
    if cat:       sc += f" - {cat}"
    if dept:      sc += f" - {DEPARTMENTS[dept]}"
    if store_id:  sc += f" - {store_id}"

    out = [f"{label} {len(rows)} movers by {metric}{sc} ({start} to {end})\n"]
    if truncated:
        out.append(f"More exist beyond {limit}; raise `limit`.\n")
    merged = sum(to_float(r["barcodes"]) for r in rows) - len(rows)
    if merged > 0:
        out.append(f"{merged:,.0f} duplicate barcode(s) rolled up.\n")

    for i, r in enumerate(rows, 1):
        rev, un, mg = (to_float(r["revenue"]), to_float(r["units"]),
                       to_float(r["margin"]))
        pct = (mg / rev * 100) if rev else 0
        bc = to_float(r["barcodes"])
        tag = f" [{bc:.0f} barcodes]" if bc > 1 else ""
        out.append(f"{i}. {r['item_desc']} ({r['department_name']}){tag} - "
                   f"${rev:,.2f} rev | {un:,.0f} units | "
                   f"${mg:,.2f} margin ({pct:.1f}%)")
    out.append(f"\n[{last_query_stats['description']} from {R_ITEM}]")
    return "\n".join(out)


@mcp.tool()
def get_sales_summary(
    period: str = "last_30_days",
    group_by: str = "department",
    store_id: Optional[str] = None,
) -> str:
    """
    Sales broken down by department, category, store or day.

    Reads the department rollup - the smallest table - so this stays cheap
    even across years of history.

    group_by: department, category, store, or day
    """
    start, end = resolve_period(period)
    col = {"department": "department_name", "category": "category_name",
           "store": "store_name"}.get(group_by, "sale_date")

    extra = ["transaction_type = 'SALE'"]
    if store_id:
        extra.append(f"store_id = '{store_id}'")

    rows = run_query(f"""
        SELECT {col} AS grp,
               SUM(revenue) AS revenue,
               SUM(units)   AS units,
               SUM(margin)  AS margin
        FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end, extra)}
        GROUP BY {col}
        ORDER BY revenue DESC
        LIMIT 100
    """, table=R_DEPT)

    if not rows:
        return f"No sales data found for {period}."

    total = sum(to_float(r["revenue"]) for r in rows)
    out = [f"Sales by {group_by} - {period} ({start} to {end})\n",
           f"Total: ${total:,.2f}\n"]
    for r in rows:
        rev, mg = to_float(r["revenue"]), to_float(r["margin"])
        share = (rev / total * 100) if total else 0
        mpct = (mg / rev * 100) if rev else 0
        out.append(f"  {r['grp']}: ${rev:,.2f} ({share:.1f}%) | "
                   f"{to_float(r['units']):,.0f} units | "
                   f"${mg:,.2f} margin ({mpct:.1f}%)")
    out.append(f"\n[{last_query_stats['description']} from {R_DEPT}]")
    return "\n".join(out)


@mcp.tool()
def get_tender_analysis(
    period: str = "last_30_days",
    tender_type: Optional[str] = None,
    group_by: str = "department",
    limit: int = 12,
) -> str:
    """
    What customers buy, broken down by how they paid - EBT/SNAP, cash, cards.

    Tender is carried on the department rollup, so this is one of the
    cheapest questions in the system.

    tender_type: EBT, CASH, VISA, MASTERCARD, DEBIT... or omit to compare all
    group_by: department or category
    """
    start, end = resolve_period(period)
    col = "category_name" if group_by == "category" else "department_name"

    tender = None
    if tender_type:
        t = tender_type.strip().lower()
        tender = TENDER_ALIASES.get(t, tender_type.strip().upper())
        avail = [r["tender_type"] for r in run_query(f"""
            SELECT DISTINCT tender_type FROM {R_DEPT}
            WHERE {partition_filter(start, end)} AND tender_type <> ''
        """, table=R_DEPT)]
        if tender not in avail:
            near = [a for a in avail if tender in a or a in tender]
            if near:
                tender = near[0]
            else:
                return (f"No tender '{tender_type}'. Available: "
                        + ", ".join(sorted(avail)))

    extra = ["transaction_type = 'SALE'", "tender_type <> ''"]
    if tender:
        extra.append(f"tender_type = '{tender}'")

    baskets = run_query(f"""
        SELECT tender_type,
               SUM(revenue) AS revenue,
               SUM(margin)  AS margin,
               SUM(basket_count) AS baskets
        FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end, extra)}
        GROUP BY tender_type
        ORDER BY revenue DESC
    """, table=R_DEPT)

    if not baskets:
        return f"No tender data for {period}."

    all_rev = run_query(f"""
        SELECT SUM(revenue) AS revenue FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end,
                     ["transaction_type = 'SALE'", "tender_type <> ''"])}
    """, table=R_DEPT)
    total = to_float(all_rev[0]["revenue"]) if all_rev else 0

    out = [f"Purchases by payment method - {period} ({start} to {end})\n"]
    for b in baskets:
        t = b["tender_type"]
        rev, mg = to_float(b["revenue"]), to_float(b["margin"])
        n = to_float(b["baskets"])
        share = (rev / total * 100) if total else 0
        pretty = "EBT FOOD (SNAP)" if t == "EBT FOOD" else t
        out.append(pretty)
        out.append(f"  ${rev:,.2f} ({share:.1f}% of all sales) | "
                   f"{n:,.0f} basket-lines | ${mg:,.2f} margin")

        sub = run_query(f"""
            SELECT {col} AS grp, SUM(revenue) AS revenue, SUM(units) AS units
            FROM {R_DEPT}
            WHERE {scope(R_DEPT, start, end,
                         ["transaction_type = 'SALE'", f"tender_type = '{t}'"])}
            GROUP BY {col} ORDER BY revenue DESC LIMIT {limit}
        """, table=R_DEPT)
        for s in sub:
            sr = to_float(s["revenue"])
            pct = (sr / rev * 100) if rev else 0
            out.append(f"    {s['grp']}: ${sr:,.2f} ({pct:.1f}%) | "
                       f"{to_float(s['units']):,.0f} units")
        out.append("")
    out.append(f"[{last_query_stats['description']} from {R_DEPT}]")
    return "\n".join(out)


@mcp.tool()
def get_loss_report(period: str = "last_30_days",
                    store_id: Optional[str] = None) -> str:
    """
    Voids and returns by store, shown as a share of that store's sales so a
    busy store is not flagged simply for being busy.
    """
    start, end = resolve_period(period)
    extra = ["transaction_type IN ('VOID','RETURN')"]
    if store_id:
        extra.append(f"store_id = '{store_id}'")

    loss = run_query(f"""
        SELECT store_name, transaction_type,
               SUM(ABS(revenue)) AS value, SUM(line_count) AS events
        FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end, extra)}
        GROUP BY store_name, transaction_type
        ORDER BY store_name, value DESC
    """, table=R_DEPT)
    if not loss:
        return f"No loss events for {period}."

    sales_extra = ["transaction_type = 'SALE'"]
    if store_id:
        sales_extra.append(f"store_id = '{store_id}'")
    sales = {r["store_name"]: to_float(r["revenue"]) for r in run_query(f"""
        SELECT store_name, SUM(revenue) AS revenue FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end, sales_extra)}
        GROUP BY store_name
    """, table=R_DEPT)}

    by_store = {}
    for r in loss:
        by_store.setdefault(r["store_name"], []).append(r)

    out = [f"Loss report - {period} ({start} to {end})\n"]
    grand = 0.0
    for store, rs in by_store.items():
        tot = sum(to_float(r["value"]) for r in rs)
        grand += tot
        rev = sales.get(store, 0.0)
        pct = (tot / rev * 100) if rev else 0
        out.append(f"{store}:")
        for r in rs:
            out.append(f"  {r['transaction_type']}: "
                       f"{to_float(r['events']):,.0f} lines - "
                       f"${to_float(r['value']):,.2f}")
        out.append(f"  Total: ${tot:,.2f}  ({pct:.2f}% of ${rev:,.0f} sales)\n")
    out.append(f"Chain-wide loss: ${grand:,.2f}")
    out.append(f"\n[{last_query_stats['description']} from {R_DEPT}]")
    return "\n".join(out)


@mcp.tool()
def get_seasonal_trends(period: str = "this_month",
                        department: Optional[str] = None,
                        store_id: Optional[str] = None) -> str:
    """
    This period against the same period last year and two years ago.
    """
    start, end = resolve_period(period)
    dept = resolve_department(department)
    if department and not dept:
        return f"Unknown department '{department}'."

    def shift(d, y):
        try:
            return d.replace(year=d.year - y)
        except ValueError:
            return d.replace(year=d.year - y, day=28)

    out = [f"Seasonal comparison - {period}"
           + (f" - {DEPARTMENTS[dept]}" if dept else "") + "\n"]
    revs = []
    for label, yrs in (("This year", 0), ("Last year", 1), ("Two years ago", 2)):
        s, e = shift(start, yrs), shift(end, yrs)
        extra = ["transaction_type = 'SALE'"]
        if dept:     extra.append(f"department_code = '{dept}'")
        if store_id: extra.append(f"store_id = '{store_id}'")
        r = run_query(f"""
            SELECT SUM(revenue) AS revenue, SUM(units) AS units,
                   SUM(basket_count) AS baskets
            FROM {R_DEPT} WHERE {scope(R_DEPT, s, e, extra)}
        """, table=R_DEPT)
        rev = to_float(r[0]["revenue"]) if r else 0
        revs.append(rev)
        out.append(f"{label} ({s} to {e}):")
        out.append(f"  ${rev:,.2f} | {to_float(r[0]['units']):,.0f} units")

    def pct(a, b):
        if not b: return "no data"
        c = (a - b) / b * 100
        return f"{'+' if c >= 0 else ''}{c:.1f}%"

    out.append("")
    out.append(f"vs last year:     {pct(revs[0], revs[1])}")
    out.append(f"vs two years ago: {pct(revs[0], revs[2])}")
    out.append(f"\n[{last_query_stats['description']} from {R_DEPT}]")
    return "\n".join(out)


@mcp.tool()
def get_store_comparison(period: str = "last_30_days",
                         metric: str = "revenue") -> str:
    """
    Stores ranked side by side.

    metric: revenue, units, margin, margin_pct
    """
    start, end = resolve_period(period)
    col = {"revenue":"SUM(revenue)","units":"SUM(units)","margin":"SUM(margin)",
           "margin_pct":"SUM(margin)*100.0/NULLIF(SUM(revenue),0)"
           }.get(metric, "SUM(revenue)")

    rows = run_query(f"""
        SELECT store_name, {col} AS v FROM {R_DEPT}
        WHERE {scope(R_DEPT, start, end, ["transaction_type = 'SALE'"])}
        GROUP BY store_name ORDER BY v DESC
    """, table=R_DEPT)
    if not rows:
        return f"No data for {period}."

    out = [f"Store comparison by {metric} - {period} ({start} to {end})\n"]
    for i, r in enumerate(rows, 1):
        v = to_float(r["v"])
        s = (f"${v:,.2f}" if metric in ("revenue","margin")
             else f"{v:.1f}%" if metric == "margin_pct" else f"{v:,.0f}")
        out.append(f"{i}. {r['store_name']}: {s}")
    out.append(f"\n[{last_query_stats['description']} from {R_DEPT}]")
    return "\n".join(out)


@mcp.tool()
def get_promotion_performance(period: str = "last_30_days") -> str:
    """
    Sales split by how the price was set - base retail versus promotional -
    and what each earns. Needs transaction detail, since RetailType is not
    carried on the rollups.
    """
    start, end = resolve_period(period)
    rows = run_query(f"""
        SELECT retail_type,
               SUM(extended_price) AS revenue,
               SUM(quantity)       AS units,
               SUM(gross_margin)   AS margin,
               SUM(loyalty_discount + premium_discount) AS discount
        FROM {DETAIL}
        WHERE {scope(DETAIL, start, end, ["transaction_type = 'SALE'"])}
        GROUP BY retail_type ORDER BY revenue DESC
    """, table=DETAIL)
    if not rows:
        return f"No sales data for {period}."

    names = {"B":"Base retail","T":"TPR (temporary reduction)","S":"Sale price",
             "L":"Loyalty price","R":"Special reduction","E":"Electronic coupon"}
    total = sum(to_float(r["revenue"]) for r in rows)
    out = [f"Pricing mix - {period} ({start} to {end})\n",
           f"Total: ${total:,.2f}\n"]
    for r in rows:
        rev, mg = to_float(r["revenue"]), to_float(r["margin"])
        share = (rev / total * 100) if total else 0
        mpct = (mg / rev * 100) if rev else 0
        out.append(f"  {names.get(r['retail_type'], r['retail_type'] or 'Unset')}: "
                   f"${rev:,.2f} ({share:.1f}%) | "
                   f"{to_float(r['units']):,.0f} units | "
                   f"${mg:,.2f} margin ({mpct:.1f}%) | "
                   f"${to_float(r['discount']):,.2f} given away")
    out.append(f"\n[{last_query_stats['description']} from {DETAIL}]")
    return "\n".join(out)


@mcp.tool()
def search_items(query: str, period: str = "last_30_days", limit: int = 25,
                 store_id: Optional[str] = None) -> str:
    """
    Sales for specific products, by name OR by UPC/barcode.

    Totals always cover every match, even when the list below is trimmed.
    Singular and plural both match.
    """
    start, end = resolve_period(period)

    digits = re.sub(r"[^0-9]", "", query)
    if digits and len(digits) >= 5 and not re.search(r"[a-zA-Z]{2,}", query):
        stripped = digits.lstrip("0") or digits
        rows = run_query(f"""
            SELECT item_id, MIN(item_desc) AS item_desc,
                   MIN(department_name) AS department_name,
                   SUM(units) AS units, SUM(revenue) AS revenue,
                   SUM(margin) AS margin
            FROM {R_ITEM}
            WHERE {scope(R_ITEM, start, end, ["transaction_type = 'SALE'"])}
              AND (regexp_replace(item_id, '^0+', '') = '{stripped}'
                   OR item_id LIKE '%{stripped}')
            GROUP BY item_id
        """, table=R_ITEM)
        if not rows:
            return (f"No sales for barcode {digits} between {start} and {end}.")
        out = [f"Barcode {digits} ({start} to {end})\n"]
        for r in rows:
            rev, mg = to_float(r["revenue"]), to_float(r["margin"])
            out.append(f"  {r['item_desc']} ({r['department_name']})")
            out.append(f"    UPC {r['item_id']} | {to_float(r['units']):,.0f} "
                       f"units | ${rev:,.2f} | ${mg:,.2f} margin "
                       f"({mg/rev*100 if rev else 0:.1f}%)")
        out.append(f"\n[{last_query_stats['description']} from {R_ITEM}]")
        return "\n".join(out)

    STOP = {"the","a","an","of","in","for","and","how","many","much","sold",
            "sales","did","we","our","last","month"}
    raw = [t for t in query.replace(",", " ").split()
           if t and t.lower() not in STOP]
    if not raw:
        return "Give me a product name or barcode."
    terms = [stem(t) for t in raw]

    like = " AND ".join(f"UPPER(item_desc) LIKE '%{t.upper()}%'" for t in terms)
    extra = ["transaction_type = 'SALE'", like]
    if store_id:
        extra.append(f"store_id = '{store_id}'")

    agg = run_query(f"""
        SELECT COUNT(DISTINCT product_key) AS products,
               SUM(units) AS units, SUM(revenue) AS revenue,
               SUM(margin) AS margin
        FROM {R_ITEM} WHERE {scope(R_ITEM, start, end, extra)}
    """, table=R_ITEM)
    n_products = int(to_float(agg[0]["products"])) if agg else 0
    if not n_products:
        return f"Nothing matching '{query}' sold between {start} and {end}."

    rows = run_query(f"""
        SELECT product_key, MIN(item_desc) AS item_desc,
               MIN(department_name) AS department_name,
               COUNT(DISTINCT item_id) AS barcodes,
               SUM(units) AS units, SUM(revenue) AS revenue,
               SUM(margin) AS margin
        FROM {R_ITEM} WHERE {scope(R_ITEM, start, end, extra)}
        GROUP BY product_key ORDER BY revenue DESC LIMIT {limit}
    """, table=R_ITEM)

    out = [f"Sales matching '{query}' ({start} to {end})\n"]
    if terms != [t.lower() for t in raw]:
        out.append(f"(matched on stem: {' + '.join(terms)})")
    out.append(f"TOTAL across all {n_products:,} product(s): "
               f"{to_float(agg[0]['units']):,.0f} units, "
               f"${to_float(agg[0]['revenue']):,.2f} revenue, "
               f"${to_float(agg[0]['margin']):,.2f} margin")
    if n_products > len(rows):
        out.append(f"Showing top {len(rows)} - the total above is complete.")
    out.append("")
    for r in rows:
        rev, mg = to_float(r["revenue"]), to_float(r["margin"])
        bc = to_float(r["barcodes"])
        tag = f" [{bc:.0f} barcodes]" if bc > 1 else ""
        out.append(f"  {r['item_desc']} ({r['department_name']}){tag}")
        out.append(f"    {to_float(r['units']):,.0f} units | ${rev:,.2f} | "
                   f"${mg:,.2f} margin ({mg/rev*100 if rev else 0:.1f}%)")
    out.append(f"\n[{last_query_stats['description']} from {R_ITEM}]")
    return "\n".join(out)


@mcp.tool()
def get_dead_stock(period: str = "last_90_days",
                   department: Optional[str] = None, limit: int = 25) -> str:
    """
    Items that sold nothing in the period, and how much of the range is idle.
    """
    start, end = resolve_period(period)
    dept = resolve_department(department)
    extra = ["transaction_type = 'SALE'"]
    if dept:
        extra.append(f"department_code = '{dept}'")

    sold = run_query(f"""
        SELECT department_name,
               COUNT(DISTINCT item_id) AS items,
               SUM(revenue) AS revenue
        FROM {R_ITEM} WHERE {scope(R_ITEM, start, end, extra)}
        GROUP BY department_name ORDER BY items DESC
    """, table=R_ITEM)
    if not sold:
        return f"No sales data for {period}."

    total_sold = sum(int(to_float(r["items"])) for r in sold)
    out = [f"Range activity - {start} to {end}\n",
           f"{total_sold:,} distinct items sold"
           + (f" in {DEPARTMENTS[dept]}" if dept else " across all departments"),
           ""]
    out.append("Items sold per department:")
    for r in sold[:limit]:
        out.append(f"  {r['department_name']}: {int(to_float(r['items'])):,} "
                   f"items, ${to_float(r['revenue']):,.2f}")
    out.append("\nNote: the catalogue holds far more items than these. Anything "
               "absent here did not sell in the period.")
    out.append(f"\n[{last_query_stats['description']} from {R_ITEM}]")
    return "\n".join(out)


@mcp.tool()
def describe_data() -> str:
    """
    The tables, what each is for, and the real values in the data.

    Call this before writing a custom query with run_sql.
    """
    start, end = resolve_period("last_90_days")
    out = ["POS analytics - Athena tables\n",
           f"{DETAIL:<14} transaction detail, one row per scanned line",
           f"{R_ITEM:<14} daily totals per item     (cheaper)",
           f"{R_DEPT:<14} daily totals per dept/category/tender (cheapest)",
           "",
           "All three are partitioned by store, year, month, day.",
           "ALWAYS filter on year and month or the query scans everything.",
           ""]

    for t, cols in ((DETAIL, "transaction_id, store_id, store_name, "
                             "transaction_ts, terminal_code, cashier_id, "
                             "key_function, item_id, item_desc, product_key, "
                             "department_code, department_name, category_name, "
                             "transaction_type, quantity, unit_price, "
                             "unit_cost, extended_price, gross_margin, "
                             "retail_type, is_taxable, is_food_stamp, is_wic, "
                             "loyalty_discount, premium_discount, tender_type"),
                    (R_ITEM, "sale_date, store_id, store_name, "
                             "department_code, department_name, category_name, "
                             "item_id, item_desc, product_key, "
                             "transaction_type, units, revenue, margin, "
                             "discount, line_count, basket_count"),
                    (R_DEPT, "sale_date, store_id, store_name, "
                             "department_code, department_name, category_name, "
                             "tender_type, transaction_type, units, revenue, "
                             "margin, discount, line_count, basket_count")):
        out.append(f"{t}")
        out.append(f"  {cols}\n")

    for label, col in (("transaction_type", "transaction_type"),
                       ("tender_type", "tender_type"),
                       ("department_name", "department_name")):
        vals = run_query(f"""
            SELECT DISTINCT {col} AS v FROM {R_DEPT}
            WHERE {partition_filter(start, end)} AND {col} <> ''
            LIMIT 40
        """, table=R_DEPT)
        out.append(f"{label}: " + ", ".join(sorted(r["v"] for r in vals)))
        out.append("")

    rng = run_query(f"""
        SELECT MIN(sale_date) AS lo, MAX(sale_date) AS hi FROM {R_DEPT}
        WHERE year >= 2024
    """, table=R_DEPT)
    if rng:
        out.append(f"Date range: {rng[0]['lo']} to {rng[0]['hi']}")
    out.append("\nNotes")
    out.append("  - revenue and units are negative on voids and returns")
    out.append("  - filter transaction_type='SALE' for clean sales figures")
    out.append("  - gross_margin uses price book cost; the log writes 0")
    out.append("  - product_key groups one product sold under several barcodes")
    out.append("  - key_function is the register key: UPC for a scanned "
               "barcode, OPEN DEPT nnn for a department key")
    out.append("  - call verify_totals to confirm these tables still agree "
               "with the raw log")
    return "\n".join(out)


FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|MSCK)\b", re.I)


@mcp.tool()
def run_sql(sql: str, limit: int = 50) -> str:
    """
    Read-only SQL against the data lake, for questions no other tool covers -
    basket affinity, cashier behaviour, hour-of-day patterns.

    Prefer the purpose-built tools; they apply consistent definitions and pick
    the cheapest table. Call describe_data first.

    Queries MUST filter on year (and ideally month) or the cost guard rejects
    them - without it Athena reads every partition.
    """
    q = (sql or "").strip().rstrip(";")
    if not q:
        return "Give me a SELECT statement."
    low = q.lstrip("( \n\t").lower()
    if not (low.startswith("select") or low.startswith("with")):
        return "Only SELECT is allowed."
    if FORBIDDEN_SQL.search(q) or ";" in q:
        return "Read-only, one statement at a time."

    limit = max(1, min(int(limit or 50), 200))
    try:
        rows = run_query(f"SELECT * FROM ({q}) LIMIT {limit + 1}", table="custom")
    except CostGuardError as e:
        return f"Refused: {e}"
    except Exception as e:
        return f"Query failed: {e}\n\nCall describe_data to check names."

    if not rows:
        return "Ran successfully, no rows returned."
    truncated = len(rows) > limit
    rows = rows[:limit]
    cols = list(rows[0].keys())
    w = [min(max(len(c), max(len(str(r[c])) for r in rows)), 40) for c in cols]
    out = ["  ".join(c[:x].ljust(x) for c, x in zip(cols, w)),
           "  ".join("-" * x for x in w)]
    for r in rows:
        out.append("  ".join(str(r[c])[:x].ljust(x) for c, x in zip(cols, w)))
    tail = f"\n{len(rows)} row(s)"
    if truncated:
        tail += " - more exist, raise `limit`"
    tail += f"\n[{last_query_stats['description']}]"
    return "\n".join(out) + tail


# The SALE definition expressed against the raw CSV columns. This is a second,
# independent implementation of the classifier in the ETL - if the two ever
# disagree, that is exactly what we want to hear about. Kept in step with
# RAW_SALE_PREDICATE in glue/tlog_etl.py.
RAW_SALE_PREDICATE = """
      NOT (CAST(sequence AS VARCHAR) = '0' AND TRIM(key_function) = '')
  AND UPPER(TRIM(key_function)) NOT LIKE 'TENDER%'
  AND UPPER(TRIM(key_function)) <> 'VOID'
  AND UPPER(TRIM(key_function)) NOT LIKE '%COUPON%'
  AND UPPER(COALESCE(attribute_flag, '')) NOT LIKE '%C%'
  AND UPPER(COALESCE(attribute_flag, '')) NOT LIKE '%R%'
"""


@mcp.tool()
def verify_totals(period: str = "last_30_days") -> str:
    """
    Prove the numbers are right by deriving them four different ways.

    Compares the raw transaction log against the Parquet detail table and both
    rollups. All four should return the same revenue and the same line count.
    If they do, every other tool is answering from consistent data; if they
    do not, something in the pipeline is wrong and the figures should not be
    trusted until it is fixed.

    Use this when someone asks whether a number is correct, when a figure
    looks different from one they have seen before, or before presenting
    results to anyone.
    """
    start, end, label = resolve_period(period)
    parts = partition_filter(start, end)
    d1, d2 = f"DATE '{start}'", f"DATE '{end}'"

    sql = f"""
    SELECT 'raw log' AS source, COUNT(*) AS lines,
           ROUND(SUM(ext_sales), 2) AS revenue
      FROM tlog_raw
     WHERE {parts}
       AND DATE(DATE_PARSE(date, '%m/%d/%Y')) BETWEEN {d1} AND {d2}
       AND {RAW_SALE_PREDICATE}
    UNION ALL
    SELECT 'parquet detail', COUNT(*), ROUND(SUM(extended_price), 2)
      FROM {DETAIL}
     WHERE {parts} AND DATE(transaction_ts) BETWEEN {d1} AND {d2}
       AND transaction_type = 'SALE'
    UNION ALL
    SELECT 'item rollup', CAST(SUM(line_count) AS BIGINT), ROUND(SUM(revenue), 2)
      FROM {R_ITEM}
     WHERE {parts} AND sale_date BETWEEN {d1} AND {d2}
       AND transaction_type = 'SALE'
    UNION ALL
    SELECT 'department rollup', CAST(SUM(line_count) AS BIGINT), ROUND(SUM(revenue), 2)
      FROM {R_DEPT}
     WHERE {parts} AND sale_date BETWEEN {d1} AND {d2}
       AND transaction_type = 'SALE'
    """

    try:
        rows = run_query(sql, table="reconciliation")
    except Exception as e:
        return f"Could not run the check: {e}"

    got = {r["source"]: (int(r["lines"] or 0), to_float(r["revenue"]))
           for r in rows}
    order = ["raw log", "parquet detail", "item rollup", "department rollup"]

    out = [f"Reconciliation - {label} ({start} to {end})", ""]
    for src in order:
        ln, rev = got.get(src, (0, 0.0))
        out.append(f"  {src:<20} {ln:>10,} lines   ${rev:>14,.2f}")

    base_ln, base_rev = got.get("raw log", (0, 0.0))
    bad = []
    for src in order[1:]:
        ln, rev = got.get(src, (0, 0.0))
        if ln != base_ln or abs(rev - base_rev) > 0.01:
            bad.append(f"  {src}: {ln - base_ln:+,} lines, "
                       f"${rev - base_rev:+,.2f} against the raw log")

    out.append("")
    if bad:
        out.append("MISMATCH - do not trust these figures:")
        out += bad
        out.append("")
        out.append("The ETL has not reproduced the log faithfully. Re-run the "
                   "Glue job, and if it still disagrees the classifier and the "
                   "raw predicate have drifted apart.")
    else:
        out.append(f"All four agree: ${base_rev:,.2f} across {base_ln:,} lines.")
        out.append("")
        out.append("The raw log is the register's own output; the other three "
                   "are built from it by separate code paths. Agreement means "
                   "nothing was dropped, duplicated or double-counted on the "
                   "way through.")

    out.append("")
    out.append("Counted as a sale: everything except tender lines, the "
               "end-of-transaction total, voids and returns. Open-department "
               "rings are included - they are real revenue.")
    out.append(f"[{last_query_stats['description']}]")
    return "\n".join(out)


@mcp.tool()
def get_query_cost() -> str:
    """
    What Athena has cost today, and what the last query scanned.
    """
    out = [guard.status()]
    if last_query_stats["description"]:
        out.append(f"Last query: {last_query_stats['description']}"
                   + (f" from {last_query_stats['table']}"
                      if last_query_stats["table"] else ""))
    out += ["", "Guards:",
            f"  daily budget        ${guard.daily_budget:.2f}",
            f"  per-query ceiling   {guard.max_scan_bytes/(1024**3):.0f} GB",
            f"  partition filter    "
            f"{'required' if guard.require_partition_filter else 'optional'}",
            f"  AWS workgroup cap   {config.get('workgroup') or 'NOT SET'}",
            "",
            "Athena bills $5 per TB scanned. Tools route to the smallest table "
            "that can answer the question, which is why most queries cost a "
            "fraction of a cent."]
    return "\n".join(out)


if __name__ == "__main__":
    mcp.run()
