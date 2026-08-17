#!/usr/bin/env python3
"""
POS Analytics MCP Server - DEMO VERSION

Same tools as the production server, but queries a local SQLite database
built from parsed RORC TLOG files instead of Athena. No AWS account needed.

Setup:
    python3 generate_demo_data.py     # creates demo_pos.db from TLOG records
    python3 demo_mcp_server.py        # test it starts

Then point Claude Desktop at this file.
"""

import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

# MCP SDK 2.0 renamed FastMCP to MCPServer. Support both.
try:
    from mcp.server import MCPServer as _Server        # SDK 2.x
except ImportError:
    from mcp.server.fastmcp import FastMCP as _Server  # SDK 1.x


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_pos.db")


def run_query(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def to_float(val):
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

# RORC stores numeric department codes. These are the client's actual codes,
# read from their price book export. Accept either the code or the name so the
# business team can just say "produce".
DEPARTMENTS = {
    "10": "Alcohol",       "11": "Produce",   "12": "Kitchen",
    "13": "Meat",          "14": "Deli",      "15": "Caviar",
    "16": "Dairy",         "17": "Bread",     "18": "Pet",
    "19": "Seafood",       "20": "Bakery",    "21": "Frozen",
    "22": "Grocery",       "25": "Non-Food",  "26": "Dry Goods",
    "27": "Pickled",       "28": "Garden",    "31": "Soda",
    "32": "Beer",          "40": "Garden",
}

_DEPT_ALIASES = {
    "alcohol": "10", "alc": "10", "liquor": "10", "wine": "10", "spirits": "10",
    "produce": "11", "pro": "11", "fruit": "11", "vegetables": "11", "veg": "11",
    "kitchen": "12", "kit": "12", "prepared": "12",
    "meat": "13", "mea": "13", "butcher": "13",
    "deli": "14", "del": "14", "delicatessen": "14",
    "caviar": "15", "cav": "15", "roe": "15",
    "dairy": "16", "dai": "16",
    "bread": "17", "bre": "17",
    "pet": "18", "pet food": "18",
    "seafood": "19", "sea": "19", "fish": "19",
    "bakery": "20", "bak": "20",
    "frozen": "21", "fro": "21", "frozen food": "21", "freezer": "21",
    "grocery": "22", "gro": "22", "center store": "22", "dry grocery": "22",
    "non-food": "25", "nonfood": "25", "non food": "25", "hba": "25",
    "health & beauty": "25", "household": "25",
    "dry goods": "26", "dry": "26",
    "pickled": "27", "pic": "27", "pickles": "27",
    "garden": "28", "gar": "28",
    "soda": "31", "sod": "31", "soft drinks": "31",
    "beer": "32", "bee": "32",
}


def resolve_department(value):
    """Accept a department code, key, or plain-English name."""
    if not value:
        return None
    v = str(value).strip()
    if v in DEPARTMENTS:
        return v
    return _DEPT_ALIASES.get(v.lower())


# ---------------------------------------------------------------------------
# Period helper
# ---------------------------------------------------------------------------

def resolve_period(period: str):
    today = datetime.now().date()
    if period == "today":
        return today, today
    if period == "yesterday":
        d = today - timedelta(days=1)
        return d, d
    if period == "last_7_days":
        return today - timedelta(days=7), today
    if period == "last_30_days":
        return today - timedelta(days=30), today
    if period == "last_90_days":
        return today - timedelta(days=90), today
    if period == "last_week":
        monday = today - timedelta(days=today.weekday() + 7)
        return monday, monday + timedelta(days=6)
    if period == "this_month":
        return today.replace(day=1), today
    if period == "last_month":
        first = today.replace(day=1)
        last = first - timedelta(days=1)
        return last.replace(day=1), last
    if period == "this_year":
        return today.replace(month=1, day=1), today
    return today - timedelta(days=30), today


def date_filter(start, end):
    return "DATE(transaction_date) BETWEEN ? AND ?", [str(start), str(end)]


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = _Server("POS Analytics")


@mcp.tool()
def get_top_movers(
    period: str = "last_30_days",
    direction: str = "top",
    metric: str = "revenue",
    limit: int = 20,
    store_id: Optional[str] = None,
    department: Optional[str] = None,
    category: Optional[str] = None
) -> str:
    """
    Get the best or worst selling items, optionally within a department or a
    merchandising category.

    Category is narrower than department and is how buyers usually think -
    BERRIES, CHEESE CHUNKS, WINE, BLACK CAVIAR, GRAINS AND CEREAL and so on.
    Use it when the question names a product group rather than a whole
    department. Partial names work, so "berries" finds "BERRIES".

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    direction: top (best sellers) or bottom (slowest movers)
    metric: revenue (dollar sales), units (quantity sold), or margin (gross profit)
    limit: how many items to return, default 20
    store_id: store code, or omit for all stores
    department: name or code, e.g. produce, frozen, deli, grocery, alcohol
    category: merchandising category, e.g. berries, wine, cheese chunks
    """
    start, end = resolve_period(period)
    order = "DESC" if direction == "top" else "ASC"
    metric_col = {
        "revenue": "SUM(extended_price)",
        "units":   "SUM(quantity)",
        "margin":  "SUM(gross_margin)",
    }.get(metric, "SUM(extended_price)")

    df, params = date_filter(start, end)
    filters = [df, "transaction_type = 'SALE'"]

    if store_id:
        filters.append("store_id = ?")
        params.append(store_id.upper())

    dept_code = resolve_department(department)
    if dept_code:
        filters.append("department_code = ?")
        params.append(dept_code)
    elif department:
        return (f"Unknown department '{department}'. Valid options: "
                + ", ".join(sorted(set(DEPARTMENTS.values()))))

    matched_category = None
    if category:
        matched_category = resolve_category(category)
        if not matched_category:
            near = suggest_categories(category)
            return (f"No category matching '{category}'."
                    + (f" Did you mean: {', '.join(near)}?" if near else
                       " Ask for a sales summary grouped by category to see"
                       " what exists."))
        filters.append("category_name = ?")
        params.append(matched_category)

    # Group by product rather than barcode so the same item sold under several
    # supplier UPCs is not split across the ranking.
    sql = f"""
        SELECT product_key,
               MIN(item_desc)       AS item_desc,
               MIN(department_name) AS department_name,
               COUNT(DISTINCT item_id) AS barcodes,
               SUM(extended_price)  AS revenue,
               SUM(quantity)        AS units,
               SUM(gross_margin)    AS margin
        FROM fact_transactions
        WHERE {' AND '.join(filters)}
        GROUP BY product_key
        ORDER BY {metric_col} {order}
        LIMIT ?
    """
    params.append(limit + 1)
    all_rows = run_query(sql, params)

    if not all_rows:
        return f"No sales data found for {period}."

    truncated = len(all_rows) > limit
    rows = all_rows[:limit]

    label = "Top" if direction == "top" else "Bottom"
    scope = ""
    if matched_category:
        scope += f" - {matched_category}"
    if dept_code:
        scope += f" - {DEPARTMENTS[dept_code]}"
    if store_id:
        scope += f" - {store_id.upper()}"

    lines = [f"{label} {len(rows)} movers by {metric}{scope} "
             f"({start} to {end})\n"]
    if truncated:
        lines.append(f"There are more products beyond these {limit}. "
                     f"Raise `limit` to see further down the list.\n")

    merged = sum(to_float(r["barcodes"]) for r in rows) - len(rows)
    if merged > 0:
        lines.append(f"{merged:,.0f} duplicate barcode(s) rolled into their "
                     f"parent product.\n")

    for i, r in enumerate(rows, 1):
        rev = to_float(r["revenue"])
        un  = to_float(r["units"])
        mg  = to_float(r["margin"])
        pct = (mg / rev * 100) if rev else 0
        bc  = to_float(r["barcodes"])
        tag = f" [{bc:.0f} barcodes]" if bc > 1 else ""
        lines.append(
            f"{i}. {r['item_desc']} ({r['department_name']}){tag} - "
            f"${rev:,.2f} rev | {un:,.0f} units | ${mg:,.2f} margin ({pct:.1f}%)"
        )
    return "\n".join(lines)


@mcp.tool()
def get_loss_report(
    period: str = "last_30_days",
    store_id: Optional[str] = None
) -> str:
    """
    Show POS-level loss events - voids and returns - with counts and dollar
    values by store, plus each store's loss as a percentage of its sales so
    bigger stores are not unfairly flagged.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    store_id: STORE01 through STORE05, or omit for all stores
    """
    start, end = resolve_period(period)

    df, params = date_filter(start, end)
    filters = [df, "is_loss_event = 1"]
    if store_id:
        filters.append("store_id = ?")
        params.append(store_id.upper())

    loss_rows = run_query(f"""
        SELECT store_name AS store,
               transaction_type_name AS loss_type,
               COUNT(*) AS event_count,
               SUM(ABS(extended_price)) AS total_value
        FROM fact_transactions
        WHERE {' AND '.join(filters)}
        GROUP BY store_name, transaction_type_name
        ORDER BY store, total_value DESC
    """, params)

    if not loss_rows:
        return f"No loss events found for {period}."

    # Sales by store, so loss can be shown as a share of revenue
    df2, params2 = date_filter(start, end)
    sales_filters = [df2, "transaction_type = 'SALE'"]
    if store_id:
        sales_filters.append("store_id = ?")
        params2.append(store_id.upper())
    sales = {
        r["store"]: to_float(r["revenue"])
        for r in run_query(f"""
            SELECT store_name AS store, SUM(extended_price) AS revenue
            FROM fact_transactions
            WHERE {' AND '.join(sales_filters)}
            GROUP BY store_name
        """, params2)
    }

    by_store = {}
    for r in loss_rows:
        by_store.setdefault(r["store"], []).append(r)

    lines = [f"Loss report - {period} ({start} to {end})\n"]
    grand = 0.0
    for store, rows in by_store.items():
        total = sum(to_float(r["total_value"]) for r in rows)
        grand += total
        rev = sales.get(store, 0.0)
        pct = (total / rev * 100) if rev else 0
        lines.append(f"{store}:")
        for r in rows:
            lines.append(f"  {r['loss_type']}: {r['event_count']:,} events "
                         f"- ${to_float(r['total_value']):,.2f}")
        lines.append(f"  Total: ${total:,.2f}  "
                     f"({pct:.2f}% of ${rev:,.0f} sales)\n")

    lines.append(f"Chain-wide loss: ${grand:,.2f}")
    return "\n".join(lines)


@mcp.tool()
def get_seasonal_trends(
    period: str = "this_month",
    department: Optional[str] = None,
    store_id: Optional[str] = None
) -> str:
    """
    Compare current period sales to the same period last year and two years ago.
    Shows revenue, units, transaction count, and percent change.

    period: last_7_days, last_30_days, last_week, this_month, last_month, last_90_days
    department: name or code, e.g. produce, frozen, meat
    store_id: STORE01 through STORE05, or omit for all stores
    """
    start, end = resolve_period(period)
    dept_code = resolve_department(department)
    if department and not dept_code:
        return (f"Unknown department '{department}'. Valid options: "
                + ", ".join(sorted(set(DEPARTMENTS.values()))))

    def shift(d, years):
        try:
            return d.replace(year=d.year - years)
        except ValueError:      # Feb 29
            return d.replace(year=d.year - years, day=28)

    periods = [
        ("This year",     start,           end),
        ("Last year",     shift(start, 1), shift(end, 1)),
        ("Two years ago", shift(start, 2), shift(end, 2)),
    ]

    results = []
    for label, s, e in periods:
        df, params = date_filter(s, e)
        filters = [df, "transaction_type = 'SALE'"]
        if store_id:
            filters.append("store_id = ?")
            params.append(store_id.upper())
        if dept_code:
            filters.append("department_code = ?")
            params.append(dept_code)
        row = run_query(f"""
            SELECT SUM(extended_price) AS revenue,
                   SUM(quantity)       AS units,
                   SUM(gross_margin)   AS margin,
                   COUNT(DISTINCT transaction_id) AS transactions
            FROM fact_transactions
            WHERE {' AND '.join(filters)}
        """, params)
        results.append((label, s, e, row[0] if row else {}))

    def pct(cur, prior):
        if not prior:
            return "no data"
        ch = ((cur - prior) / prior) * 100
        return f"{'+' if ch >= 0 else ''}{ch:.1f}%"

    scope = []
    if dept_code:
        scope.append(DEPARTMENTS[dept_code])
    if store_id:
        scope.append(store_id.upper())
    scope_str = f" - {', '.join(scope)}" if scope else ""

    lines = [f"Seasonal comparison - {period}{scope_str}\n"]
    revs = []
    for label, s, e, r in results:
        rev = to_float(r.get("revenue"))
        revs.append(rev)
        lines.append(f"{label} ({s} to {e}):")
        lines.append(f"  ${rev:,.2f} | {to_float(r.get('units')):,.0f} units "
                     f"| {to_float(r.get('transactions')):,.0f} transactions")

    lines.append("")
    lines.append(f"vs last year:     {pct(revs[0], revs[1])}")
    lines.append(f"vs two years ago: {pct(revs[0], revs[2])}")
    return "\n".join(lines)


@mcp.tool()
def get_sales_summary(
    period: str = "last_30_days",
    group_by: str = "department",
    store_id: Optional[str] = None
) -> str:
    """
    Get a sales breakdown grouped by department, store, or day.
    Shows revenue, margin, and share of total for each group.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    group_by: department, category, store, or day
    store_id: filter to one store (use with group_by=department or day)
    """
    start, end = resolve_period(period)
    df, params = date_filter(start, end)
    filters = [df, "transaction_type = 'SALE'"]

    if store_id:
        filters.append("store_id = ?")
        params.append(store_id.upper())

    label_col = {
        "department": "department_name",
        "category":   "category_name",
        "store":      "store_name",
    }.get(group_by, "DATE(transaction_date)")

    rows = run_query(f"""
        SELECT {label_col} AS group_label,
               SUM(extended_price) AS revenue,
               SUM(quantity)       AS units,
               SUM(gross_margin)   AS margin,
               COUNT(DISTINCT transaction_id) AS transactions
        FROM fact_transactions
        WHERE {' AND '.join(filters)}
        GROUP BY {label_col}
        ORDER BY revenue DESC
    """, params)

    if not rows:
        return f"No sales data found for {period}."

    total = sum(to_float(r["revenue"]) for r in rows)
    lines = [f"Sales by {group_by} - {period} ({start} to {end})\n",
             f"Chain total: ${total:,.2f}\n"]
    for r in rows:
        rev = to_float(r["revenue"])
        mg  = to_float(r["margin"])
        share = (rev / total * 100) if total else 0
        mpct  = (mg / rev * 100) if rev else 0
        lines.append(f"  {r['group_label']}: ${rev:,.2f} ({share:.1f}%) | "
                     f"{to_float(r['units']):,.0f} units | "
                     f"${mg:,.2f} margin ({mpct:.1f}%)")
    return "\n".join(lines)


@mcp.tool()
def get_store_comparison(
    period: str = "last_30_days",
    metric: str = "revenue"
) -> str:
    """
    Rank all stores side by side on a chosen metric.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    metric: revenue, units, transactions, avg_basket, margin, margin_pct
    """
    start, end = resolve_period(period)
    metric_col = {
        "revenue":      "SUM(extended_price)",
        "units":        "SUM(quantity)",
        "transactions": "COUNT(DISTINCT transaction_id)",
        "avg_basket":   "SUM(extended_price) * 1.0 / COUNT(DISTINCT transaction_id)",
        "margin":       "SUM(gross_margin)",
        "margin_pct":   "SUM(gross_margin) * 100.0 / NULLIF(SUM(extended_price),0)",
    }.get(metric, "SUM(extended_price)")

    df, params = date_filter(start, end)
    rows = run_query(f"""
        SELECT store_name AS store, {metric_col} AS metric_value
        FROM fact_transactions
        WHERE {df} AND transaction_type = 'SALE'
        GROUP BY store_name
        ORDER BY metric_value DESC
    """, params)

    if not rows:
        return f"No data found for {period}."

    lines = [f"Store comparison by {metric} - {period} ({start} to {end})\n"]
    for i, r in enumerate(rows, 1):
        v = to_float(r["metric_value"])
        if metric in ("revenue", "avg_basket", "margin"):
            s = f"${v:,.2f}"
        elif metric == "margin_pct":
            s = f"{v:.1f}%"
        else:
            s = f"{v:,.0f}"
        lines.append(f"{i}. {r['store']}: {s}")
    return "\n".join(lines)


@mcp.tool()
def get_promotion_performance(
    period: str = "last_30_days",
    department: Optional[str] = None
) -> str:
    """
    Break sales down by how the price was set - base retail versus promotional
    pricing - and show what each is earning. Useful for spotting promotions
    that move volume without making money.

    RetailType comes straight from the TLOG: B = base retail, T = temporary
    price reduction, S = sale price, L = loyalty price.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    department: name or code, or omit for all
    """
    start, end = resolve_period(period)
    dept_code = resolve_department(department)
    if department and not dept_code:
        return (f"Unknown department '{department}'. Valid options: "
                + ", ".join(sorted(set(DEPARTMENTS.values()))))

    df, params = date_filter(start, end)
    filters = [df, "transaction_type = 'SALE'"]
    if dept_code:
        filters.append("department_code = ?")
        params.append(dept_code)

    rows = run_query(f"""
        SELECT retail_type,
               SUM(extended_price) AS revenue,
               SUM(quantity)       AS units,
               SUM(gross_margin)   AS margin,
               SUM(loyalty_discount + premium_discount) AS discount
        FROM fact_transactions
        WHERE {' AND '.join(filters)}
        GROUP BY retail_type
        ORDER BY revenue DESC
    """, params)

    if not rows:
        return f"No sales data found for {period}."

    names = {
        "B": "Base retail",
        "T": "TPR (temporary price reduction)",
        "S": "Sale price",
        "L": "Loyalty price",
        "R": "Special price reduction",
        "E": "Electronic coupon",
    }

    total = sum(to_float(r["revenue"]) for r in rows)
    scope = f" - {DEPARTMENTS[dept_code]}" if dept_code else ""
    lines = [f"Pricing mix{scope} - {period} ({start} to {end})\n",
             f"Total sales: ${total:,.2f}\n"]
    for r in rows:
        rev = to_float(r["revenue"])
        mg  = to_float(r["margin"])
        disc = to_float(r["discount"])
        share = (rev / total * 100) if total else 0
        mpct  = (mg / rev * 100) if rev else 0
        label = names.get(r["retail_type"], r["retail_type"] or "Unspecified")
        lines.append(f"  {label}: ${rev:,.2f} ({share:.1f}%) | "
                     f"{to_float(r['units']):,.0f} units | "
                     f"${mg:,.2f} margin ({mpct:.1f}%) | "
                     f"${disc:,.2f} given away")
    return "\n".join(lines)


def resolve_category(value):
    """
    Match a spoken category name to one in the data.

    Categories come from the price book and are written in full caps with
    their own punctuation - "SALAMI, BOLOGNA, HAMS AND WIEN", "TEA & COFFEE".
    Exact match first, then a contains match, so "berries" finds "BERRIES"
    and "salami" finds the long one.
    """
    if not value:
        return None
    v = value.strip().upper()

    rows = run_query("SELECT DISTINCT category_name FROM fact_transactions "
                     "WHERE category_name != ''")
    names = [r["category_name"] for r in rows]

    for n in names:
        if n.upper() == v:
            return n
    starts = [n for n in names if n.upper().startswith(v)]
    if starts:
        return sorted(starts, key=len)[0]
    contains = [n for n in names if v in n.upper()]
    if contains:
        return sorted(contains, key=len)[0]
    return None


def suggest_categories(value, limit=6):
    """Offer near matches when a category name is not recognised."""
    v = (value or "").strip().upper()
    rows = run_query("SELECT DISTINCT category_name FROM fact_transactions "
                     "WHERE category_name != ''")
    names = [r["category_name"] for r in rows]
    words = [w for w in re.split(r"[^A-Z]+", v) if len(w) > 2]
    hits = [n for n in names if any(w in n.upper() for w in words)]
    return sorted(hits, key=len)[:limit]


def stem(word):
    """
    Reduce a search word to a stem so singular and plural both match.

    The catalogue writes the same fruit as STRAWBERRIES, STRAWBERRY and
    WILD STRAWBERRY. Searching the plural alone found 8 products and missed
    80 - about 41% of the dollars - which made every name-based total wrong.

    Only words long enough to survive it are stemmed, so short ones like
    "oz" or "red" are left alone.
    """
    w = word.strip().lower()
    if len(w) < 6:
        return w
    for suffix, cut in (("ies", 3), ("ches", 2), ("shes", 2),
                        ("ses", 2), ("s", 1), ("y", 1)):
        if w.endswith(suffix):
            stemmed = w[:-cut]
            if len(stemmed) >= 4:
                return stemmed
            break
    return w


def _search_by_upc(digits, start, end, store_id, period):
    """
    Look an item up by barcode. Store staff identify products by UPC, and the
    same barcode shows up padded to different lengths depending on which
    report it came from, so matching ignores leading zeros.
    """
    stripped = digits.lstrip("0") or digits

    params = [f"%{stripped}", stripped, str(start), str(end)]
    if store_id:
        params.append(store_id.upper())
    store_clause = "AND f.store_id = ?" if store_id else ""

    rows = run_query(f"""
        SELECT i.item_id, i.item_desc, i.department_name, i.category_name,
               i.unit_price, i.margin_pct,
               COALESCE(SUM(f.quantity), 0)       AS units,
               COALESCE(SUM(f.extended_price), 0) AS revenue,
               COALESCE(SUM(f.gross_margin), 0)   AS margin,
               COUNT(DISTINCT f.transaction_id)   AS baskets
        FROM dim_item i
        LEFT JOIN fact_transactions f
               ON f.item_id = i.item_id
              AND f.transaction_type = 'SALE'
              AND DATE(f.transaction_date) BETWEEN ? AND ?
              {store_clause}
        WHERE (LTRIM(i.item_id, '0') = ? OR i.item_id LIKE ?)
        GROUP BY i.item_id, i.item_desc, i.department_name, i.category_name,
                 i.unit_price, i.margin_pct
        ORDER BY revenue DESC
        LIMIT 25
    """, [str(start), str(end)] + ([store_id.upper()] if store_id else [])
         + [stripped, f"%{stripped}"])

    if not rows:
        return (f"No item in the catalogue has barcode {digits}. "
                "Check the number, or search by product name instead.")

    lines = [f"Barcode {digits} ({start} to {end})\n"]
    for r in rows:
        u   = to_float(r["units"])
        rev = to_float(r["revenue"])
        mg  = to_float(r["margin"])
        lines.append(f"  {r['item_desc']}")
        lines.append(f"    UPC {r['item_id']} | {r['department_name']} / "
                     f"{r['category_name']} | ${to_float(r['unit_price']):.2f} retail")
        if u:
            mp = (mg / rev * 100) if rev else 0
            lines.append(f"    {u:,.0f} units in {to_float(r['baskets']):,.0f} "
                         f"baskets | ${rev:,.2f} | ${mg:,.2f} margin ({mp:.1f}%)")
        else:
            lines.append(f"    No sales in this period - carried but not moving.")
    return "\n".join(lines)


@mcp.tool()
def search_items(
    query: str,
    period: str = "last_30_days",
    limit: int = 25,
    store_id: Optional[str] = None,
    group_duplicates: bool = True
) -> str:
    """
    Look up sales for specific products, by name OR by UPC/barcode.

    Use this when someone asks about a particular item rather than a whole
    department - "how many 16 oz strawberries sold last month", "what did UPC
    71575620002 do", "how is the kashkaval doing".

    Barcodes work in any form: with or without leading zeros, with or without
    dashes. Name searches match on the product description and will relax to
    fewer words if the full phrase finds nothing.

    IMPORTANT: the catalogue often carries the same product under several
    barcodes, one per supplier. By default those are rolled up into a single
    line so rankings are not split. Set group_duplicates to false to see each
    barcode separately.

    query: a barcode, or words to match, e.g. "strawberries 1 lb"
    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    limit: how many products to return
    store_id: STORE01 through STORE05, or omit for all stores
    group_duplicates: roll up the same product sold under multiple barcodes
    """
    start, end = resolve_period(period)

    # ---- barcode lookup --------------------------------------------------
    digits = re.sub(r"[^0-9]", "", query)
    if digits and len(digits) >= 5 and not re.search(r"[a-zA-Z]{2,}", query):
        return _search_by_upc(digits, start, end, store_id, period)

    # Drop filler words so "how many 16 oz strawberries" still finds something
    STOP = {"the", "a", "an", "of", "in", "for", "and", "how", "many",
            "much", "sold", "sales", "did", "we", "our", "last", "month"}
    raw_terms = [t for t in query.replace(",", " ").split()
                 if t and t.lower() not in STOP]
    if not raw_terms:
        return "Give me a product name to search for."

    terms = [stem(t) for t in raw_terms]

    # Rolling up by product_key merges the same item sold under several
    # supplier barcodes; item_id keeps them separate.
    key_col = "product_key" if group_duplicates else "item_id"

    def search(term_list, cap):
        df, params = date_filter(start, end)
        filters = [df, "transaction_type = 'SALE'"]
        for t in term_list:
            filters.append("UPPER(item_desc) LIKE ?")
            params.append(f"%{t.upper()}%")
        if store_id:
            filters.append("store_id = ?")
            params.append(store_id.upper())
        return run_query(f"""
            SELECT {key_col}            AS grp,
                   MIN(item_desc)       AS item_desc,
                   MIN(department_name) AS department_name,
                   MIN(category_name)   AS category_name,
                   COUNT(DISTINCT item_id) AS barcodes,
                   SUM(quantity)        AS units,
                   SUM(extended_price)  AS revenue,
                   SUM(gross_margin)    AS margin,
                   AVG(unit_price)      AS avg_price,
                   COUNT(DISTINCT transaction_id) AS baskets
            FROM fact_transactions
            WHERE {' AND '.join(filters)}
            GROUP BY {key_col}
            ORDER BY revenue DESC
            LIMIT ?
        """, params + [cap])

    # Try the full phrase first, then progressively relax by dropping the
    # least distinctive terms. Sizes and pack counts often do not appear in
    # the description the way a person says them.
    def totals(term_list):
        """
        Aggregate across every match, ignoring the display limit, so the
        headline numbers are the real totals rather than the sum of whatever
        happened to fit on screen.
        """
        df, params = date_filter(start, end)
        filters = [df, "transaction_type = 'SALE'"]
        for t in term_list:
            filters.append("UPPER(item_desc) LIKE ?")
            params.append(f"%{t.upper()}%")
        if store_id:
            filters.append("store_id = ?")
            params.append(store_id.upper())
        r = run_query(f"""
            SELECT COUNT(DISTINCT {key_col}) AS products,
                   COUNT(DISTINCT item_id)   AS barcodes,
                   SUM(quantity)             AS units,
                   SUM(extended_price)       AS revenue,
                   SUM(gross_margin)         AS margin
            FROM fact_transactions
            WHERE {' AND '.join(filters)}
        """, params)
        return r[0] if r else {}

    rows, used_terms = search(terms, limit), terms
    if not rows and len(terms) > 1:
        ranked = sorted(terms, key=len, reverse=True)
        for n in range(len(terms) - 1, 0, -1):
            attempt = ranked[:n]
            rows = search(attempt, limit)
            if rows:
                used_terms = attempt
                break

    relaxed = used_terms != terms
    agg = totals(used_terms)
    total_products = int(to_float(agg.get("products")))
    truncated = total_products > len(rows)

    if not rows:
        # Check whether the product exists at all but simply did not sell
        cat_filters, cat_params = [], []
        for t in terms:
            cat_filters.append("UPPER(item_desc) LIKE ?")
            cat_params.append(f"%{t.upper()}%")
        in_catalogue = run_query(f"""
            SELECT item_desc, department_name, unit_price
            FROM dim_item
            WHERE {' AND '.join(cat_filters)}
            LIMIT 10
        """, cat_params)
        if in_catalogue:
            lines = [f"No sales of '{query}' between {start} and {end}, but "
                     f"{len(in_catalogue)} matching item(s) are in the catalogue:\n"]
            for r in in_catalogue:
                lines.append(f"  {r['item_desc']} ({r['department_name']}) - "
                             f"${to_float(r['unit_price']):.2f} retail")
            lines.append("\nThese are carried but not selling in this period.")
            return "\n".join(lines)
        return f"Nothing in the catalogue matches '{query}'."

    tot_u = to_float(agg.get("units"))
    tot_r = to_float(agg.get("revenue"))
    tot_m = to_float(agg.get("margin"))
    tot_b = int(to_float(agg.get("barcodes")))

    scope = f" at {store_id.upper()}" if store_id else ""
    lines = [f"Sales matching '{query}'{scope} ({start} to {end})\n"]

    if relaxed:
        lines.append(f"No match on the full phrase, so this matches on "
                     f"{' + '.join(used_terms)} instead.\n")

    # Totals always cover every match, so they stay correct even when the
    # list below is trimmed for readability.
    lines.append(
        f"TOTAL across all {total_products:,} matching product(s)"
        f"{f' / {tot_b:,} barcodes' if tot_b > total_products else ''}: "
        f"{tot_u:,.0f} units, ${tot_r:,.2f} revenue, ${tot_m:,.2f} margin"
    )
    if terms != [t.lower() for t in raw_terms]:
        lines.append(f"(matched on stem: {' + '.join(terms)} - so singular "
                     f"and plural spellings are both included)")
    lines.append("")

    if truncated:
        lines.append(f"Top {len(rows)} of {total_products:,} by revenue. "
                     f"The total above is complete; the list is trimmed. "
                     f"Raise `limit` to see more.\n")

    for r in rows:
        u   = to_float(r["units"])
        rev = to_float(r["revenue"])
        mg  = to_float(r["margin"])
        mp  = (mg / rev * 100) if rev else 0
        bc  = to_float(r["barcodes"])
        tag = f" [{bc:.0f} barcodes]" if bc > 1 else ""
        lines.append(
            f"  {r['item_desc']} ({r['department_name']}){tag}\n"
            f"    {u:,.0f} units in {to_float(r['baskets']):,.0f} baskets | "
            f"${rev:,.2f} | ${to_float(r['avg_price']):.2f} avg price | "
            f"${mg:,.2f} margin ({mp:.1f}%)"
        )
    return "\n".join(lines)


@mcp.tool()
def get_tender_analysis(
    period: str = "last_30_days",
    tender_type: Optional[str] = None,
    group_by: str = "department",
    limit: int = 12
) -> str:
    """
    Show what customers buy broken down by how they paid. Answers questions
    like "what do EBT and food stamp customers buy", "how do cash baskets
    differ from credit", or "which departments depend on SNAP".

    Tender types in the data: CASH, CREDIT, DEBIT, FOODSTAMP (EBT/SNAP), CHECK.

    period: today, yesterday, last_7_days, last_30_days, last_week,
            this_month, last_month, last_90_days, this_year
    tender_type: focus on one payment method, or omit to compare all of them
    group_by: department or category
    limit: how many departments or categories to list per tender type
    """
    start, end = resolve_period(period)
    label_col = "category_name" if group_by == "category" else "department_name"

    # Tender names as they appear in the real logs
    aliases = {
        "ebt": "EBT FOOD", "snap": "EBT FOOD", "food stamp": "EBT FOOD",
        "food stamps": "EBT FOOD", "foodstamp": "EBT FOOD",
        "ebt food": "EBT FOOD", "ebt cash": "EBT CASH",
        "credit card": "VISA", "amex": "AMERICAN EXPRESS",
        "mc": "MASTERCARD", "debit card": "DEBIT",
    }
    tender = None
    if tender_type:
        t = tender_type.strip().lower()
        tender = aliases.get(t, tender_type.strip().upper())

        # If the requested tender is not present, say what is rather than
        # returning an empty result that reads like a broken tool.
        available = [r["tender_type"] for r in run_query(
            "SELECT DISTINCT tender_type FROM fact_transactions "
            "WHERE tender_type != '' ORDER BY tender_type")]
        if tender not in available:
            match = [a for a in available if tender in a or a in tender]
            if match:
                tender = match[0]
            else:
                return (f"No tender called '{tender_type}'. "
                        f"Available: {', '.join(available)}")

    # Basket-level summary per tender type
    df, params = date_filter(start, end)
    basket_filters = [df, "transaction_type = 'SALE'", "tender_type != ''"]
    if tender:
        basket_filters.append("tender_type = ?")
        params.append(tender)

    baskets = run_query(f"""
        SELECT tender_type,
               COUNT(DISTINCT transaction_id) AS baskets,
               SUM(extended_price)            AS revenue,
               SUM(gross_margin)              AS margin
        FROM fact_transactions
        WHERE {' AND '.join(basket_filters)}
        GROUP BY tender_type
        ORDER BY revenue DESC
    """, params)

    if not baskets:
        if tender:
            return (f"No sales paid by {tender} between {start} and {end}. "
                    "Valid tender types: CASH, CREDIT, DEBIT, FOODSTAMP, CHECK.")
        return f"No tender data found for {period}."

    # When one tender is singled out, its share has to be measured against
    # all sales in the period, not just the filtered rows.
    if tender:
        df_all, p_all = date_filter(start, end)
        all_rev = run_query(f"""
            SELECT SUM(extended_price) AS revenue
            FROM fact_transactions
            WHERE {df_all} AND transaction_type = 'SALE' AND tender_type != ''
        """, p_all)
        total_rev = to_float(all_rev[0]["revenue"]) if all_rev else 0
    else:
        total_rev = sum(to_float(b["revenue"]) for b in baskets)

    lines = [f"Purchases by payment method - {period} ({start} to {end})\n"]

    for b in baskets:
        t   = b["tender_type"]
        rev = to_float(b["revenue"])
        n   = to_float(b["baskets"])
        mg  = to_float(b["margin"])
        avg = rev / n if n else 0
        share = (rev / total_rev * 100) if total_rev else 0
        pretty = "FOODSTAMP (EBT/SNAP)" if t == "FOODSTAMP" else t

        lines.append(f"{pretty}")
        lines.append(f"  ${rev:,.2f} ({share:.1f}% of all sales) | "
                     f"{n:,.0f} baskets | ${avg:.2f} avg basket | "
                     f"${mg:,.2f} margin")

        df2, p2 = date_filter(start, end)
        rows = run_query(f"""
            SELECT {label_col} AS grp,
                   SUM(extended_price) AS revenue,
                   SUM(quantity)       AS units
            FROM fact_transactions
            WHERE {df2} AND transaction_type = 'SALE' AND tender_type = ?
            GROUP BY {label_col}
            ORDER BY revenue DESC
            LIMIT ?
        """, p2 + [t, limit])

        for r in rows:
            r_rev = to_float(r["revenue"])
            pct = (r_rev / rev * 100) if rev else 0
            lines.append(f"    {r['grp']}: ${r_rev:,.2f} ({pct:.1f}%) | "
                         f"{to_float(r['units']):,.0f} units")
        lines.append("")

    if not tender and len(baskets) > 1:
        lines.append("Note: a basket is counted under the tender that paid for "
                     "it, so split payments follow the primary tender.")
    return "\n".join(lines)


@mcp.tool()
def get_dead_stock(
    period: str = "last_90_days",
    department: Optional[str] = None,
    limit: int = 25
) -> str:
    """
    Find items in the catalogue that did not sell at all during a period, and
    summarise how much of the range is not moving. Useful for range reviews
    and deciding what to discontinue.

    period: last_30_days, last_90_days, this_year
    department: name or code, or omit for all
    limit: how many example items to list
    """
    start, end = resolve_period(period)
    dept_code = resolve_department(department)
    if department and not dept_code:
        return (f"Unknown department '{department}'. Valid options: "
                + ", ".join(sorted(set(DEPARTMENTS.values()))))

    dept_clause = "WHERE i.department_code = ?" if dept_code else ""
    dept_params = [dept_code] if dept_code else []

    summary = run_query(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN s.item_id IS NULL THEN 1 ELSE 0 END) AS unsold
        FROM dim_item i
        LEFT JOIN (
            SELECT DISTINCT item_id FROM fact_transactions
            WHERE DATE(transaction_date) BETWEEN ? AND ?
        ) s ON s.item_id = i.item_id
        {dept_clause}
    """, [str(start), str(end)] + dept_params)

    total  = int(to_float(summary[0]["total"]))
    unsold = int(to_float(summary[0]["unsold"]))
    if not total:
        return "No catalogue data available."

    rows = run_query(f"""
        SELECT i.item_desc, i.department_name, i.unit_price, i.margin_pct
        FROM dim_item i
        LEFT JOIN (
            SELECT DISTINCT item_id FROM fact_transactions
            WHERE DATE(transaction_date) BETWEEN ? AND ?
        ) s ON s.item_id = i.item_id
        {dept_clause}
        {'AND' if dept_code else 'WHERE'} s.item_id IS NULL
        ORDER BY i.unit_price DESC
        LIMIT ?
    """, [str(start), str(end)] + dept_params + [limit])

    scope = f" - {DEPARTMENTS[dept_code]}" if dept_code else ""
    lines = [
        f"Dead stock{scope} - nothing sold between {start} and {end}\n",
        f"{unsold:,} of {total:,} items had no sales "
        f"({unsold / total * 100:.1f}% of the range)\n",
    ]
    if rows:
        lines.append(f"Highest-priced examples ({len(rows)} of {unsold:,}):")
        for r in rows:
            lines.append(f"  {r['item_desc']} ({r['department_name']}) - "
                         f"${to_float(r['unit_price']):.2f} retail, "
                         f"{to_float(r['margin_pct']):.0f}% margin")

    by_dept = run_query("""
        SELECT i.department_name AS dept,
               COUNT(*) AS total,
               SUM(CASE WHEN s.item_id IS NULL THEN 1 ELSE 0 END) AS unsold
        FROM dim_item i
        LEFT JOIN (
            SELECT DISTINCT item_id FROM fact_transactions
            WHERE DATE(transaction_date) BETWEEN ? AND ?
        ) s ON s.item_id = i.item_id
        GROUP BY i.department_name
        HAVING total > 20
        ORDER BY (unsold * 1.0 / total) DESC
        LIMIT 8
    """, [str(start), str(end)])

    if by_dept and not dept_code:
        lines.append("\nWorst departments by share of range not selling:")
        for r in by_dept:
            t = to_float(r["total"])
            u = to_float(r["unsold"])
            lines.append(f"  {r['dept']}: {u:,.0f} of {t:,.0f} "
                         f"({u / t * 100:.0f}%)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Escape hatch
#
# The curated tools above cover the questions we expected. Real users ask
# questions nobody anticipated - "which cashier rings the most voids", "what
# sells together with caviar", "what hour is busiest on Saturdays". Rather
# than shipping a new tool every time, these two let Claude inspect the data
# and compose its own query, under guard rails.
# ---------------------------------------------------------------------------

FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|"
    r"DETACH|PRAGMA|VACUUM|REINDEX)\b", re.I)

MAX_SQL_ROWS = 200


@mcp.tool()
def describe_data() -> str:
    """
    Show the tables, columns and the values that actually appear in the data.

    Call this before writing a custom query with run_sql, so the query uses
    real column names and real category, department and tender values rather
    than guesses.
    """
    out = ["POS analytics database - schema and live values\n"]

    for table in ("fact_transactions", "dim_item", "dim_store",
                  "dim_department"):
        cols = run_query(f"PRAGMA table_info({table})")
        if not cols:
            continue
        out.append(f"{table}")
        out.append("  " + ", ".join(f"{c['name']} {c['type']}" for c in cols))
        n = run_query(f"SELECT COUNT(*) AS n FROM {table}")
        out.append(f"  rows: {int(to_float(n[0]['n'])):,}\n")

    rng = run_query("""
        SELECT MIN(DATE(transaction_date)) AS lo,
               MAX(DATE(transaction_date)) AS hi
        FROM fact_transactions
    """)
    if rng:
        out.append(f"Date range: {rng[0]['lo']} to {rng[0]['hi']}\n")

    def distinct(col, limit=40):
        rows = run_query(f"""
            SELECT {col} AS v, COUNT(*) AS n
            FROM fact_transactions WHERE {col} != ''
            GROUP BY {col} ORDER BY n DESC LIMIT {limit}
        """)
        return [r["v"] for r in rows]

    out.append("transaction_type: " + ", ".join(distinct("transaction_type")))
    out.append("")
    out.append("tender_type: " + ", ".join(distinct("tender_type")))
    out.append("")
    out.append("retail_type: " + ", ".join(distinct("retail_type"))
               + "   (B=base, T=temporary reduction, S=sale, L=loyalty)")
    out.append("")
    out.append("department_name: " + ", ".join(distinct("department_name")))
    out.append("")
    cats = distinct("category_name", 60)
    out.append(f"category_name (top {len(cats)} by volume): " + ", ".join(cats))
    out.append("")
    out.append("Notes")
    out.append("  - quantity and extended_price are negative on voids and returns")
    out.append("  - is_loss_event = 1 marks voids and returns")
    out.append("  - gross_margin uses price book cost; the log writes UnitCost as 0")
    out.append("  - product_key groups the same product sold under several barcodes")
    out.append("  - filter transaction_type='SALE' for clean sales figures")
    return "\n".join(out)


@mcp.tool()
def run_sql(sql: str, limit: int = 50) -> str:
    """
    Run a read-only SQL query against the POS database when no other tool
    fits the question.

    Use the purpose-built tools first - they apply consistent definitions of
    sales, losses and margin. Reach for this when the question is genuinely
    outside them, for example basket affinity, cashier behaviour, hour-of-day
    patterns, or a metric nobody has asked for before.

    Call describe_data first so the query uses real column and value names.

    Only SELECT is permitted. Results are capped. SQLite syntax.

    sql: a single SELECT statement
    limit: maximum rows returned, capped at 200
    """
    q = (sql or "").strip().rstrip(";")
    if not q:
        return "Give me a SELECT statement."

    lowered = q.lstrip("( \n\t").lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return "Only SELECT queries are allowed here."
    if FORBIDDEN_SQL.search(q):
        return ("That statement modifies data or schema. This tool is "
                "read-only - use SELECT.")
    if ";" in q:
        return "One statement at a time, please."

    limit = max(1, min(int(limit or 50), MAX_SQL_ROWS))
    wrapped = f"SELECT * FROM ({q}) LIMIT {limit + 1}"

    try:
        rows = run_query(wrapped)
    except Exception as e:
        return (f"Query failed: {e}\n\n"
                "Call describe_data to check column names and values.")

    if not rows:
        return "Query ran successfully but returned no rows."

    truncated = len(rows) > limit
    rows = rows[:limit]
    cols = list(rows[0].keys())

    def fmt(v):
        if isinstance(v, float):
            return f"{v:,.2f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v) if v is not None else ""

    widths = [max(len(c), max(len(fmt(r[c])) for r in rows)) for c in cols]
    widths = [min(w, 42) for w in widths]

    lines = ["  ".join(c[:w].ljust(w) for c, w in zip(cols, widths)),
             "  ".join("-" * w for w in widths)]
    for r in rows:
        lines.append("  ".join(fmt(r[c])[:w].ljust(w)
                               for c, w in zip(cols, widths)))

    footer = f"\n{len(rows)} row(s)"
    if truncated:
        footer += f" - more exist, raise `limit` (max {MAX_SQL_ROWS})"
    return "\n".join(lines) + footer


if __name__ == "__main__":
    mcp.run()
