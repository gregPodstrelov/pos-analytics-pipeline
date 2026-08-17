#!/usr/bin/env python3
"""
Athena cost guard.

Athena bills $5 per terabyte scanned. A single careless query against a few
years of transaction logs can scan the whole table, and unlike a server there
is no fixed ceiling - the bill just grows. This puts three layers in the way.

  1. AWS workgroup limit (the hard stop)
     A workgroup with BytesScannedCutoffPerQuery makes AWS kill any query that
     exceeds the byte limit mid-flight. This is the only guard that cannot be
     bypassed by application code, so it is the one that actually protects the
     bill. Create it with:  python3 athena_cost_guard.py --setup

  2. Pre-flight checks (catches the common mistake)
     Refuse queries with no partition filter, no LIMIT on a raw SELECT *, or
     an obviously unbounded date range. Partition pruning is the difference
     between scanning one day and scanning three years.

  3. Post-query accounting (keeps the running total honest)
     Athena reports DataScannedInBytes for every execution. Each query's cost
     is recorded, a daily budget is enforced, and the spend is reported back
     so whoever is asking can see what their question cost.

Usage as a library:

    from athena_cost_guard import CostGuard
    guard = CostGuard(daily_budget_usd=5.00, max_scan_gb=20)
    guard.check(sql)                      # raises CostGuardError if unsafe
    ...run the query...
    guard.record(bytes_scanned)           # after it finishes
"""

import re
import os
import json
import argparse
from datetime import date

PRICE_PER_TB = 5.00
BYTES_PER_TB = 1024 ** 4
BYTES_PER_GB = 1024 ** 3

# Resolved against this file's directory, not the working directory - the MCP
# server is launched by Claude Desktop from an arbitrary cwd, and spend
# tracking should not silently reset because of where the process started.
STATE_FILE = os.environ.get(
    "ATHENA_GUARD_STATE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 ".athena_spend.json"))


class CostGuardError(Exception):
    """Raised when a query is refused before it runs."""


# ---------------------------------------------------------------------------
# Query inspection
# ---------------------------------------------------------------------------

# Columns the table is partitioned on. A query touching none of these reads
# every partition.
PARTITION_COLUMNS = ("year", "month", "day", "transaction_date", "date_key")

WRITE_STATEMENTS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|MSCK|REPLACE|TRUNCATE)\b", re.I)


def has_partition_filter(sql):
    """
    True when the query constrains at least one partition column.

    Deliberately simple - it looks for a partition column next to a comparison
    or BETWEEN. The aim is to catch the query that forgot a date filter
    entirely, not to prove the filter is efficient.
    """
    s = re.sub(r"\s+", " ", sql).lower()
    for col in PARTITION_COLUMNS:
        if re.search(rf"\b{col}\b\s*(=|>|<|>=|<=|between|in\s*\()", s):
            return True
    return False


# Catalogue lookups read table definitions, not table contents - Athena
# reports zero bytes scanned for them. Requiring a partition filter on
# "which columns does this table have" is a false alarm, and a guard that
# cries wolf gets switched off.
METADATA_QUERY = re.compile(
    r"\b(information_schema|show\s+(tables|columns|partitions|create|databases)"
    r"|describe)\b", re.I)


def is_metadata_query(sql):
    return bool(METADATA_QUERY.search(sql))


def selects_everything(sql):
    """A bare SELECT * with no aggregation and no LIMIT reads whole objects."""
    s = re.sub(r"\s+", " ", sql).lower()
    if "select *" not in s:
        return False
    if " limit " in s:
        return False
    if re.search(r"\b(count|sum|avg|min|max|group by)\b", s):
        return False
    return True


# ---------------------------------------------------------------------------
# Spend tracking
# ---------------------------------------------------------------------------

def _load_state():
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    if state.get("date") != date.today().isoformat():
        state = {"date": date.today().isoformat(), "bytes": 0, "queries": 0}
    return state


def _save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except OSError:
        pass          # tracking is best effort; never block a query on it


class CostGuard:
    def __init__(self, daily_budget_usd=5.00, max_scan_gb=20,
                 require_partition_filter=True):
        self.daily_budget = daily_budget_usd
        self.max_scan_bytes = int(max_scan_gb * BYTES_PER_GB)
        self.require_partition_filter = require_partition_filter

    # ---- before the query ----
    def check(self, sql):
        if WRITE_STATEMENTS.search(sql):
            raise CostGuardError(
                "This connection is read-only. Only SELECT is permitted.")

        state = _load_state()
        spent = self.spend_usd(state["bytes"])
        if spent >= self.daily_budget:
            raise CostGuardError(
                f"Daily Athena budget reached: ${spent:.2f} of "
                f"${self.daily_budget:.2f} across {state['queries']} queries. "
                "Raise the budget or wait until tomorrow.")

        if is_metadata_query(sql):
            return                      # scans nothing; nothing to guard

        if self.require_partition_filter and not has_partition_filter(sql):
            raise CostGuardError(
                "This query has no date or partition filter, so Athena would "
                "read every partition in the table - potentially years of data "
                "for one answer.\n\n"
                "Add a bound on one of: "
                + ", ".join(PARTITION_COLUMNS) + "\n"
                "For example:  WHERE year = 2026 AND month = 8\n"
                "          or  WHERE transaction_date >= DATE '2026-08-01'")

        if selects_everything(sql):
            raise CostGuardError(
                "A bare SELECT * with no aggregation and no LIMIT reads whole "
                "files. Add a LIMIT, or aggregate with GROUP BY.")

    # ---- after the query ----
    def record(self, bytes_scanned):
        state = _load_state()
        state["bytes"] += int(bytes_scanned or 0)
        state["queries"] += 1
        _save_state(state)
        return state

    # ---- reporting ----
    @staticmethod
    def spend_usd(bytes_scanned):
        return (bytes_scanned / BYTES_PER_TB) * PRICE_PER_TB

    def describe_cost(self, bytes_scanned):
        """One line describing what a single query cost."""
        cost = self.spend_usd(bytes_scanned)
        if bytes_scanned < BYTES_PER_GB:
            size = f"{bytes_scanned / (1024 ** 2):.1f} MB"
        else:
            size = f"{bytes_scanned / BYTES_PER_GB:.2f} GB"
        cents = cost * 100
        price = f"${cost:.2f}" if cost >= 0.01 else f"{cents:.2f}c"
        return f"scanned {size} ({price})"

    def status(self):
        state = _load_state()
        spent = self.spend_usd(state["bytes"])
        gb = state["bytes"] / BYTES_PER_GB
        return (f"Athena spend today: ${spent:.2f} of ${self.daily_budget:.2f} "
                f"({gb:.2f} GB across {state['queries']} queries)")


# ---------------------------------------------------------------------------
# AWS-side setup - the guard that actually enforces
# ---------------------------------------------------------------------------

def setup_workgroup(name="pos-analytics", cutoff_gb=20, output=None,
                    region=None, profile=None):
    """
    Create or update an Athena workgroup with a per-query byte cutoff.

    This is the only limit AWS enforces itself. Application checks can be
    bypassed by anything that talks to Athena directly; this cannot.
    """
    import boto3

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    client = session.client("athena", region_name=region)

    cutoff_bytes = int(cutoff_gb * BYTES_PER_GB)
    conf = {
        "BytesScannedCutoffPerQuery": cutoff_bytes,
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": True,
        "RequesterPaysEnabled": False,
    }
    if output:
        conf["ResultConfiguration"] = {"OutputLocation": output}

    try:
        client.create_work_group(
            Name=name,
            Configuration=conf,
            Description="POS analytics - per-query scan limit enforced",
        )
        print(f"Created workgroup '{name}' with a {cutoff_gb} GB per-query cap "
              f"(about ${(cutoff_bytes / BYTES_PER_TB) * PRICE_PER_TB:.2f} "
              f"maximum per query).")
    except client.exceptions.InvalidRequestException as e:
        if "already exists" not in str(e):
            raise
        client.update_work_group(
            WorkGroup=name,
            ConfigurationUpdates={
                "BytesScannedCutoffPerQuery": cutoff_bytes,
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
            },
        )
        print(f"Updated workgroup '{name}' to a {cutoff_gb} GB per-query cap.")

    print()
    print("Point the MCP server at it by adding to athena_config.json:")
    print(f'  "workgroup": "{name}"')
    print()
    print("Anything exceeding the cap is cancelled by AWS mid-query, so the "
          "bill stops there rather than after the fact.")


def main():
    ap = argparse.ArgumentParser(description="Athena cost guard")
    ap.add_argument("--setup", action="store_true",
                    help="create/update the Athena workgroup with a scan cap")
    ap.add_argument("--name", default="pos-analytics")
    ap.add_argument("--cutoff-gb", type=float, default=20)
    ap.add_argument("--output", help="s3://bucket/athena-results/")
    ap.add_argument("--region")
    ap.add_argument("--profile")
    ap.add_argument("--status", action="store_true",
                    help="show today's recorded spend")
    ap.add_argument("--check", help="test a SQL string against the guard")
    args = ap.parse_args()

    guard = CostGuard()

    if args.setup:
        setup_workgroup(args.name, args.cutoff_gb, args.output,
                        args.region, args.profile)
    elif args.status:
        print(guard.status())
    elif args.check:
        try:
            guard.check(args.check)
            print("PASS - this query would be allowed.")
        except CostGuardError as e:
            print(f"BLOCKED - {e}")
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
