# Running the demo

Everything here runs locally against a synthetic dataset. No AWS account, no
credentials, no client data. It takes about two minutes end to end.

## 1. Build the catalogue

```bash
cd demo
python3 make_sample_catalogue.py
```

12,000 products across 18 departments, with a long-tail demand curve, seasonal
assignment, and realistic price and margin bands. Every barcode, name, brand,
vendor and price is invented. Barcodes use GS1 prefix `2`, which is reserved
for in-store use and never issued to a manufacturer, so nothing here can
collide with a real product.

## 2. Generate transactions

```bash
python3 generate_demo_data.py
```

Writes TLOG files in Dumac RORC format - one file per transaction, 36
comma-separated fields, the same layout the real POS exports. Demand follows
the catalogue's popularity curve, modulated by season, day of week and store
size, so the data has structure worth querying rather than uniform noise.

Set `HISTORY_DAYS` to control the span:

```bash
HISTORY_DAYS=400 python3 generate_demo_data.py
```

## 3. Load it

```bash
python3 load_real_tlogs.py tlog_files/
```

Parses the TLOGs into SQLite. This is the same parser the production Glue job
uses - `tlog_format.py` is shared between the generator and the reader, so if
the two ever disagree about the format, the tests fail rather than the data
silently drifting.

## 4. Point Claude at it

```bash
python3 demo_mcp_server.py
```

Then add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pos-demo": {
      "command": "python3",
      "args": ["/absolute/path/to/demo/demo_mcp_server.py"]
    }
  }
}
```

Restart Claude Desktop. See `demo-questions.txt` for questions to try.

## What the demo is showing

The point is not that Claude can run SQL. It is that the schema, the tool
definitions and the aggregation grain have been chosen so that a business
question maps onto exactly one query, and the answer comes back with the
qualifications a analyst would add - what was excluded, what was estimated,
what the number does not cover.

A few things worth looking at in the code:

**Sub-department mapping** (`glue/tlog_etl.py`). The transaction log writes a
three-digit sub-department where the price book uses a two-digit department.
Truncating to the first two digits does not work, because 239 belongs to
department 22 rather than 23. The mapping is learned by voting over items that
appear in both sources. Without it, every unmatched barcode invented its own
department and a department report came back with eighty near-empty rows.

**Totals computed separately from display** (`demo_mcp_server.py`). A search
that shows the top 25 matches still computes its total from an unlimited
aggregate. Deriving the total from the truncated list makes the arithmetic
depend on the display limit, which produces a number that is wrong in a way
nobody notices.

**Cost imputation rather than exclusion** (`build_item_master.py`). Items with
no cost in the price book get the department median instead of being dropped.
Dropping them is cleaner code and produces a movers report that is missing real
products.

**Reconciliation** (`glue/tlog_etl.py`, `mcp_server.py`). Every ETL run
re-derives its output four ways and fails if they disagree. The raw-log check
re-implements the sale classifier in SQL rather than reusing the Python, so the
two are genuinely independent.
