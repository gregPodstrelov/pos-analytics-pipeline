# MCP Server - Install Guide
This runs on the machine where Claude Desktop is installed (not on the store servers).
It connects Claude to Athena so the business team can ask questions in plain English.

---

## Step 1 - Install dependencies

Open a terminal and run:

```
pip install mcp boto3
```

If you get a permissions error on Mac:
```
pip install mcp boto3 --break-system-packages
```

---

## Step 2 - Fill in athena_config.json

Copy `athena_config.example.json` to `athena_config.json` and replace the placeholder values.

- database: `pos_raw_catalog` (the Glue database you created)
- output_bucket: the S3 path where Athena stores query results, e.g. `s3://yourcompany-pos-datalake/athena-results/`
- region: the region your bucket is in, e.g. `us-east-1`
- aws_access_key and aws_secret_key: the credentials for the `pos-mcp-server` IAM user

Keep this file private - it is gitignored for a reason.

---

## Step 3 - Test the server manually

Run this from the folder containing mcp_server.py:

```
python mcp_server.py
```

If it starts without errors, the server is working. Press Ctrl+C to stop it.

If you get a credentials error, check the keys in athena_config.json. If you get a permissions error, confirm the IAM user has both `AmazonAthenaFullAccess` and `AmazonS3ReadOnlyAccess`.

---

## Step 4 - Connect to Claude Desktop

Find the Claude Desktop config file:
- Mac: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

Open it in any text editor. If it does not exist, create it. Add this (replace the path with the actual full path to mcp_server.py):

```json
{
  "mcpServers": {
    "pos-analytics": {
      "command": "python",
      "args": ["/Users/yourname/path/to/mcp_server.py"]
    }
  }
}
```

If the file already has other MCP servers in it, add pos-analytics alongside them inside the mcpServers block.

---

## Step 5 - Restart Claude Desktop

Fully quit and reopen Claude Desktop. It will launch the MCP server automatically in the background.

Verify it connected by asking Claude "what tools do you have available?" - it should list the POS tools.

---

## What Claude can now answer

- "What were the top 20 movers in produce last month?"
- "Show me a loss report for all stores this week"
- "How does this month compare to the same month last year?"
- "Which store had the highest revenue last 30 days?"
- "Give me a sales breakdown by department for this year"
- "What are the bottom 10 selling items in frozen food?"
- "Compare all stores by average basket size last quarter"

Claude picks the right tool automatically, queries Athena, and responds in plain English.

---

## Available tools

`get_top_movers` - best or worst selling items, filterable by store and department
`get_loss_report` - voids, returns, and override events with dollar values by store
`get_seasonal_trends` - current period vs same period last year and two years ago
`get_sales_summary` - revenue breakdown by department, store, or day
`get_store_comparison` - all stores ranked by revenue, units, transactions, or average basket

All tools accept a period parameter: today, yesterday, last_7_days, last_30_days, last_week, this_month, last_month, last_90_days, this_year

---

## A note on query costs

Athena charges $5 per TB scanned. Because the table is partitioned by date and stored as Parquet, a typical query scans a tiny fraction of the data - usually well under a penny. Queries that omit a date filter will scan everything, so all the tools here always include one.
