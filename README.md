# pos-analytics-pipeline

A data pipeline that takes raw point-of-sale transaction logs from a
multi-store grocery chain, lands them in S3, transforms them nightly with
Glue, and exposes them to Claude through an MCP server so the business team
can ask questions in plain English instead of filing report requests.

Built for a specialty grocery chain running Dumac RORC v7. **All data in this
repository is synthetic** - see [Data](#data) below.

Python, boto3, S3, Glue, Athena, Parquet, Anthropic MCP SDK.

## the problem

The POS writes one file per transaction to a folder on each store's back-office
server. Nobody can query it. Answering "which products are we losing money on"
meant asking someone to export a spreadsheet, and by the time it arrived the
question had moved on.

The chain wanted four things: movers, losses, seasonality, and historical
comparison. They already had a good experience connecting Claude to a NetSuite
MCP server, so the target was the same interaction model.

## how it works

Each store runs an upload agent that watches the export folder and ships files
to S3 as they appear. A nightly Glue job converts the day's raw CSV into
Parquet and builds two rollup tables. An MCP server answers questions by
routing each one to the smallest table that can answer it.

```
store server  ->  S3 raw/  ->  Glue (nightly)  ->  processed/ + rollups
                                                          |
                                          Athena  <-  MCP server  <-  Claude
```

Three separate AWS identities, each of which leaks a bounded amount if
compromised: stores can write but not read, the MCP server can read but not
write, the deploy key ships code but cannot see sales.

## what's in here

`upload_agent.py` - runs on each store's back-office server. Watches the folder
the POS drops TLOGs into and uploads as files appear. Tracks what it has sent
in a local SQLite db so restarts do not re-upload, waits for files to finish
being written before touching them, retries on network failure, and sweeps for
missed files on startup. Same script on every store, just swap the config.

`glue/tlog_etl.py` - the nightly job. Raw CSV to Parquet, plus a per-item daily
rollup and a per-department daily rollup. Python shell rather than Spark:
at this data size a distributed engine buys nothing, and Spark's ten-minute
minimum billing costs about $4.40/month for work that finishes in seconds.
This costs about $0.04. Every run ends by reconciling its own output against
the raw log and fails if they disagree.

`mcp_server.py` - the MCP server. Movers, losses, seasonality, tender mix,
promotions, dead stock, item search, guarded free-form SQL, and a
`verify_totals` tool that re-derives revenue four different ways to prove the
tables agree. Each tool routes to the smallest table that can answer it, which
is what keeps a typical question at a fraction of a cent.

`athena_cost_guard.py` - Athena bills $5 per terabyte scanned with no ceiling,
so one careless query against three years of data is a real bill. Three layers:
a workgroup-level per-query byte cap that AWS enforces and application code
cannot bypass, pre-flight checks that refuse queries with no partition filter,
and running spend accounting against a daily budget.

`demo/` - a self-contained local demo. Generates a synthetic catalogue and
transaction history, loads it into SQLite, and serves it over MCP. Runs with no
AWS account. See [demo/DEMO-RUNBOOK.md](demo/DEMO-RUNBOOK.md).

`scrub_check.py` - scans the working tree and git history for credentials,
client identifiers and real data exports. Exits non-zero so it can be wired in
as a pre-commit hook.

`provision_aws.py`, `deploy_etl.py`, `verify_access.py` - infrastructure setup,
code deployment, and IAM verification. `verify_access.py` uses IAM policy
simulation to confirm each credential can do exactly what it should and nothing
more, without needing live keys.

## try it

```bash
git clone https://github.com/gregPodstrelov/pos-analytics-pipeline
cd pos-analytics-pipeline/demo

pip install mcp
python3 make_sample_catalogue.py     # 12,000 synthetic products
python3 generate_demo_data.py        # transaction history in RORC TLOG format
python3 load_real_tlogs.py tlog_files/
python3 demo_mcp_server.py
```

Then point Claude Desktop at `demo/demo_mcp_server.py` and try the questions in
[demo/demo-questions.txt](demo/demo-questions.txt).

## design notes

**Why Parquet and rollups at all.** At four stores and under a megabyte a day,
the obvious answer is to skip the transformation and let Athena read the CSV.
That is right for recent-window queries, because Athena charges a 10 MB minimum
per query and a day of data is smaller than that either way. It stops being
right once queries reach back across the full history. At a 50/50 split of
recent versus historical questions and 500 queries a day, raw CSV runs about
$89/month by year three against $2.25/month for Parquet plus rollups.
Break-even is about five weeks of accumulated data.

**Why two rollups instead of one.** Measured, not assumed. A single combined
grain including both item and tender collapsed the data only 1.7x, because
most items sell once or twice a day per store. Splitting into an item rollup
and a department rollup gave 2.9x and 11.3x. The department rollup is what
makes multi-year questions cheap enough to be free.

**Partition layout is the whole game.** `store=X/year=Y/month=M/day=D` is worth
roughly 100x on scan cost. The cost guard refuses any query that does not
constrain a partition column, because the difference between a filtered and an
unfiltered query is a fraction of a cent against several dollars.

**Reconciliation is not optional.** A pipeline that quietly serves numbers that
are 0.5% wrong is worse than one that is down, because nobody investigates a
plausible-looking figure. Every run compares four independent derivations and
fails loudly rather than publishing.

## data

Everything in this repository is synthetic.

`demo/make_sample_catalogue.py` generates the product catalogue. It preserves
the schema, the long-tail demand curve, the department structure, seasonal
assignment and realistic price and margin bands of a real grocery price book,
because those properties are what make the demo worth looking at. It invents
every barcode, product name, brand, vendor, price and cost.

Barcodes use GS1 prefix `2`, reserved for in-store and variable-weight use and
never issued to a manufacturer, so no generated barcode can collide with a real
product.

The client's price book, transaction archive and movement reports are not in
this repository and are not in its history. `scrub_check.py --history` verifies
that.

`demo/build_item_master.py` is the code that processes a real price book
export. The code is here because handling that file - half the barcodes written
in scientific notation, a third of costs missing, categories recoverable only
through a secondary lookup - is most of the actual engineering. The input file
is not.
