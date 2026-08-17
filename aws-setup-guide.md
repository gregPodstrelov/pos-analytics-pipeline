# AWS Infrastructure Setup - POS Data Pipeline
Step-by-step via AWS Console. Complete these in order.

This uses S3 + Glue + Athena. There is no database cluster to provision - Athena queries the data directly in S3 and you pay per query instead of per hour.

---

## Step 1 - Create S3 Buckets

You need one bucket for the data lake and one folder inside it for Athena query results.

1. Go to https://console.aws.amazon.com and sign in
2. In the search bar at the top, type **S3** and click it
3. Click **Create bucket**
4. Set the bucket name - use something like `yourcompany-pos-datalake` (must be globally unique, all lowercase, no spaces)
5. Choose a region - pick **us-east-1** (N. Virginia) unless you have a reason to use another
6. Under "Block Public Access settings" - leave all four checkboxes checked (default). This keeps the bucket private.
7. Under "Default encryption" - leave it on **SSE-S3**. This encrypts files at rest automatically.
8. Leave everything else as default
9. Click **Create bucket**

Once created, open the bucket and create two folders:
- `raw/` - where the store upload agents drop TLOG files
- `processed/` - where Glue writes the cleaned Parquet files that Athena queries
- `athena-results/` - where Athena stores query output

The upload agent creates the store/date subfolder structure automatically under `raw/` (format: `raw/store-001/2026/08/06/filename.tlog`).

---

## Step 2 - Create IAM Roles

IAM roles control what each piece of the system is allowed to do. You need three.

### Role 1 - Store Upload Agent
This is what the script at each store uses to push files to S3. Write access only.

1. In the AWS search bar, type **IAM** and click it
2. In the left menu, click **Users**, then **Create user**
3. Name it `pos-upload-agent-store-001` (create one per store so you can revoke access individually)
4. Click **Next**
5. Select **Attach policies directly**, search for and check **AmazonS3FullAccess**
   - Note: this is broader than needed. Tighten it to your specific bucket after setup.
6. Click **Next**, then **Create user**
7. Click into the user, go to **Security credentials**, click **Create access key**
8. Choose **Application running outside AWS**, click **Next**, then **Create access key**
9. Copy the access key and secret - these go into that store's `config.json`

### Role 2 - Glue Service Role
This is what AWS Glue uses to read raw files and write processed ones.

1. In IAM, click **Roles**, then **Create role**
2. Under "Trusted entity type" select **AWS service**, then choose **Glue**
3. Click **Next**
4. Add these two policies:
   - `AmazonS3FullAccess`
   - `AWSGlueServiceRole`
5. Click **Next**
6. Name the role: `pos-glue-service-role`
7. Click **Create role**

### Role 3 - MCP Server User
This is what the MCP server uses to run Athena queries.

1. In IAM, click **Users**, then **Create user**
2. Name it `pos-mcp-server`
3. Click **Next**, select **Attach policies directly**
4. Search for and add:
   - `AmazonAthenaFullAccess`
   - `AmazonS3ReadOnlyAccess`
5. Click **Next**, then **Create user**
6. Create an access key the same way as above - these go into `athena_config.json`

---

## Step 3 - Create the Glue Database

This is the catalog that tells Athena what tables exist and where the data lives.

1. In the AWS search bar, type **Glue** and click it
2. In the left menu, click **Databases** (under Data Catalog)
3. Click **Add database**
4. Name it: `pos_raw_catalog`
5. Click **Create**

---

## Step 4 - Create the Athena Table

Athena needs a table definition pointing at your processed data in S3. This is a draft - column names will be adjusted once you receive the TLOG field spec from Dumac.

1. In the AWS search bar, type **Athena** and click it
2. If prompted, set the query result location to `s3://yourcompany-pos-datalake/athena-results/`
3. In the query editor, select database `pos_raw_catalog`
4. Run this to create the table:

```sql
CREATE EXTERNAL TABLE fact_transactions (
    transaction_id          STRING,
    store_id                STRING,
    store_name              STRING,
    transaction_date        TIMESTAMP,
    cashier_id              STRING,
    register_id             STRING,
    item_id                 STRING,
    item_desc               STRING,
    department_code         STRING,
    department_name         STRING,
    category_code           STRING,
    category_name           STRING,
    transaction_type        STRING,
    transaction_type_name   STRING,
    is_loss_event           BOOLEAN,
    quantity                DECIMAL(10,3),
    unit_price              DECIMAL(10,2),
    extended_price          DECIMAL(10,2),
    discount_amount         DECIMAL(10,2),
    tender_type             STRING,
    raw_file_source         STRING
)
PARTITIONED BY (
    year    INT,
    month   INT,
    day     INT
)
STORED AS PARQUET
LOCATION 's3://yourcompany-pos-datalake/processed/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Note the table is flat rather than a star schema. Athena works better with denormalized data - joins across separate dimension tables mean scanning more S3 objects, which costs more per query. Glue handles the lookups during processing and writes everything into one wide table.

The `PARTITIONED BY` clause matters a lot for cost. Athena charges $5 per TB scanned, and partitioning by date means a query for last month only scans last month's files instead of every file in the bucket.

---

## Step 5 - Set Up Partition Discovery

After Glue writes new data, Athena needs to know the new partitions exist.

Run this in Athena whenever new data lands (or schedule it):

```sql
MSCK REPAIR TABLE fact_transactions;
```

Alternatively, set up a Glue Crawler to do this automatically:

1. In Glue, click **Crawlers**, then **Create crawler**
2. Name it `pos-partition-crawler`
3. Data source: `s3://yourcompany-pos-datalake/processed/`
4. IAM role: `pos-glue-service-role`
5. Target database: `pos_raw_catalog`
6. Schedule: **Hourly** (or match however often data lands)
7. Click **Create crawler**

---

## Step 6 - Enable CloudWatch Logging

This lets you see what's happening across the pipeline - file arrivals, Glue job runs, errors.

1. In the AWS search bar, type **CloudWatch** and click it
2. In the left menu, click **Log groups**
3. Click **Create log group**
4. Name it: `/pos-pipeline/glue-jobs`
5. Set retention: **90 days**
6. Click **Create**

Repeat for a second log group:
- Name: `/pos-pipeline/upload-agent`
- Retention: 90 days

---

## What You Have Now

- An S3 bucket ready to receive TLOG files from every store
- IAM users and roles controlling what each component can access
- A Glue catalog and Athena table ready to query processed data
- A crawler keeping partitions up to date
- CloudWatch log groups for monitoring

The remaining piece is the Glue ETL job that parses TLOG files into Parquet. That is blocked until the vendor delivers the TLOG field specification and sample files.

---

## Costs to Expect

- S3: roughly $0.023 per GB per month. A few years of POS transaction data compressed to Parquet is typically a few GB.
- Athena: $5 per TB scanned. With date partitioning and Parquet compression, typical queries scan well under a GB - fractions of a cent each.
- Glue: charged per job run at $0.44 per DPU-hour. Small daily jobs run a few dollars a month.
- CloudWatch: negligible at this log volume.

The main advantage over Redshift is there is no fixed hourly cost. A Redshift dc2.large cluster runs about $180/month whether you query it or not. This setup costs close to nothing when idle.
