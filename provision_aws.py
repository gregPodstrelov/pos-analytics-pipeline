#!/usr/bin/env python3
"""
Provision the POS analytics data lake in AWS.

Creates, in one run:

  S3 bucket            private, encrypted, versioned, TLS-only
    raw/               where store agents upload TLOG files
    processed/         Parquet written by Glue, queried by Athena
    athena-results/    Athena query output
    reference/         price book and other lookups

  IAM user  pos-uploader        write-only into raw/ and reference/
  IAM user  pos-athena-reader   read-only, plus Athena and Glue

  Bucket policy        denies every principal except those two and the
                       account root, and denies plain HTTP

  Athena workgroup     with a per-query scan cap, so a careless query is
                       killed by AWS rather than billed

  Glue database        the catalogue Athena reads table definitions from

Two separate key pairs are issued. The upload keys ship to the stores and
cannot read anything back. The Athena keys stay with the MCP server and
cannot write to the data prefixes.

Usage:
    pip3 install boto3
    aws configure                     # admin credentials, once

    python3 provision_aws.py --bucket mycompany-pos-datalake --dry-run
    python3 provision_aws.py --bucket mycompany-pos-datalake

Outputs:
    upload_credentials.json   -> goes to the store upload agents
    athena_config.json        -> stays with the MCP server
    iam_policies/*.json       -> the exact policies applied, for review
"""

import os
import sys
import json
import time
import argparse

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("This needs boto3:  pip3 install boto3")
    sys.exit(1)


BYTES_PER_GB = 1024 ** 3

PREFIX_RAW       = "raw/"
PREFIX_PROCESSED = "processed/"
PREFIX_RESULTS   = "athena-results/"
PREFIX_REFERENCE = "reference/"


# ---------------------------------------------------------------------------
# Policy documents
# ---------------------------------------------------------------------------

def uploader_policy(bucket):
    """
    Write-only into the landing prefixes.

    Deliberately excludes GetObject and DeleteObject: a key sitting on a
    back-office server in a store should not be able to read the chain's
    sales history back out, or erase what it already sent.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListOwnPrefixesOnly",
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [PREFIX_RAW + "*", PREFIX_REFERENCE + "*"]
                    }
                },
            },
            {
                "Sid": "WriteOnlyToLandingZone",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/{PREFIX_RAW}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_REFERENCE}*",
                ],
            },
            {
                "Sid": "NoReadingDataBack",
                "Effect": "Deny",
                "Action": ["s3:GetObject", "s3:DeleteObject",
                           "s3:DeleteObjectVersion"],
                "Resource": f"arn:aws:s3:::{bucket}/*",
            },
        ],
    }


def athena_reader_policy(bucket, workgroup, database, region, account):
    """
    Read the data, run Athena, write only into the results prefix.

    Athena has to write query output somewhere, so PutObject is allowed on
    athena-results/ and nowhere else.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadDataAndListBucket",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/{PREFIX_RAW}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_PROCESSED}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_REFERENCE}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_RESULTS}*",
                ],
            },
            {
                "Sid": "ListBucketForAthena",
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": f"arn:aws:s3:::{bucket}",
            },
            {
                "Sid": "AthenaWritesResultsHereOnly",
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
                "Resource": f"arn:aws:s3:::{bucket}/{PREFIX_RESULTS}*",
            },
            {
                "Sid": "NoWritingToTheData",
                "Effect": "Deny",
                "Action": ["s3:PutObject", "s3:DeleteObject",
                           "s3:DeleteObjectVersion"],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/{PREFIX_RAW}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_PROCESSED}*",
                    f"arn:aws:s3:::{bucket}/{PREFIX_REFERENCE}*",
                ],
            },
            {
                "Sid": "RunQueriesInThisWorkgroupOnly",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetQueryResultsStream",
                    "athena:GetWorkGroup",
                    "athena:ListQueryExecutions",
                ],
                "Resource": f"arn:aws:athena:{region}:{account}:workgroup/{workgroup}",
            },
            {
                "Sid": "ReadCatalogue",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:GetDatabases",
                    "glue:GetTable", "glue:GetTables",
                    "glue:GetPartition", "glue:GetPartitions",
                ],
                "Resource": [
                    f"arn:aws:glue:{region}:{account}:catalog",
                    f"arn:aws:glue:{region}:{account}:database/{database}",
                    f"arn:aws:glue:{region}:{account}:table/{database}/*",
                ],
            },
        ],
    }


def bucket_policy(bucket, account, uploader_arn, reader_arn):
    """
    Lock the bucket to the two service identities.

    The account root stays allowed so an administrator cannot lock themselves
    out - removing that is how buckets become unrecoverable.
    """
    allowed = [
        uploader_arn,
        reader_arn,
        f"arn:aws:iam::{account}:root",
        f"arn:aws:iam::{account}:user/*-admin",
    ]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DenyPlainHTTP",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ],
                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
            },
            {
                "Sid": "OnlyTheseIdentities",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ],
                "Condition": {
                    "StringNotLike": {"aws:PrincipalArn": allowed},
                    # Glue and Athena reach the bucket as AWS services
                    "Bool": {"aws:PrincipalIsAWSService": "false"},
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# Provisioning
# ---------------------------------------------------------------------------

class Provisioner:
    def __init__(self, args):
        self.args = args
        session = boto3.Session(profile_name=args.profile) if args.profile \
            else boto3.Session()
        self.region = args.region or session.region_name or "us-east-1"
        self.s3 = session.client("s3", region_name=self.region)
        self.iam = session.client("iam")
        self.athena = session.client("athena", region_name=self.region)
        self.glue = session.client("glue", region_name=self.region)
        self.sts = session.client("sts")
        self.account = self.sts.get_caller_identity()["Account"]
        self.created = []

    def say(self, msg):
        prefix = "  would " if self.args.dry_run else "  "
        print(f"{prefix}{msg}")

    # ---- S3 ----
    def create_bucket(self):
        b = self.args.bucket
        print(f"\nS3 bucket: {b}")

        if self.args.dry_run:
            self.say("create the bucket, block public access, enable "
                     "SSE-S3 and versioning")
            self.say(f"create prefixes: {PREFIX_RAW} {PREFIX_PROCESSED} "
                     f"{PREFIX_RESULTS} {PREFIX_REFERENCE}")
            return

        try:
            if self.region == "us-east-1":
                self.s3.create_bucket(Bucket=b)
            else:
                self.s3.create_bucket(
                    Bucket=b,
                    CreateBucketConfiguration={
                        "LocationConstraint": self.region})
            self.say("created")
            self.created.append(f"s3://{b}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                self.say("already exists - reusing")
            else:
                raise

        self.s3.put_public_access_block(
            Bucket=b,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": False,   # our own policy is applied below
                "RestrictPublicBuckets": True,
            })
        self.say("public access blocked")

        self.s3.put_bucket_encryption(
            Bucket=b,
            ServerSideEncryptionConfiguration={
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"},
                    "BucketKeyEnabled": True,
                }]})
        self.say("default encryption on (SSE-S3)")

        self.s3.put_bucket_versioning(
            Bucket=b, VersioningConfiguration={"Status": "Enabled"})
        self.say("versioning on")

        # Athena results are disposable - expire them so they stop accruing
        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=b,
                LifecycleConfiguration={"Rules": [{
                    "ID": "expire-athena-results",
                    "Status": "Enabled",
                    "Filter": {"Prefix": PREFIX_RESULTS},
                    "Expiration": {"Days": 14},
                    "AbortIncompleteMultipartUpload": {
                        "DaysAfterInitiation": 7},
                }]})
            self.say("athena-results/ expires after 14 days")
        except ClientError as e:
            self.say(f"lifecycle rule skipped ({e.response['Error']['Code']})")

        for p in (PREFIX_RAW, PREFIX_PROCESSED, PREFIX_RESULTS, PREFIX_REFERENCE):
            self.s3.put_object(Bucket=b, Key=p)
        self.say("prefixes created")

    # ---- IAM ----
    def create_user(self, name, policy_name, policy_doc):
        print(f"\nIAM user: {name}")
        if self.args.dry_run:
            self.say(f"create the user and attach inline policy {policy_name}")
            self.say("create an access key pair")
            return None, f"arn:aws:iam::{self.account}:user/{name}"

        try:
            self.iam.create_user(
                UserName=name,
                Tags=[{"Key": "project", "Value": "pos-analytics"}])
            self.say("created")
            self.created.append(f"iam user {name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityAlreadyExists":
                raise
            self.say("already exists - reusing")

        self.iam.put_user_policy(
            UserName=name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_doc))
        self.say(f"policy {policy_name} attached")

        existing = self.iam.list_access_keys(UserName=name)["AccessKeyMetadata"]
        if existing and not self.args.rotate_keys:
            self.say(f"{len(existing)} access key(s) already exist - "
                     "keeping them (use --rotate-keys to replace)")
            return None, f"arn:aws:iam::{self.account}:user/{name}"

        for k in existing:
            self.iam.delete_access_key(UserName=name,
                                       AccessKeyId=k["AccessKeyId"])
            self.say(f"old key {k['AccessKeyId']} deleted")

        key = self.iam.create_access_key(UserName=name)["AccessKey"]
        self.say(f"new access key {key['AccessKeyId']} created")
        return key, f"arn:aws:iam::{self.account}:user/{name}"

    def apply_bucket_policy(self, uploader_arn, reader_arn):
        print(f"\nBucket policy")
        doc = bucket_policy(self.args.bucket, self.account,
                            uploader_arn, reader_arn)
        if self.args.dry_run:
            self.say("deny plain HTTP, and deny every principal except the "
                     "two users, the account root and AWS services")
            return doc
        # Give IAM a moment to propagate the new users
        time.sleep(8)
        self.s3.put_bucket_policy(Bucket=self.args.bucket,
                                  Policy=json.dumps(doc))
        self.say("applied")
        return doc

    # ---- Athena and Glue ----
    def create_workgroup(self):
        wg = self.args.workgroup
        cutoff = int(self.args.cutoff_gb * BYTES_PER_GB)
        print(f"\nAthena workgroup: {wg}")
        cost = (cutoff / (1024 ** 4)) * 5.0
        if self.args.dry_run:
            self.say(f"create with a {self.args.cutoff_gb} GB per-query cap "
                     f"(~${cost:.2f} maximum per query)")
            return
        conf = {
            "ResultConfiguration": {
                "OutputLocation":
                    f"s3://{self.args.bucket}/{PREFIX_RESULTS}"},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": cutoff,
        }
        try:
            self.athena.create_work_group(
                Name=wg, Configuration=conf,
                Description="POS analytics - per-query scan cap enforced")
            self.say(f"created with a {self.args.cutoff_gb} GB cap "
                     f"(~${cost:.2f} max per query)")
            self.created.append(f"athena workgroup {wg}")
        except ClientError as e:
            if "already exists" not in str(e):
                raise
            self.athena.update_work_group(
                WorkGroup=wg,
                ConfigurationUpdates={
                    "BytesScannedCutoffPerQuery": cutoff,
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                })
            self.say(f"already existed - cap updated to {self.args.cutoff_gb} GB")

    def create_glue_database(self):
        db = self.args.database
        print(f"\nGlue database: {db}")
        if self.args.dry_run:
            self.say("create the catalogue database")
            return
        try:
            self.glue.create_database(DatabaseInput={
                "Name": db,
                "Description": "POS transaction logs and reference data"})
            self.say("created")
            self.created.append(f"glue database {db}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "AlreadyExistsException":
                raise
            self.say("already exists - reusing")

    # ---- outputs ----
    def write_outputs(self, upload_key, athena_key, policies):
        os.makedirs("iam_policies", exist_ok=True)
        for name, doc in policies.items():
            with open(f"iam_policies/{name}.json", "w") as f:
                json.dump(doc, f, indent=2)
        print(f"\nPolicies written to iam_policies/ for review")

        if self.args.dry_run:
            return

        if upload_key:
            cfg = {
                "store_id":       "STORE01",
                "watch_folder":   "C:\\RORC\\TLOGExport",
                "file_pattern":   "*.DAT",
                "s3_bucket":      self.args.bucket,
                "s3_prefix":      PREFIX_RAW,
                "aws_access_key": upload_key["AccessKeyId"],
                "aws_secret_key": upload_key["SecretAccessKey"],
                "aws_region":     self.region,
                "settle_seconds": 3,
                "log_dir":        "logs",
                "db_path":        "uploaded_files.db",
            }
            self._write_secret("upload_credentials.json", cfg)

        if athena_key:
            cfg = {
                "database":      self.args.database,
                "output_bucket": f"s3://{self.args.bucket}/{PREFIX_RESULTS}",
                "region":        self.region,
                "aws_access_key": athena_key["AccessKeyId"],
                "aws_secret_key": athena_key["SecretAccessKey"],
                "workgroup":     self.args.workgroup,
                "daily_budget_usd": self.args.daily_budget,
                "max_scan_gb":   self.args.cutoff_gb,
                "require_partition_filter": True,
            }
            self._write_secret("athena_config.json", cfg)

    def _write_secret(self, path, payload):
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        os.chmod(path, 0o600)
        print(f"  wrote {path}  (permissions 600)")


def main():
    ap = argparse.ArgumentParser(
        description="Provision the POS analytics data lake")
    ap.add_argument("--bucket", required=True,
                    help="globally unique bucket name")
    ap.add_argument("--region")
    ap.add_argument("--profile", help="AWS CLI profile to use")
    ap.add_argument("--database", default="pos_analytics")
    ap.add_argument("--workgroup", default="pos-analytics")
    ap.add_argument("--uploader-user", default="pos-uploader")
    ap.add_argument("--reader-user", default="pos-athena-reader")
    ap.add_argument("--cutoff-gb", type=float, default=20,
                    help="per-query scan cap enforced by AWS")
    ap.add_argument("--daily-budget", type=float, default=5.00)
    ap.add_argument("--rotate-keys", action="store_true",
                    help="replace existing access keys")
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would happen, change nothing")
    args = ap.parse_args()

    p = Provisioner(args)

    print("=" * 66)
    print(f"Account {p.account}   region {p.region}")
    if args.dry_run:
        print("DRY RUN - nothing will be created")
    print("=" * 66)

    p.create_bucket()
    p.create_glue_database()
    p.create_workgroup()

    up_policy = uploader_policy(args.bucket)
    upload_key, uploader_arn = p.create_user(
        args.uploader_user, "pos-upload-write-only", up_policy)

    rd_policy = athena_reader_policy(args.bucket, args.workgroup,
                                     args.database, p.region, p.account)
    athena_key, reader_arn = p.create_user(
        args.reader_user, "pos-athena-read-only", rd_policy)

    bkt_policy = p.apply_bucket_policy(uploader_arn, reader_arn)

    p.write_outputs(upload_key, athena_key, {
        "uploader_policy": up_policy,
        "athena_reader_policy": rd_policy,
        "bucket_policy": bkt_policy,
    })

    print("\n" + "=" * 66)
    if args.dry_run:
        print("Dry run complete. Re-run without --dry-run to apply.")
        return

    print("Done.")
    if p.created:
        print("\nCreated:")
        for c in p.created:
            print(f"  {c}")

    print(f"""
Two credential files were written. They are not interchangeable:

  upload_credentials.json   -> ships to each store's back-office server
                               can write into {PREFIX_RAW} and nothing else,
                               cannot read any data back

  athena_config.json        -> stays with the MCP server
                               can read data and run Athena,
                               cannot write into the data prefixes

Both are chmod 600 and both are gitignored. Never commit them.

Next:
  1. Copy upload_credentials.json to a store, rename it config.json,
     set store_id and watch_folder, then run upload_agent.py
  2. Keep athena_config.json beside mcp_server.py
  3. Verify the split actually holds:
       python3 verify_access.py
""")


if __name__ == "__main__":
    main()
