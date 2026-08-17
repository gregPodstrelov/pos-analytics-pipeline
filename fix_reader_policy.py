#!/usr/bin/env python3
"""
Grant the Athena reader access to the rollup prefixes.

The reader policy was written before the ETL existed, so it covers raw/,
processed/ and reference/ but not rollup_item/ or rollup_dept/. Nine of the
twelve MCP tools route to a rollup table, so almost every question fails with:

    PERMISSION_DENIED: pos-athena-reader is not authorized to perform
    s3:GetObject on .../rollup_dept/...

This rewrites the policy to cover all five data prefixes for reading, while
keeping the explicit Deny that stops the reader modifying any of them.

Needs admin credentials - run with an AWS profile that can call IAM:

    python3 fix_reader_policy.py                  # uses default profile
    python3 fix_reader_policy.py --profile admin
"""

import json
import sys
import argparse

try:
    import boto3
except ImportError:
    print("Needs boto3:  pip3 install boto3")
    sys.exit(1)

BUCKET   = "example-pos-datalake"
ACCOUNT  = "123456789012"
REGION   = "us-east-1"
DATABASE = "pos_analytics"
WORKGROUP = "pos-analytics"
USER     = "pos-athena-reader"
POLICY   = "pos-athena-read-only"

# Every prefix holding data the reader may read but must never modify.
DATA_PREFIXES = ["raw/", "processed/", "rollup_item/", "rollup_dept/",
                 "reference/"]


def build_policy():
    data_arns = [f"arn:aws:s3:::{BUCKET}/{p}*" for p in DATA_PREFIXES]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadData",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                "Resource": data_arns + [f"arn:aws:s3:::{BUCKET}/athena-results/*"],
            },
            {
                "Sid": "ListBucket",
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": f"arn:aws:s3:::{BUCKET}",
            },
            {
                # Athena has to put query output somewhere - this prefix only
                "Sid": "AthenaResultsWriteOnly",
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
                "Resource": f"arn:aws:s3:::{BUCKET}/athena-results/*",
            },
            {
                "Sid": "NoWritingToTheData",
                "Effect": "Deny",
                "Action": ["s3:PutObject", "s3:DeleteObject",
                           "s3:DeleteObjectVersion"],
                "Resource": data_arns,
            },
            {
                # Scoped to one workgroup, so the reader inherits its scan cap
                "Sid": "RunQueriesInThisWorkgroupOnly",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution", "athena:StopQueryExecution",
                    "athena:GetQueryExecution", "athena:GetQueryResults",
                    "athena:GetQueryResultsStream", "athena:GetWorkGroup",
                    "athena:ListQueryExecutions",
                ],
                "Resource":
                    f"arn:aws:athena:{REGION}:{ACCOUNT}:workgroup/{WORKGROUP}",
            },
            {
                "Sid": "ReadCatalogue",
                "Effect": "Allow",
                "Action": ["glue:GetDatabase", "glue:GetDatabases",
                           "glue:GetTable", "glue:GetTables",
                           "glue:GetPartition", "glue:GetPartitions"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:database/{DATABASE}",
                    f"arn:aws:glue:{REGION}:{ACCOUNT}:table/{DATABASE}/*",
                ],
            },
        ],
    }


CHECKS = [
    ("read rollup_dept",  "s3:GetObject",  f"arn:aws:s3:::{BUCKET}/rollup_dept/x.parquet", True),
    ("read rollup_item",  "s3:GetObject",  f"arn:aws:s3:::{BUCKET}/rollup_item/x.parquet", True),
    ("read processed",    "s3:GetObject",  f"arn:aws:s3:::{BUCKET}/processed/x.parquet",   True),
    ("read raw",          "s3:GetObject",  f"arn:aws:s3:::{BUCKET}/raw/f.dat",             True),
    ("write rollup_dept", "s3:PutObject",  f"arn:aws:s3:::{BUCKET}/rollup_dept/x.parquet", False),
    ("write processed",   "s3:PutObject",  f"arn:aws:s3:::{BUCKET}/processed/x.parquet",   False),
    ("delete raw",        "s3:DeleteObject", f"arn:aws:s3:::{BUCKET}/raw/f.dat",           False),
    ("write results",     "s3:PutObject",  f"arn:aws:s3:::{BUCKET}/athena-results/r.csv",  True),
    ("create IAM user",   "iam:CreateUser", "*",                                           False),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile) if args.profile \
        else boto3.Session()
    iam = session.client("iam")

    iam.put_user_policy(UserName=USER, PolicyName=POLICY,
                        PolicyDocument=json.dumps(build_policy()))
    print(f"policy {POLICY} updated on {USER}\n")

    arn = f"arn:aws:iam::{ACCOUNT}:user/{USER}"
    passed = 0
    for label, action, resource, expect in CHECKS:
        r = iam.simulate_principal_policy(
            PolicySourceArn=arn, ActionNames=[action], ResourceArns=[resource])
        decision = r["EvaluationResults"][0]["EvalDecision"]
        ok = (decision == "allowed") == expect
        passed += ok
        print(f"  {'PASS' if ok else 'FAIL'}  {label:<20} {decision:<14} "
              f"want {'allow' if expect else 'DENY'}")

    print(f"\n{passed}/{len(CHECKS)} checks passed")
    if passed == len(CHECKS):
        print("\nThe reader can now read every table but still cannot modify "
              "data or escalate privileges.\nRestart Claude Desktop and try a "
              "query.")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
