#!/usr/bin/env python3
"""
Prove the two key pairs are actually scoped the way the policies claim.

Writing a policy and having it behave are different things. This exercises
both credential sets against the live bucket and reports what each one can
and cannot do. Every check states what it expects, so a surprise is obvious.

    python3 verify_access.py

Expected result:

    upload keys   CAN  write to raw/
                  CANNOT read anything
                  CANNOT write to processed/
    athena keys   CAN  read data, run Athena
                  CANNOT write to raw/ or processed/
"""

import io
import json
import sys
import uuid

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("This needs boto3:  pip3 install boto3")
    sys.exit(1)


PASS = "  PASS  "
FAIL = "  FAIL  "

results = []


def check(label, expected_allowed, fn):
    """
    Run one probe. `expected_allowed` says whether the call should succeed.
    A denial when we expected one is a pass.
    """
    try:
        fn()
        ok = expected_allowed
        detail = "allowed"
    except ClientError as e:
        code = e.response["Error"]["Code"]
        denied = code in ("AccessDenied", "AccessDeniedException",
                          "AllAccessDisabled", "InvalidAccessKeyId",
                          "UnauthorizedOperation", "Forbidden")
        ok = (not expected_allowed) and denied
        detail = f"denied ({code})"
    except Exception as e:                       # noqa: BLE001
        ok = False
        detail = f"error ({type(e).__name__}: {str(e)[:50]})"

    want = "should be allowed" if expected_allowed else "should be denied"
    print(f"{PASS if ok else FAIL}{label:52} {detail:28} {want}")
    results.append(ok)


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except OSError:
        print(f"Cannot find {path} - run provision_aws.py first.")
        sys.exit(1)


def main():
    up = load("upload_credentials.json")
    at = load("athena_config.json")

    bucket = up["s3_bucket"]
    region = up["aws_region"]
    raw = up.get("s3_prefix", "raw/")
    probe = f"{raw}_access_check_{uuid.uuid4().hex[:8]}.txt"

    up_s3 = boto3.client("s3", region_name=region,
                         aws_access_key_id=up["aws_access_key"],
                         aws_secret_access_key=up["aws_secret_key"])
    at_s3 = boto3.client("s3", region_name=at["region"],
                         aws_access_key_id=at["aws_access_key"],
                         aws_secret_access_key=at["aws_secret_key"])
    at_ath = boto3.client("athena", region_name=at["region"],
                          aws_access_key_id=at["aws_access_key"],
                          aws_secret_access_key=at["aws_secret_key"])

    print(f"\nBucket: {bucket}   region: {region}")
    print("=" * 100)
    print("\nUPLOAD KEYS  (belong on store back-office servers)")
    print("-" * 100)

    check("write a file into raw/", True,
          lambda: up_s3.put_object(Bucket=bucket, Key=probe, Body=b"probe"))

    check("read that same file back", False,
          lambda: up_s3.get_object(Bucket=bucket, Key=probe))

    check("write into processed/", False,
          lambda: up_s3.put_object(Bucket=bucket,
                                   Key="processed/_should_fail.txt",
                                   Body=b"x"))

    check("list the whole bucket", False,
          lambda: up_s3.list_objects_v2(Bucket=bucket, MaxKeys=1))

    check("list only raw/", True,
          lambda: up_s3.list_objects_v2(Bucket=bucket, Prefix=raw, MaxKeys=1))

    check("delete a file", False,
          lambda: up_s3.delete_object(Bucket=bucket, Key=probe))

    print("\nATHENA KEYS  (belong with the MCP server)")
    print("-" * 100)

    check("read the file the uploader wrote", True,
          lambda: at_s3.get_object(Bucket=bucket, Key=probe))

    check("write into raw/", False,
          lambda: at_s3.put_object(Bucket=bucket,
                                   Key=f"{raw}_should_fail.txt", Body=b"x"))

    check("write into processed/", False,
          lambda: at_s3.put_object(Bucket=bucket,
                                   Key="processed/_should_fail.txt", Body=b"x"))

    check("write into athena-results/", True,
          lambda: at_s3.put_object(Bucket=bucket,
                                   Key="athena-results/_probe.txt", Body=b"x"))

    check("delete data", False,
          lambda: at_s3.delete_object(Bucket=bucket, Key=probe))

    check("start an Athena query", True,
          lambda: at_ath.start_query_execution(
              QueryString="SELECT 1",
              WorkGroup=at.get("workgroup", "primary"),
              ResultConfiguration={"OutputLocation": at["output_bucket"]}))

    check("create an IAM user (privilege escalation)", False,
          lambda: boto3.client(
              "iam",
              aws_access_key_id=at["aws_access_key"],
              aws_secret_access_key=at["aws_secret_key"],
          ).create_user(UserName="should-never-work"))

    # Clean up with admin credentials if available
    try:
        admin = boto3.client("s3", region_name=region)
        for k in (probe, "athena-results/_probe.txt"):
            admin.delete_object(Bucket=bucket, Key=k)
        print("\n  probe files cleaned up")
    except Exception:
        print(f"\n  note: could not clean up {probe} - remove it manually")

    print("=" * 100)
    passed, total = sum(results), len(results)
    if passed == total:
        print(f"\nAll {total} checks passed. The two key pairs are properly "
              f"separated.\n")
    else:
        print(f"\n{passed}/{total} passed - {total - passed} FAILED. "
              f"Do not ship these keys until the failures are understood.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
