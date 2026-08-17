#!/usr/bin/env python3
"""
Upload the ETL script and item master to S3.

Both files have to come from this machine - the item master is derived from
the price book, and the ETL script needs to sit in S3 for Glue to run it.

    python3 deploy_etl.py
"""

import json
import os
import sys
import hashlib
import subprocess

try:
    import boto3
except ImportError:
    print("This needs boto3:  pip3 install boto3")
    sys.exit(1)

HERE = os.path.dirname(os.path.abspath(__file__))

UPLOAD_CFG = "upload_credentials.json"   # write-only into raw/ and reference/
DEPLOY_CFG = "deploy_credentials.json"   # write-only into scripts/

# Each file goes up with the narrowest credential that can carry it. The
# store keys handle reference data; a separate deploy key handles code. Neither
# can read the sales history back.
UPLOADS = [
    ("demo/item_master.csv", "reference/item_master.csv", "upload"),
    ("glue/tlog_etl.py",     "scripts/tlog_etl.py",       "deploy"),
]


def client_for(cfg):
    return boto3.client("s3", region_name=cfg["aws_region"],
                        aws_access_key_id=cfg["aws_access_key"],
                        aws_secret_access_key=cfg["aws_secret_key"])


def run_tests():
    """
    Never ship a script that fails its own tests. The ETL runs unattended at
    3am; a mistake found here costs a minute, the same mistake found in
    production costs a day of wrong numbers.
    """
    test = os.path.join(HERE, "glue", "test_tlog_etl.py")
    if not os.path.exists(test):
        print("  (no test file found, skipping)")
        return True
    r = subprocess.run([sys.executable, test], capture_output=True, text=True)
    tail = r.stdout.strip().splitlines()[-1:] or ["no output"]
    print(f"  tests: {tail[0]}")
    if r.returncode != 0:
        print(r.stdout)
        print("Tests failed - nothing uploaded.")
    return r.returncode == 0


def verify(client, bucket, key, local):
    """
    Confirm the object in S3 is byte-for-byte what is on disk.

    A previous deploy reported success while the script never landed, and the
    backfill then failed with "Script file doesn't exist" - which points at
    Glue rather than at the upload, so it costs an hour to diagnose. Comparing
    the ETag closes that gap.
    """
    with open(local, "rb") as f:
        local_md5 = hashlib.md5(f.read()).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        return False, f"not found after upload ({e})"
    etag = head["ETag"].strip('"')
    if "-" in etag:
        return True, "multipart, size checked only"      # ETag is not an md5
    if etag != local_md5:
        return False, "content in S3 does not match the local file"
    return True, "verified"


def main():
    print("checking the ETL before shipping it")
    if not run_tests():
        sys.exit(1)
    print()

    cfgs = {}
    for name, path in (("upload", UPLOAD_CFG), ("deploy", DEPLOY_CFG)):
        try:
            cfgs[name] = json.load(open(path))
        except OSError:
            print(f"Cannot find {path}")
            sys.exit(1)

    clients = {k: client_for(v) for k, v in cfgs.items()}
    bucket = cfgs["upload"]["s3_bucket"]
    ok = True

    for local, key, which in UPLOADS:
        if not os.path.exists(local):
            print(f"  MISSING  {local}")
            ok = False
            continue
        size = os.path.getsize(local)
        try:
            clients[which].upload_file(local, bucket, key)
        except Exception as e:
            print(f"  FAILED   {local}")
            print(f"    {e}")
            if "AccessDenied" in str(e):
                print("    -> the credential or the bucket policy is blocking "
                      "this prefix")
            ok = False
            continue

        good, note = verify(clients[which], bucket, key, local)
        print(f"  {'uploaded' if good else 'PROBLEM '} {local:<24} -> "
              f"{key:<28} {size/1024/1024:6.2f} MB  ({note})")
        ok = ok and good

    if not ok:
        sys.exit(1)

    print(f"\nBoth files are in s3://{bucket}/.")
    print("Rebuild every partition with the new schema:")
    print("  python3 deploy_etl.py --run-backfill")


def run_backfill():
    cfg = json.load(open(DEPLOY_CFG))
    glue = boto3.client("glue", region_name=cfg["aws_region"],
                        aws_access_key_id=cfg["aws_access_key"],
                        aws_secret_access_key=cfg["aws_secret_key"])
    r = glue.start_job_run(JobName=cfg["glue_job"],
                           Arguments={"--mode": "backfill"})
    print(f"started backfill: run {r['JobRunId']}")
    print("It rewrites every partition - expect several minutes.")
    print()
    print("The run ends by reconciling the rebuilt tables against the raw log.")
    print("If those disagree the job fails on purpose, so a FAILED state here")
    print("means the numbers are wrong, not that the job crashed. Check the")
    print("CloudWatch log for the reconcile block either way.")


if __name__ == "__main__":
    if "--run-backfill" in sys.argv:
        run_backfill()
    else:
        main()
