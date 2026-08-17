#!/usr/bin/env python3
"""
Bulk-upload the historical TLOG archive to S3.

The watch-folder agent (upload_agent.py) handles the ongoing daily trickle.
This is the one-time backfill for everything already on disk.

Files land in a Hive-style partition layout so Athena can prune on date:

    raw/store=0000000003/year=2026/month=08/day=12/TLOG_....DAT

Partitioning is what keeps queries cheap. Without it, a question about last
week reads every file in the bucket; with it, Athena opens one day's folder.

Usage:
    pip3 install boto3
    python3 upload_archive.py ~/Downloads/archive

    python3 upload_archive.py ~/Downloads/archive --since 2026-01-01
    python3 upload_archive.py ~/Downloads/archive --dry-run
"""

import os
import re
import sys
import glob
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import ClientError
except ImportError:
    print("This needs boto3:  pip3 install boto3")
    sys.exit(1)

CONFIG = "upload_credentials.json"

# Filenames look like TLOG_2026-08-12_20261308030117_0000000003.DAT
NAME_RE = re.compile(r"TLOG_(\d{4})-(\d{2})-(\d{2})_\d+_(\d+)\.DAT", re.I)

_print_lock = threading.Lock()


def parse_name(filename):
    """Pull the date and store code out of the filename."""
    m = NAME_RE.match(os.path.basename(filename))
    if not m:
        return None
    y, mo, d, store = m.groups()
    return {"year": y, "month": mo, "day": d, "store": store}


def s3_key(meta, filename):
    return (f"raw/store={meta['store']}/year={meta['year']}"
            f"/month={meta['month']}/day={meta['day']}/{os.path.basename(filename)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", help="folder of TLOG_*.DAT files")
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--since", help="only upload files dated on/after YYYY-MM-DD")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.archive):
        print(f"Not a folder: {args.archive}")
        sys.exit(1)

    try:
        with open(args.config) as f:
            cfg = json.load(f)
    except OSError:
        print(f"Cannot find {args.config} - it holds the upload credentials.")
        sys.exit(1)

    bucket = cfg["s3_bucket"]

    files = sorted(glob.glob(os.path.join(args.archive, "*.DAT")) +
                   glob.glob(os.path.join(args.archive, "*.dat")))
    files = [f for f in files if os.path.getsize(f) > 0]

    since = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else None

    work, skipped = [], 0
    for f in files:
        meta = parse_name(f)
        if not meta:
            skipped += 1
            continue
        if since:
            d = datetime(int(meta["year"]), int(meta["month"]),
                         int(meta["day"])).date()
            if d < since:
                continue
        work.append((f, s3_key(meta, f)))

    total_bytes = sum(os.path.getsize(f) for f, _ in work)
    print(f"Archive:  {args.archive}")
    print(f"Bucket:   s3://{bucket}/raw/")
    print(f"Files:    {len(work):,} to upload"
          + (f", {skipped} unrecognised names skipped" if skipped else ""))
    print(f"Size:     {total_bytes / 1024 / 1024:.0f} MB")

    if not work:
        print("Nothing to do.")
        return

    print(f"\nExample destination:\n  {work[0][1]}\n")

    if args.dry_run:
        print("Dry run - nothing uploaded.")
        return

    s3 = boto3.client(
        "s3",
        region_name=cfg["aws_region"],
        aws_access_key_id=cfg["aws_access_key"],
        aws_secret_access_key=cfg["aws_secret_key"],
    )
    transfer = TransferConfig(multipart_threshold=16 * 1024 * 1024,
                              max_concurrency=4)

    done = [0]
    failed = []

    def put(item):
        path, key = item
        try:
            s3.upload_file(path, bucket, key, Config=transfer)
            with _print_lock:
                done[0] += 1
                if done[0] % 25 == 0 or done[0] == len(work):
                    pct = done[0] / len(work) * 100
                    print(f"  {done[0]:,}/{len(work):,}  ({pct:.0f}%)", flush=True)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            with _print_lock:
                failed.append((os.path.basename(path), code))
                if code in ("AccessDenied", "InvalidAccessKeyId"):
                    print(f"  ACCESS DENIED on {os.path.basename(path)} - "
                          f"check the upload credentials", flush=True)

    print("Uploading...")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(as_completed(pool.submit(put, w) for w in work))

    print()
    print(f"Uploaded {done[0]:,} of {len(work):,} files")
    if failed:
        print(f"{len(failed)} failed:")
        for name, code in failed[:10]:
            print(f"  {name}  {code}")
        sys.exit(1)

    print(f"""
Done. Next, register the partitions so Athena can see them:

  In the Athena console, or ask Claude to run it:
    MSCK REPAIR TABLE tlog_raw;
""")


if __name__ == "__main__":
    main()
