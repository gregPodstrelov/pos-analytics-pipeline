#!/usr/bin/env python3
"""
POS TLOG Upload Agent
Watches a local folder for new TLOG files and uploads them to S3.
Organized in S3 by store and date: store-id/YYYY/MM/DD/filename
Run this on each store's back-office server.
"""

import os
import sys
import json
import time
import logging
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"upload_agent_{datetime.now().strftime('%Y%m')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


# ---------------------------------------------------------------------------
# Upload Tracker
# Keeps a local SQLite database so files are never uploaded twice,
# even if the agent restarts or the same file gets modified in place.
# ---------------------------------------------------------------------------

class UploadTracker:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_files (
                file_path   TEXT PRIMARY KEY,
                file_hash   TEXT,
                s3_key      TEXT,
                uploaded_at TEXT
            )
        """)
        self.conn.commit()

    def already_uploaded(self, file_path, file_hash):
        row = self.conn.execute(
            "SELECT file_hash FROM uploaded_files WHERE file_path = ?",
            (file_path,)
        ).fetchone()
        # File is considered already uploaded only if the hash matches.
        # If the file changed since last upload, we re-upload it.
        return row is not None and row[0] == file_hash

    def mark_uploaded(self, file_path, file_hash, s3_key):
        self.conn.execute("""
            INSERT OR REPLACE INTO uploaded_files
                (file_path, file_hash, s3_key, uploaded_at)
            VALUES (?, ?, ?, ?)
        """, (file_path, file_hash, s3_key, datetime.utcnow().isoformat()))
        self.conn.commit()


def compute_hash(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# S3 Uploader
# ---------------------------------------------------------------------------

class S3Uploader:
    def __init__(self, config):
        self.bucket    = config["s3_bucket"]
        self.store_id  = config["store_id"]
        self.client    = boto3.client(
            "s3",
            region_name          = config["aws_region"],
            aws_access_key_id    = config["aws_access_key"],
            aws_secret_access_key= config["aws_secret_key"]
        )

    def build_s3_key(self, file_path):
        """
        Builds the destination path in S3.
        Example: store-007/2026/08/06/tx080626.tlog
        """
        now      = datetime.utcnow()
        filename = Path(file_path).name
        return f"{self.store_id}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{filename}"

    def upload(self, file_path, max_retries=3, retry_delay=5):
        s3_key = self.build_s3_key(file_path)
        for attempt in range(1, max_retries + 1):
            try:
                self.client.upload_file(file_path, self.bucket, s3_key)
                logging.info(f"Uploaded: {file_path} -> s3://{self.bucket}/{s3_key}")
                return s3_key
            except ClientError as e:
                logging.error(f"Upload attempt {attempt}/{max_retries} failed for {file_path}: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)
        logging.error(f"All upload attempts failed for: {file_path}")
        return None


# ---------------------------------------------------------------------------
# File System Event Handler
# ---------------------------------------------------------------------------

class TLogHandler(FileSystemEventHandler):
    def __init__(self, config, uploader, tracker):
        self.uploader       = uploader
        self.tracker        = tracker
        # Strip the leading * from patterns like "*.tlog" -> ".tlog"
        raw_pattern         = config.get("file_pattern", "").lower()
        self.file_extension = raw_pattern.lstrip("*") if raw_pattern else ""
        self.settle_seconds = config.get("settle_seconds", 3)

    def should_process(self, path):
        """Ignore directories and files that don't match the configured extension."""
        if not os.path.isfile(path):
            return False
        if self.file_extension and not path.lower().endswith(self.file_extension):
            return False
        return True

    def wait_for_stable(self, path):
        """
        Wait until the file size stops changing.
        Prevents uploading a file that is still being written by the POS system.
        """
        prev_size = -1
        for _ in range(10):
            try:
                size = os.path.getsize(path)
            except OSError:
                return False
            if size == prev_size and size > 0:
                return True
            prev_size = size
            time.sleep(self.settle_seconds)
        return True

    def process(self, path):
        if not self.should_process(path):
            return
        if not self.wait_for_stable(path):
            logging.warning(f"File did not stabilize, skipping: {path}")
            return
        try:
            fhash = compute_hash(path)
            if self.tracker.already_uploaded(path, fhash):
                logging.debug(f"Already uploaded, skipping: {path}")
                return
            s3_key = self.uploader.upload(path)
            if s3_key:
                self.tracker.mark_uploaded(path, fhash, s3_key)
        except Exception as e:
            logging.error(f"Unexpected error processing {path}: {e}")

    def on_created(self, event):
        if not event.is_directory:
            self.process(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self.process(event.src_path)


# ---------------------------------------------------------------------------
# Startup scan
# Uploads any files already sitting in the folder when the agent starts.
# Catches files that arrived while the agent was offline.
# ---------------------------------------------------------------------------

def scan_existing_files(watch_folder, handler):
    logging.info("Scanning for existing files in watch folder...")
    for filename in os.listdir(watch_folder):
        full_path = os.path.join(watch_folder, filename)
        handler.process(full_path)
    logging.info("Startup scan complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    setup_logging(config.get("log_dir", "logs"))
    logging.info(f"Starting POS upload agent for store: {config['store_id']}")
    logging.info(f"Watching folder: {config['watch_folder']}")
    logging.info(f"Destination bucket: s3://{config['s3_bucket']}")

    tracker  = UploadTracker(config.get("db_path", "uploaded_files.db"))
    uploader = S3Uploader(config)
    handler  = TLogHandler(config, uploader, tracker)

    # Upload any files that arrived before the agent started
    scan_existing_files(config["watch_folder"], handler)

    # Start watching for new files
    observer = Observer()
    observer.schedule(handler, config["watch_folder"], recursive=False)
    observer.start()
    logging.info("Watching for new files. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutdown requested.")
        observer.stop()
    observer.join()
    logging.info("Upload agent stopped.")


if __name__ == "__main__":
    main()
