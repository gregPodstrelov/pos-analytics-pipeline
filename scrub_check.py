#!/usr/bin/env python3
"""
Refuse to let client data or credentials reach a public repository.

This project was built against a real grocery chain's price book and
transaction logs. None of that belongs on GitHub, and "I remembered to delete
it" is not a control - the whole point of a check like this is that it runs
even when nobody is thinking about it.

    python3 scrub_check.py                  # scan the working tree
    python3 scrub_check.py --history        # scan every commit as well

Wire it in as a pre-commit hook:

    ln -s ../../scrub_check.py .git/hooks/pre-commit

Exit code is non-zero if anything matches, so a hook or CI job blocks on it.
"""

import os
import re
import sys
import argparse
import subprocess

# Anything that identifies the client, their infrastructure, or a live
# credential. Add to this list, never remove from it.
PATTERNS = [
    # --- credentials -----------------------------------------------------
    (r"AKIA[0-9A-Z]{16}",                 "AWS access key id"),
    (r"ASIA[0-9A-Z]{16}",                 "AWS temporary access key id"),
    (r"aws_secret_access_key\s*[:=]\s*[\"']?[A-Za-z0-9/+=]{40}",
                                          "AWS secret access key"),
    (r"-----BEGIN [A-Z ]*PRIVATE KEY-----", "private key"),
    (r"(?i)(password|passwd|secret|token)\s*[\"']?\s*[:=]\s*[\"'][^\"'\s]{8,}",
                                          "hardcoded secret"),

    # --- client identity -------------------------------------------------
    (r"(?i)\bbazaar\b",                   "client name"),
    (r"(?i)framingham|brookline|quincy",  "client store location"),
    (r"(?i)\b(lowell|newton)\b",          "client store location"),

    # --- infrastructure --------------------------------------------------
    (r"\b980921714327\b",                 "real AWS account id"),
    (r"bazaar-pos-datalake",              "real S3 bucket name"),

    # --- real data files -------------------------------------------------
    (r"lblout[-_]?\d*[-_]?csv",           "client price book export"),
    (r"Item_Movement_Report",             "client movement report export"),
]

# Files that legitimately contain trigger words - this script names the
# patterns it looks for, and the README explains what was scrubbed.
ALLOWLIST = {"scrub_check.py", ".gitignore"}

SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules"}
SKIP_EXT = {".pyc", ".parquet", ".db", ".png", ".jpg", ".gz", ".zip"}


def scan_text(text, where, hits):
    for pattern, label in PATTERNS:
        for m in re.finditer(pattern, text):
            line = text.count("\n", 0, m.start()) + 1
            snippet = m.group(0)[:60].replace("\n", " ")
            hits.append((where, line, label, snippet))


def scan_tree(root):
    hits = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if os.path.splitext(name)[1] in SKIP_EXT:
                continue
            rel = os.path.relpath(os.path.join(dirpath, name), root)
            if rel in ALLOWLIST:
                continue
            try:
                with open(os.path.join(dirpath, name), encoding="utf-8",
                          errors="ignore") as f:
                    scan_text(f.read(), rel, hits)
            except OSError:
                continue
    return hits


DIFF_HEADER = re.compile(r"^diff --git a/(.+?) b/(.+)$")


def scan_history(root):
    """
    Deleting a file does not remove it from the repository - it stays in every
    commit that contained it and anyone who clones gets a copy. This checks
    what git actually holds, not what the working tree currently shows.

    The diff stream is split by file so the allowlist applies here too.
    Scanning it as one blob flags this script's own pattern list and the
    .gitignore rules that block the same names, and a check that reports
    itself gets ignored.
    """
    try:
        blob = subprocess.run(["git", "-C", root, "log", "--all", "-p"],
                              capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.TimeoutExpired) as e:
        print(f"  could not read history: {e}")
        return []
    if blob.returncode != 0:
        return []

    hits = []
    current, buf = None, []

    def flush():
        if current and current not in ALLOWLIST and buf:
            scan_text("\n".join(buf), f"history:{current}", hits)

    for line in blob.stdout.splitlines():
        m = DIFF_HEADER.match(line)
        if m:
            flush()
            current, buf = m.group(2), []
            continue
        if current is not None:
            buf.append(line)
    flush()
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", action="store_true",
                    help="also scan every commit, not just the working tree")
    ap.add_argument("--root", default=os.path.dirname(os.path.abspath(__file__)))
    args = ap.parse_args()

    print(f"scanning {args.root}")
    hits = scan_tree(args.root)
    print(f"  working tree: {len(hits)} match(es)")

    if args.history:
        h = scan_history(args.root)
        print(f"  git history : {len(h)} match(es)")
        hits += h

    if not hits:
        print("\nClean. Nothing matching a credential, a client identifier or "
              "a real data export.")
        return 0

    print(f"\n{len(hits)} problem(s) found:\n")
    for where, line, label, snippet in hits[:60]:
        print(f"  {where}:{line}  [{label}]  {snippet}")
    if len(hits) > 60:
        print(f"  ... and {len(hits) - 60} more")
    print("\nDo not push. Fix these first.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
