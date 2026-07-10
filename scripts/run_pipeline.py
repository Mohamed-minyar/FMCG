"""
run_bridge.py
=============
Single command to run the entire bridge pipeline:
  1. (Optional) Refresh Ali's Silver layer in SQL Server
  2. Extract Silver tables from SQL Server → Parquet
  3. Upload Parquet → GCS → BigQuery

This is what Airflow (or cron, or your CI/CD) calls.

USAGE
-----
    python run_bridge.py                            # full pipeline
    python run_bridge.py --refresh-silver           # refresh Ali's Silver first
    python run_bridge.py --extract-only             # skip BigQuery upload
    python run_bridge.py --bq-only                  # skip extraction (use existing parquet)
    python run_bridge.py --overwrite                # DROP+recreate BigQuery tables
    python run_bridge.py --dry-run                  # connect + count, write nothing
    python run_bridge.py --table fact_sales_parent  # one table only
    python run_bridge.py --no-reference             # skip bronze_shared reference tables

EXIT CODES
----------
    0  success
    1  extraction failed
    2  upload failed
    3  config / env missing

Author: Mohamed Elshabasy (Python & Automation Engineer)
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from config import get_logger

log = get_logger("bridge")


def parse_args():
    p = argparse.ArgumentParser(description="FMCG Silver → Parquet → BigQuery bridge runner")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--extract-only", action="store_true",
                      help="Run only the extraction step (skip BQ upload)")
    mode.add_argument("--bq-only", action="store_true",
                      help="Run only the BigQuery upload (use existing Parquet)")
    p.add_argument("--refresh-silver", action="store_true",
                   help="Call Ali's sp_refresh_all_silver_layers before extracting")
    p.add_argument("--no-reference", action="store_true",
                   help="Skip bronze_shared reference tables")
    p.add_argument("--overwrite", action="store_true",
                   help="Overwrite BigQuery tables (WRITE_TRUNCATE)")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't write any files, just verify connectivity + counts")
    p.add_argument("--table", help="Process a single table only")
    return p.parse_args()


def run_extract(refresh: bool, table_filter: str | None, no_reference: bool,
                dry_run: bool) -> bool:
    log.info("▶ Step 1: Extract Silver → Parquet")
    import subprocess
    cmd = [sys.executable, "extract_silver_to_parquet.py"]
    if refresh:
        cmd += ["--refresh-silver"]
    if no_reference:
        cmd += ["--no-reference"]
    if table_filter:
        cmd += ["--table", table_filter]
    if dry_run:
        cmd += ["--dry-run"]
    started = time.time()
    rc = subprocess.call(cmd)
    duration = round(time.time() - started, 2)
    if rc != 0:
        log.error(f"Extraction failed (exit {rc}) after {duration}s")
        return False
    log.info(f"✓ Extraction completed in {duration}s")
    return True


def run_upload(table_filter: str | None, overwrite: bool) -> bool:
    log.info("▶ Step 2: Upload Parquet → GCS → BigQuery")
    import subprocess
    cmd = [sys.executable, "upload_to_bigquery.py"]
    if table_filter:
        cmd += ["--table", table_filter]
    if overwrite:
        cmd += ["--overwrite"]
    started = time.time()
    rc = subprocess.call(cmd)
    duration = round(time.time() - started, 2)
    if rc != 0:
        log.error(f"Upload failed (exit {rc}) after {duration}s")
        return False
    log.info(f"✓ Upload completed in {duration}s")
    return True


def main():
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    log.info("=" * 70)
    log.info("FMCG BRIDGE PIPELINE")
    log.info(f"  mode           : {'dry-run' if args.dry_run else 'normal'}")
    log.info(f"  refresh_silver : {args.refresh_silver}")
    log.info(f"  extract        : {not args.bq_only}")
    log.info(f"  upload         : {not args.extract_only and not args.dry_run}")
    log.info(f"  include_ref    : {not args.no_reference}")
    log.info(f"  overwrite_bq   : {args.overwrite}")
    log.info(f"  table          : {args.table or 'ALL'}")
    log.info("=" * 70)

    ok = True

    if not args.bq_only:
        ok = run_extract(args.refresh_silver, args.table, args.no_reference, args.dry_run)
        if not ok:
            sys.exit(1)

    if not args.extract_only and not args.dry_run:
        ok = run_upload(args.table, args.overwrite)
        if not ok:
            sys.exit(2)

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    log.info("=" * 70)
    log.info(f"BRIDGE PIPELINE {'✓ DONE' if ok else '✗ FAILED'}  (total {elapsed:.1f}s)")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
