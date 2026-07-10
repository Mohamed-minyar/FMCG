"""
upload_to_bigquery.py
=====================
Uploads the Parquet files produced by extract_silver_to_parquet.py
to GCS, then loads them into BigQuery as the Silver layer.

This is the handoff to Amr (@Amr Elyamani) — once these tables
exist in BigQuery under `fmcg_silver` dataset, Amr's dbt project
can reference them as {{ source('fmcg_silver', 'fact_sales_parent') }}
and build the Gold layer on top.

USAGE
-----
    python upload_to_bigquery.py                         # upload all
    python upload_to_bigquery.py --table fact_sales_parent
    python upload_to_bigquery.py --gcs-only              # skip BigQuery load
    python upload_to_bigquery.py --overwrite             # WRITE_TRUNCATE

REQUIREMENTS
------------
    pip install google-cloud-storage google-cloud-bigquery

Author: Mohamed Elshabasy (Python & Automation Engineer)
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from config import (
    PARQUET_DIR,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    BIGQUERY_DATASET,
    BIGQUERY_TABLE_MAP,
    get_logger,
)

log = get_logger("bq_upload")


# ────────────────────────────────────────────────────────────
# 1. GCS upload
# ────────────────────────────────────────────────────────────
def upload_to_gcs(local_path: str, gcs_path: str) -> str:
    from google.cloud import storage
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path, content_type="application/octet-stream")
    uri = f"gs://{GCS_BUCKET}/{gcs_path}"
    log.info(f"  ✓ uploaded {Path(local_path).name} → {uri}  "
             f"({os.path.getsize(local_path) / 1024 / 1024:.2f} MB)")
    return uri


# ────────────────────────────────────────────────────────────
# 2. BigQuery load (from GCS Parquet)
# ────────────────────────────────────────────────────────────
def load_parquet_to_bq(gcs_uri: str, table_name: str, overwrite: bool = False) -> dict:
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE if overwrite
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
        autodetect=False,
        use_avro_logical_types=True,
    )

    log.info(f"  loading {gcs_uri} → {table_id}  "
             f"(mode={'overwrite' if overwrite else 'append'})")
    started = time.time()
    job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()

    dest = client.get_table(table_id)
    duration = round(time.time() - started, 2)
    log.info(f"  ✓ {table_id}  ({dest.num_rows:,} rows, "
             f"{len(dest.schema)} cols, {duration}s)")
    return {
        "table":         table_id,
        "rows":          dest.num_rows,
        "columns":       len(dest.schema),
        "duration_sec":  duration,
        "mode":          "overwrite" if overwrite else "append",
    }


# ────────────────────────────────────────────────────────────
# 3. Ensure the BigQuery dataset exists
# ────────────────────────────────────────────────────────────
def ensure_dataset():
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"
    try:
        client.get_dataset(dataset_id)
        log.info(f"BigQuery dataset already exists: {dataset_id}")
    except Exception:
        log.info(f"Creating BigQuery dataset: {dataset_id}")
        ds = bigquery.Dataset(dataset_id)
        ds.location = "EU"
        ds.description = "FMCG Silver layer — populated by Elshabasy bridge (Ali Atef source)"
        client.create_dataset(ds, exists_ok=True)


# ────────────────────────────────────────────────────────────
# 4. CLI orchestrator
# ────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Upload Silver Parquet files to BigQuery via GCS.")
    p.add_argument("--table", help="Upload only this table (output name from manifest)")
    p.add_argument("--gcs-only", action="store_true",
                   help="Upload to GCS only; skip BigQuery load")
    p.add_argument("--overwrite", action="store_true",
                   help="DROP+recreate BigQuery tables (WRITE_TRUNCATE)")
    return p.parse_args()


def main():
    args = parse_args()
    log.info("=" * 60)
    log.info("FMCG Parquet → GCS → BigQuery uploader")
    log.info("=" * 60)

    if not GCP_PROJECT_ID or not GCS_BUCKET:
        log.error("GCP_PROJECT_ID and GCS_BUCKET must be set in .env")
        return

    manifest_path = os.path.join(PARQUET_DIR, "_manifest.json")
    if not os.path.exists(manifest_path):
        log.error(f"Manifest not found: {manifest_path}")
        log.error("Run extract_silver_to_parquet.py first.")
        return
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    if not args.gcs_only:
        ensure_dataset()

    gcs_prefix = f"silver/{manifest['generated_at'][:10]}"

    results = []
    for entry in manifest["tables"]:
        name = entry["output"].replace(".parquet", "")
        if args.table and args.table != name:
            continue
        if not os.path.exists(entry["path"]):
            log.warning(f"  SKIP {name}: file missing ({entry['path']})")
            continue

        log.info(f"━━━ {name} ━━━")
        gcs_path = f"{gcs_prefix}/{name}.parquet"
        gcs_uri = upload_to_gcs(entry["path"], gcs_path)

        if not args.gcs_only:
            bq_table = BIGQUERY_TABLE_MAP.get(name, name)
            result = load_parquet_to_bq(gcs_uri, bq_table, overwrite=args.overwrite)
            result["gcs_uri"] = gcs_uri
            result["source_rows"] = entry["rows"]
            results.append(result)

    log.info("=" * 60)
    log.info("Upload Summary")
    log.info("=" * 60)
    for r in results:
        match = "✓" if r["rows"] == r["source_rows"] else "⚠"
        log.info(f"  {match} {r['table']:<55} rows={r['rows']:>10,}  "
                 f"({r['duration_sec']}s)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
