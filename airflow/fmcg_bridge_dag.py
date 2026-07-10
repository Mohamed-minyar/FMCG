"""
fmcg_bridge_dag.py
==================
Airflow DAG that runs Mohamed Elshabasy's Silver → Parquet → BigQuery bridge
on a schedule. Drop this file into Airflow's `dags/` folder.

WHAT IT DOES (per run):
  1. refresh_silver          — calls Ali's silver_parent.sp_refresh_all_silver_layers
                                to rebuild Silver from Bronze in SQL Server.
  2. extract_silver_to_parquet — pulls all 6 Silver tables + 2 bronze_shared
                                  reference tables, writes Parquet + manifest.
  3. upload_to_bigquery       — uploads Parquet to GCS, loads into BigQuery
                                  `fmcg_silver` dataset (WRITE_TRUNCATE).
  4. verify_row_counts        — DQ gate: BQ row counts must match the manifest.

DEPENDENCIES
  - Airflow Connection `mssql_default`  (SQL Server — Ali's database)
  - Airflow Connection `gcp_default`    (Google Cloud — Amr's project)
  - The bridge scripts at /opt/airflow/bridge/
  - Ali's depi_capstone_project.sql must already be deployed.

SCHEDULE
  @daily — runs after Ali's Bronze ingestion (whatever schedule that's on).

Author: Mohamed Elshabasy (Python & Automation Engineer)
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

BRIDGE_DIR = "/opt/airflow/bridge"
PY         = "/usr/local/bin/python"

DEFAULT_ARGS = {
    "owner": "elshabasy",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="fmcg_silver_to_bigquery",
    default_args=DEFAULT_ARGS,
    description="Elshabasy bridge: SQL Server Silver → Parquet → BigQuery",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fmcg", "bridge", "elshabasy", "silver", "bigquery"],
) as dag:

    # ── Task 1: Refresh Ali's Silver layer in SQL Server ───
    refresh_silver = MsSqlOperator(
        task_id="refresh_silver",
        mssql_conn_id="mssql_default",
        sql="EXEC silver_parent.sp_refresh_all_silver_layers @p_debug = 1, @p_log_execution = 1;",
        doc_md="""Calls Ali's master Silver refresh proc. This TRUNCATEs and
        re-INSERTs all 6 Silver tables from Bronze.""",
    )

    # ── Task 2: Extract Silver → Parquet ───────────────────
    extract = BashOperator(
        task_id="extract_silver_to_parquet",
        bash_command=f"cd {BRIDGE_DIR} && {PY} extract_silver_to_parquet.py",
        doc_md="""Streams all 6 Silver tables + 2 bronze_shared reference
        tables from SQL Server to Parquet files. Writes _manifest.json.""",
    )

    # ── Task 3: Upload Parquet → GCS → BigQuery ────────────
    upload = BashOperator(
        task_id="upload_to_bigquery",
        bash_command=f"cd {BRIDGE_DIR} && {PY} upload_to_bigquery.py --overwrite",
        doc_md="""Uploads each Parquet file to GCS, then loads it into BigQuery
        (WRITE_TRUNCATE). Tables live in the fmcg_silver dataset.""",
    )

    # ── Task 4: DQ gate — verify row counts match manifest ─
    def _verify_row_counts(**ctx):
        """Compare BigQuery row counts against the manifest produced in step 2."""
        import json
        from google.cloud import bigquery
        from config import (PARQUET_DIR, GCP_PROJECT_ID, BIGQUERY_DATASET,
                            BIGQUERY_TABLE_MAP)

        manifest_path = Path(PARQUET_DIR) / "_manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)

        client = bigquery.Client(project=GCP_PROJECT_ID)
        failures = []
        for entry in manifest["tables"]:
            name = entry["output"].replace(".parquet", "")
            bq_table = BIGQUERY_TABLE_MAP.get(name, name)
            query = f"SELECT COUNT(*) AS n FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{bq_table}`"
            actual = client.query(query).result().to_dataframe()["n"].iloc[0]
            expected = entry["rows"]
            status = "✓" if actual == expected else "✗"
            print(f"  {status} {bq_table:<35} expected={expected:>10,}  actual={actual:>10,}")
            if actual != expected:
                failures.append(bq_table)

        if failures:
            raise RuntimeError(f"Row count mismatch in: {failures}")
        print(f"\n✓ All {len(manifest['tables'])} tables verified.")

    verify = PythonOperator(
        task_id="verify_row_counts",
        python_callable=_verify_row_counts,
    )

    # ── Pipeline order ──────────────────────────────────────
    refresh_silver >> extract >> upload >> verify
