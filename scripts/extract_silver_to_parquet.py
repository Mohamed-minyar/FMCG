"""
extract_silver_to_parquet.py
============================
The bridge between Ali's SQL Server Silver layer and Amr's BigQuery Gold layer.

WHAT IT DOES
------------
1. (Optional) Calls Ali's `silver_parent.sp_refresh_all_silver_layers` to
   rebuild Silver from Bronze before extracting.
2. Connects to SQL Server (Silver + bronze_shared reference tables).
3. Streams each table in batches (memory-safe — won't OOM on big facts).
4. Enforces a strict PyArrow schema (Amr gets stable types, no surprises).
5. Writes columnar Parquet (Snappy-compressed) to ./data/parquet/.
6. Emits a per-table manifest (row count, bytes, schema hash, checksum)
   so Amr can verify integrity before loading to BigQuery.

ALIGNED TO ALI ATEF'S ACTUAL SILVER SCHEMA (depi_capstone_project.sql):
  - 6 Silver tables: dim_customers / dim_products / fact_sales (×parent + ×acquired)
  - NO silver_shared schema exists in Ali's file.
  - NO signup_date column in dim_customers (it became valid_from).
  - gender is VARCHAR(10) — may contain 'Male'/'Female'/'M'/'F'/'U'.
  - sales_key is BIGINT (per Ali's DDL).
  - Reference tables (bronze_shared.channels, bronze_shared.currency_rates)
    are extracted as a bonus so Amr can build channel/currency-aware Gold.

USAGE
-----
    # Extract all Silver tables (and reference tables)
    python extract_silver_to_parquet.py

    # Refresh Silver first (calls Ali's master proc), then extract
    python extract_silver_to_parquet.py --refresh-silver

    # Extract a single table
    python extract_silver_to_parquet.py --table fact_sales_parent

    # Skip the bronze_shared reference tables
    python extract_silver_to_parquet.py --no-reference

    # Dry run (connects + shows what WOULD be extracted, writes nothing)
    python extract_silver_to_parquet.py --dry-run

REQUIREMENTS
------------
    pip install pandas pyodbc sqlalchemy python-dotenv pyarrow

Author: Mohamed Elshabasy (Python & Automation Engineer)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from config import (
    MSSQL_CONN_URI,
    MSSQL_ODBC_STR,
    PARQUET_DIR,
    BATCH_SIZE,
    COMPRESSION,
    SILVER_TABLES,
    REFERENCE_TABLES,
    EXTRACT_REFERENCE_TABLES,
    SILVER_REFRESH_PROC,
    PYARROW_SCHEMA,
    get_logger,
)

log = get_logger("extract")


# ────────────────────────────────────────────────────────────
# 1. Connection helpers
# ────────────────────────────────────────────────────────────
def get_engine():
    """Create a SQLAlchemy engine for SQL Server."""
    log.info("Connecting to SQL Server via SQLAlchemy …")
    engine = create_engine(
        MSSQL_CONN_URI,
        fast_executemethod=True,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    with engine.connect() as conn:
        ver = conn.execute(text("SELECT @@VERSION")).scalar()
        log.info(f"Connected. Server: {ver.splitlines()[0]}")
    return engine


# ────────────────────────────────────────────────────────────
# 2. Optional: trigger Ali's Silver refresh before extracting
# ────────────────────────────────────────────────────────────
def refresh_silver_layer():
    """Call Ali's silver_parent.sp_refresh_all_silver_layers stored proc.
    This rebuilds all 6 Silver tables from Bronze (TRUNCATE + INSERT).
    Uses pyodbc directly because SQLAlchemy doesn't play nicely with
    multi-statement T-SQL procs that PRINT progress messages."""
    import pyodbc
    log.info(f"Calling Ali's master Silver refresh proc: {SILVER_REFRESH_PROC}")
    started = time.time()
    with pyodbc.connect(MSSQL_ODBC_STR, autocommit=True) as cn:
        cn.add_output_callback(lambda cmd, msg: log.info(f"  [SQL PRINT] {msg.strip()}"))
        with cn.cursor() as cur:
            cur.execute(f"EXEC {SILVER_REFRESH_PROC} @p_debug = 1, @p_log_execution = 1;")
            while cur.nextset():
                pass
    log.info(f"Silver refresh completed in {time.time() - started:.1f}s")


# ────────────────────────────────────────────────────────────
# 3. Schema discovery (verification, NOT enforcement source)
# ────────────────────────────────────────────────────────────
def fetch_source_schema(engine, schema: str, table: str) -> dict:
    """Return column name → SQL type from INFORMATION_SCHEMA."""
    sql = text("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t
        ORDER BY ORDINAL_POSITION
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"s": schema, "t": table}).fetchall()
    return {r[0]: {"type": r[1], "nullable": r[2]} for r in rows}


# ────────────────────────────────────────────────────────────
# 4. Streaming fetch — the memory-safe core
# ────────────────────────────────────────────────────────────
def stream_table_batches(engine, schema: str, table: str, batch_size: int = BATCH_SIZE
                         ) -> Iterator[pd.DataFrame]:
    """Yield the table in chunks using SQLAlchemy's streaming cursor."""
    sql = f"SELECT * FROM [{schema}].[{table}]"
    log.info(f"Streaming: {sql}  (batch_size={batch_size})")

    conn = engine.connect().execution_options(
        stream_results=True, max_row_buffer=batch_size
    )
    try:
        for chunk in pd.read_sql(sql, conn, chunksize=batch_size):
            yield chunk
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────
# 5. Type-coerce a pandas DataFrame to the declared PyArrow schema
# ────────────────────────────────────────────────────────────
def coerce_to_schema(df: pd.DataFrame, arrow_schema: pa.Schema) -> pa.Table:
    """Convert a pandas DataFrame into a PyArrow Table that EXACTLY
    matches the declared schema. This is what guarantees Amr gets
    stable types in BigQuery."""
    for field in arrow_schema:
        if pa.types.is_date32(field.type) and field.name in df.columns:
            df[field.name] = pd.to_datetime(df[field.name], errors="coerce").dt.date
        if pa.types.is_decimal(field.type) and field.name in df.columns:
            df[field.name] = df[field.name].apply(
                lambda x: None if pd.isna(x) else x
            )
    table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
    return table


# ────────────────────────────────────────────────────────────
# 6. Write Parquet (streaming — appends row groups per batch)
# ────────────────────────────────────────────────────────────
def write_parquet_streaming(
    batches: Iterator[pd.DataFrame],
    output_path: str,
    arrow_schema: pa.Schema,
    compression: str = COMPRESSION,
) -> dict:
    writer = None
    total_rows = 0
    row_groups = 0
    sha = hashlib.sha256()

    try:
        for batch_df in batches:
            table = coerce_to_schema(batch_df, arrow_schema)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as ipc_writer:
                ipc_writer.write_table(table)
            sha.update(sink.getvalue().to_pybytes())

            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    arrow_schema,
                    compression=compression,
                    use_dictionary=True,
                    write_statistics=True,
                    data_page_size=1024 * 1024,
                )
            writer.write_table(table)
            total_rows += len(batch_df)
            row_groups += 1
            log.info(f"  wrote batch: +{len(batch_df):,} rows "
                     f"(cumulative {total_rows:,}, {row_groups} row groups)")
    finally:
        if writer is not None:
            writer.close()

    return {
        "rows": total_rows,
        "row_groups": row_groups,
        "sha256": sha.hexdigest(),
    }


# ────────────────────────────────────────────────────────────
# 7. Per-table extraction orchestrator
# ────────────────────────────────────────────────────────────
def extract_table(engine, schema: str, table: str, output_name: str,
                  dry_run: bool = False) -> dict:
    log.info(f"━━━ {schema}.{table} → {output_name}.parquet ━━━")
    started = time.time()

    src_schema = fetch_source_schema(engine, schema, table)
    if not src_schema:
        raise RuntimeError(f"Source table {schema}.{table} not found.")
    log.info(f"  source columns: {len(src_schema)}")

    arrow_schema = PYARROW_SCHEMA.get(output_name)
    if arrow_schema is None:
        raise RuntimeError(
            f"No PyArrow schema declared for '{output_name}'. "
            f"Add it to config.PYARROW_SCHEMA."
        )
    missing = set(src_schema) - {f.name for f in arrow_schema}
    if missing:
        raise RuntimeError(
            f"Schema mismatch: source columns not in arrow schema: {missing}"
        )

    with engine.connect() as conn:
        total_count = conn.execute(
            text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        ).scalar()
    log.info(f"  source row count: {total_count:,}")

    if dry_run:
        log.info(f"  [DRY RUN] would write to {output_name}.parquet")
        return {
            "table": f"{schema}.{table}",
            "output": f"{output_name}.parquet",
            "rows": total_count,
            "dry_run": True,
            "duration_sec": round(time.time() - started, 2),
        }

    os.makedirs(PARQUET_DIR, exist_ok=True)
    output_path = os.path.join(PARQUET_DIR, f"{output_name}.parquet")
    stats = write_parquet_streaming(
        stream_table_batches(engine, schema, table),
        output_path,
        arrow_schema,
    )
    duration = round(time.time() - started, 2)
    size_mb = os.path.getsize(output_path) / (1024 * 1024)

    manifest_entry = {
        "table":         f"{schema}.{table}",
        "output":        f"{output_name}.parquet",
        "path":          output_path,
        "rows":          stats["rows"],
        "row_groups":    stats["row_groups"],
        "size_mb":       round(size_mb, 2),
        "compression":   COMPRESSION,
        "sha256":        stats["sha256"],
        "schema_fields": [f.name for f in arrow_schema],
        "extracted_at":  datetime.now(timezone.utc).isoformat(),
        "duration_sec":  duration,
    }
    log.info(f"  ✓ wrote {output_path}  "
             f"({size_mb:.2f} MB, {stats['rows']:,} rows, {duration}s)")
    return manifest_entry


# ────────────────────────────────────────────────────────────
# 8. Manifest writer
# ────────────────────────────────────────────────────────────
def write_manifest(entries: list, path: str):
    manifest = {
        "pipeline":      "fmcg_silver_to_parquet",
        "author":        "Mohamed Elshabasy",
        "source_sql":    "Ali Atef — depi_capstone_project.sql",
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "parquet_dir":   PARQUET_DIR,
        "table_count":   len(entries),
        "total_rows":    sum(e.get("rows", 0) for e in entries),
        "tables":        entries,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    log.info(f"Manifest written → {path}")
    log.info(f"  total tables: {manifest['table_count']}")
    log.info(f"  total rows:   {manifest['total_rows']:,}")


# ────────────────────────────────────────────────────────────
# 9. CLI entry point
# ────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Extract Ali's Silver tables from SQL Server to Parquet for BigQuery load."
    )
    p.add_argument("--table", help="Extract only this output name (e.g. fact_sales_parent)")
    p.add_argument("--refresh-silver", action="store_true",
                   help=f"Call Ali's {SILVER_REFRESH_PROC} before extracting")
    p.add_argument("--no-reference", action="store_true",
                   help="Skip bronze_shared reference tables (channels, currency_rates)")
    p.add_argument("--dry-run", action="store_true",
                   help="Connect + show counts, write nothing")
    return p.parse_args()


def main():
    args = parse_args()
    log.info("=" * 60)
    log.info("FMCG Silver → Parquet Bridge — starting")
    log.info("=" * 60)

    if args.refresh_silver and not args.dry_run:
        try:
            refresh_silver_layer()
        except Exception as e:
            log.error(f"Silver refresh failed: {e}")
            log.error("Continuing anyway — will extract whatever Silver currently has.")
            log.error("(Ali's proc may not be deployed yet. Check with Ali.)")

    engine = get_engine()
    entries = []
    targets = list(SILVER_TABLES)
    if EXTRACT_REFERENCE_TABLES and not args.no_reference:
        targets.extend(REFERENCE_TABLES)
        log.info(f"Including {len(REFERENCE_TABLES)} reference table(s) from bronze_shared.")
    else:
        log.info("Skipping reference tables.")

    try:
        for schema, table, output_name in targets:
            if args.table and args.table != output_name:
                continue
            try:
                entry = extract_table(engine, schema, table, output_name,
                                      dry_run=args.dry_run)
                entries.append(entry)
            except Exception as e:
                log.error(f"FAILED to extract {schema}.{table}: {e}")
                if "not found" in str(e).lower():
                    log.error("  → Ali may not have created this table yet. Skipping.")
                    continue
                raise
    finally:
        engine.dispose()
        log.info("SQL Server engine disposed.")

    if not entries:
        log.error("No tables extracted. Check --table filter or SILVER_TABLES in config.")
        sys.exit(2)

    if not args.dry_run:
        manifest_path = os.path.join(PARQUET_DIR, "_manifest.json")
        write_manifest(entries, manifest_path)

    log.info("=" * 60)
    log.info(f"DONE — {len(entries)} tables processed.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
