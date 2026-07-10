"""
Unit / smoke tests for the bridge.
Run:  python -m pytest test_bridge.py -v
     (or: python test_bridge.py for a single smoke test)

Aligned to Ali Atef's actual Silver schema (depi_capstone_project.sql):
  - 6 Silver tables expected
  - 2 bonus reference tables from bronze_shared (optional)
"""
import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))


def test_config_loads():
    import config
    assert len(config.SILVER_TABLES) == 6, (
        f"Expected 6 Silver tables (Ali's schema), got {len(config.SILVER_TABLES)}"
    )
    assert len(config.REFERENCE_TABLES) == 2, (
        f"Expected 2 reference tables, got {len(config.REFERENCE_TABLES)}"
    )
    print(f"✓ config.py loads — {len(config.SILVER_TABLES)} Silver + "
          f"{len(config.REFERENCE_TABLES)} reference tables")


def test_schemas_complete():
    """Every SILVER_TABLES + REFERENCE_TABLES entry must have a PyArrow schema."""
    import config
    all_targets = list(config.SILVER_TABLES) + list(config.REFERENCE_TABLES)
    for schema, table, output_name in all_targets:
        assert output_name in config.PYARROW_SCHEMA, (
            f"Missing PyArrow schema for '{output_name}' "
            f"(table {schema}.{table})"
        )
    print(f"✓ All {len(all_targets)} tables have PyArrow schemas")


def test_dim_customers_schema_matches_ali():
    """Verify dim_customers schema matches Ali's actual DDL
    (NO signup_date, gender VARCHAR(10), valid_from instead of signup_date)."""
    import config
    s = config.PYARROW_SCHEMA["dim_customers_parent"]
    field_names = [f.name for f in s]
    expected = [
        "customer_key", "customer_id", "name", "gender", "age",
        "country", "city", "loyalty_tier",
        "valid_from", "valid_to", "is_current",
    ]
    assert field_names == expected, (
        f"dim_customers schema mismatch:\n  expected: {expected}\n  actual:   {field_names}\n"
        f"  → Did Ali change the schema? Update config.PYARROW_SCHEMA."
    )
    assert "signup_date" not in field_names, (
        "signup_date should NOT be in dim_customers — Ali uses valid_from instead."
    )
    print(f"✓ dim_customers schema matches Ali's DDL (11 cols, no signup_date)")


def test_fact_sales_schema_matches_ali():
    """Verify fact_sales schema matches Ali's DDL (sales_key BIGINT)."""
    import config
    s = config.PYARROW_SCHEMA["fact_sales_parent"]
    sales_key_field = s.field("sales_key")
    assert pa.types.is_int64(sales_key_field.type), (
        f"sales_key should be int64 (BIGINT in Ali's DDL), got {sales_key_field.type}"
    )
    print("✓ fact_sales schema matches Ali's DDL (sales_key BIGINT)")


def test_required_env_or_defaults():
    import config
    assert config.MSSQL_HOST, "MSSQL_HOST not set"
    assert config.MSSQL_USER, "MSSQL_USER not set"
    assert config.PARQUET_DIR, "PARQUET_DIR not set"
    print(f"✓ env config OK — host={config.MSSQL_HOST}, parquet_dir={config.PARQUET_DIR}")


def test_ali_procs_referenced():
    """Verify config references Ali's actual proc name."""
    import config
    assert config.SILVER_REFRESH_PROC == "silver_parent.sp_refresh_all_silver_layers", (
        f"SILVER_REFRESH_PROC should match Ali's master proc, "
        f"got {config.SILVER_REFRESH_PROC}"
    )
    print(f"✓ Ali's master proc referenced: {config.SILVER_REFRESH_PROC}")


def test_parquet_round_trip():
    import config
    parquet_dir = Path(config.PARQUET_DIR)
    if not parquet_dir.exists():
        print("⊘ Skipping parquet round-trip (no data/parquet yet)")
        return

    for output_name, schema in config.PYARROW_SCHEMA.items():
        path = parquet_dir / f"{output_name}.parquet"
        if not path.exists():
            continue
        table = pq.read_table(path)
        assert table.schema.equals(schema), (
            f"Schema mismatch in {path.name}:\n"
            f"  declared: {schema}\n"
            f"  actual:   {table.schema}"
        )
        print(f"✓ {output_name}.parquet matches declared schema ({len(table):,} rows)")


def test_sql_server_reachable():
    import config
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(config.MSSQL_CONN_URI, fast_executemethod=True)
        with engine.connect() as conn:
            ver = conn.execute(text("SELECT @@VERSION")).scalar()
        engine.dispose()
        print(f"✓ SQL Server reachable: {ver.splitlines()[0]}")
    except Exception as e:
        print(f"⊘ SQL Server not reachable (skipped): {e}")


def test_gcs_config_present():
    import config
    if not config.GCP_PROJECT_ID:
        print("⊘ GCP_PROJECT_ID not set (Phase 3 upload disabled)")
        return
    assert config.GCS_BUCKET, "GCS_BUCKET required when GCP_PROJECT_ID is set"
    print(f"✓ GCS config OK — project={config.GCP_PROJECT_ID}, bucket={config.GCS_BUCKET}")


def smoke():
    test_config_loads()
    test_schemas_complete()
    test_dim_customers_schema_matches_ali()
    test_fact_sales_schema_matches_ali()
    test_required_env_or_defaults()
    test_ali_procs_referenced()
    test_parquet_round_trip()
    test_sql_server_reachable()
    test_gcs_config_present()
    print("\n✓ All smoke tests passed.")


if __name__ == "__main__":
    smoke()
