"""
config.py
---------
Single source of truth for the Silver → Parquet → BigQuery bridge.
Aligned to Ali Atef's actual Silver schema (depi_capstone_project.sql).

Author: Mohamed Elshabasy (Python & Automation Engineer)
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────
# 1. SQL Server connection (Ali's on-prem Silver source)
# ────────────────────────────────────────────────────────────
MSSQL_HOST     = os.getenv("MSSQL_HOST",     "localhost")
MSSQL_PORT     = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "master")
MSSQL_USER     = os.getenv("MSSQL_USER",     "sa")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "Your_password123")
MSSQL_DRIVER   = os.getenv("MSSQL_DRIVER",   "ODBC Driver 17 for SQL Server")

# SQLAlchemy URI (used by pandas.read_sql for streaming)
MSSQL_CONN_URI = (
    f"mssql+pyodbc://{MSSQL_USER}:{MSSQL_PASSWORD}"
    f"@{MSSQL_HOST}:{MSSQL_PORT}/{MSSQL_DATABASE}"
    f"?driver={MSSQL_DRIVER.replace(' ', '+')}"
)

# pyodbc connection string (used for raw cursor / pre-refresh calls)
MSSQL_ODBC_STR = (
    f"DRIVER={{{MSSQL_DRIVER}}};"
    f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USER};PWD={MSSQL_PASSWORD};"
    f"TrustServerCertificate=yes;"
)

# ────────────────────────────────────────────────────────────
# 2. Ali's Silver tables to extract.
#    Mirrors depi_capstone_project.sql Section 3.
#    NOTE: Ali's Silver layer has 6 tables only — no silver_shared,
#    no dim_channels, no dim_currency_rates SCD2, no dim_date.
#    See README "What's missing" section for what Amr will need.
# ────────────────────────────────────────────────────────────
SILVER_TABLES = [
    # (schema,           table,            output_filename)
    ("silver_parent",   "dim_customers",  "dim_customers_parent"),
    ("silver_parent",   "dim_products",   "dim_products_parent"),
    ("silver_parent",   "fact_sales",     "fact_sales_parent"),
    ("silver_acquired", "dim_customers",  "dim_customers_acquired"),
    ("silver_acquired", "dim_products",   "dim_products_acquired"),
    ("silver_acquired", "fact_sales",     "fact_sales_acquired"),
]

# Bonus reference tables (NOT in Ali's Silver, but Amr needs them for Gold).
# Extracted as-is from bronze_shared. Disable with EXTRACT_REFERENCE_TABLES=False.
EXTRACT_REFERENCE_TABLES = os.getenv("EXTRACT_REFERENCE_TABLES", "true").lower() == "true"
REFERENCE_TABLES = [
    ("bronze_shared",   "channels",        "dim_channels_ref"),
    ("bronze_shared",   "currency_rates",  "currency_rates_ref"),
]

# ────────────────────────────────────────────────────────────
# 3. Ali's master Silver refresh proc — call this before extract
#    to make sure Silver is fresh.
# ────────────────────────────────────────────────────────────
SILVER_REFRESH_PROC = "silver_parent.sp_refresh_all_silver_layers"

# ────────────────────────────────────────────────────────────
# 4. Local filesystem paths
# ────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PARQUET_DIR  = os.getenv("PARQUET_DIR", os.path.join(PROJECT_ROOT, "data", "parquet"))
LOG_DIR      = os.getenv("LOG_DIR",     os.path.join(PROJECT_ROOT, "logs"))

# ────────────────────────────────────────────────────────────
# 5. Extraction tuning
# ────────────────────────────────────────────────────────────
BATCH_SIZE        = 50_000      # rows per fetch batch
COMPRESSION       = "snappy"    # snappy | gzip | zstd

# ────────────────────────────────────────────────────────────
# 6. BigQuery target (Amr's handoff)
# ────────────────────────────────────────────────────────────
GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID",    "")
GCS_BUCKET        = os.getenv("GCS_BUCKET",        "")
BIGQUERY_DATASET  = os.getenv("BIGQUERY_DATASET",  "fmcg_silver")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", ""
)

# Mapping: parquet filename → BigQuery table name
BIGQUERY_TABLE_MAP = {
    "dim_customers_parent":    "dim_customers_parent",
    "dim_products_parent":     "dim_products_parent",
    "fact_sales_parent":       "fact_sales_parent",
    "dim_customers_acquired":  "dim_customers_acquired",
    "dim_products_acquired":   "dim_products_acquired",
    "fact_sales_acquired":     "fact_sales_acquired",
    "dim_channels_ref":        "dim_channels_ref",
    "currency_rates_ref":      "currency_rates_ref",
}

# ────────────────────────────────────────────────────────────
# 7. Schema enforcement — EXACTLY matches Ali's depi_capstone_project.sql
#    Guarantees Amr gets stable types in BigQuery.
# ────────────────────────────────────────────────────────────
import pyarrow as pa

PYARROW_SCHEMA = {
    # ── silver_parent.dim_customers / silver_acquired.dim_customers ──
    # Ali's schema: NO signup_date (it became valid_from).
    # gender is VARCHAR(10), so 'Male'/'Female'/'M'/'F'/'U' all possible.
    "dim_customers_parent": pa.schema([
        pa.field("customer_key",  pa.int32()),
        pa.field("customer_id",   pa.int32()),
        pa.field("name",          pa.string()),
        pa.field("gender",        pa.string()),     # VARCHAR(10)
        pa.field("age",           pa.int32()),
        pa.field("country",       pa.string()),
        pa.field("city",          pa.string()),
        pa.field("loyalty_tier",  pa.string()),
        pa.field("valid_from",    pa.date32()),
        pa.field("valid_to",      pa.date32()),
        pa.field("is_current",    pa.bool_()),
    ]),
    "dim_customers_acquired": pa.schema([
        pa.field("customer_key",  pa.int32()),
        pa.field("customer_id",   pa.int32()),
        pa.field("name",          pa.string()),
        pa.field("gender",        pa.string()),
        pa.field("age",           pa.int32()),
        pa.field("country",       pa.string()),
        pa.field("city",          pa.string()),
        pa.field("loyalty_tier",  pa.string()),
        pa.field("valid_from",    pa.date32()),
        pa.field("valid_to",      pa.date32()),
        pa.field("is_current",    pa.bool_()),
    ]),

    # ── dim_products (parent + acquired) ──
    "dim_products_parent": pa.schema([
        pa.field("product_key",  pa.int32()),
        pa.field("product_id",   pa.int32()),
        pa.field("product_name", pa.string()),
        pa.field("brand",        pa.string()),
        pa.field("category",     pa.string()),
        pa.field("subcategory",  pa.string()),
    ]),
    "dim_products_acquired": pa.schema([
        pa.field("product_key",  pa.int32()),
        pa.field("product_id",   pa.int32()),
        pa.field("product_name", pa.string()),
        pa.field("brand",        pa.string()),
        pa.field("category",     pa.string()),
        pa.field("subcategory",  pa.string()),
    ]),

    # ── fact_sales (parent + acquired) — sales_key is BIGINT per Ali ──
    "fact_sales_parent": pa.schema([
        pa.field("sales_key",           pa.int64()),     # BIGINT
        pa.field("transaction_id",      pa.int64()),
        pa.field("transaction_date",    pa.date32()),
        pa.field("customer_key",        pa.int32()),
        pa.field("product_key",         pa.int32()),
        pa.field("channel_id",          pa.int32()),
        pa.field("quantity",            pa.int32()),
        pa.field("unit_price_usd",      pa.decimal128(10, 2)),
        pa.field("discount_amount_usd", pa.decimal128(10, 2)),
        pa.field("cost_usd",            pa.decimal128(10, 2)),
        pa.field("revenue_usd",         pa.decimal128(10, 2)),
        pa.field("profit_usd",          pa.decimal128(10, 2)),
    ]),
    "fact_sales_acquired": pa.schema([
        pa.field("sales_key",           pa.int64()),
        pa.field("transaction_id",      pa.int64()),
        pa.field("transaction_date",    pa.date32()),
        pa.field("customer_key",        pa.int32()),
        pa.field("product_key",         pa.int32()),
        pa.field("channel_id",          pa.int32()),
        pa.field("quantity",            pa.int32()),
        pa.field("unit_price_usd",      pa.decimal128(10, 2)),
        pa.field("discount_amount_usd", pa.decimal128(10, 2)),
        pa.field("cost_usd",            pa.decimal128(10, 2)),
        pa.field("revenue_usd",         pa.decimal128(10, 2)),
        pa.field("profit_usd",          pa.decimal128(10, 2)),
    ]),

    # ── Reference tables from bronze_shared (NOT in Ali's Silver) ──
    "dim_channels_ref": pa.schema([
        pa.field("channel_id",    pa.int32()),
        pa.field("channel_type",  pa.string()),
        pa.field("region",        pa.string()),
        pa.field("store_size",    pa.string()),
    ]),
    "currency_rates_ref": pa.schema([
        pa.field("rate_date",            pa.date32()),
        pa.field("currency",             pa.string()),
        pa.field("exchange_rate_to_usd", pa.decimal128(10, 4)),
    ]),
}


def get_logger(name: str = "bridge"):
    """Shared logger that writes to console + logs/bridge.log."""
    import logging
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "bridge.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger
