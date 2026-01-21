"""DuckDB IO helpers for schema and table operations."""

from pathlib import Path

import duckdb
import polars as pl


def connect_db(path: Path) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database file."""
    return duckdb.connect(str(path))


def ensure_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    """Ensure schema exists."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def write_lazyframe(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    lf: pl.LazyFrame,
) -> None:
    """Write LazyFrame to DuckDB table in given schema."""
    ensure_schema(con, schema)
    df = lf.collect()
    temp_name = f"{schema}_{table}_temp"
    con.register(temp_name, df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM {temp_name}"
    )
    con.unregister(temp_name)
