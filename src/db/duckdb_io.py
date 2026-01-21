"""DuckDB IO helpers for schema and table operations."""

from pathlib import Path

import duckdb
import polars as pl

from src.common.sql import qualified_table, quote_ident


def connect_db(path: Path) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database file."""
    return duckdb.connect(str(path))


def ensure_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    """Ensure schema exists."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")


def write_lazyframe(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    lf: pl.LazyFrame,
) -> None:
    """Write LazyFrame to DuckDB table in given schema."""
    df = lf.collect()
    write_dataframe(con, schema, table, df)


def write_dataframe(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    df: pl.DataFrame,
) -> None:
    """Write DataFrame to DuckDB table in given schema."""
    ensure_schema(con, schema)
    temp_name = f"{schema}_{table}_temp"
    con.register(temp_name, df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {qualified_table(schema, table)} "
        f"AS SELECT * FROM {temp_name}"
    )
    con.unregister(temp_name)


def write_dataframes(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    frames: dict[str, pl.DataFrame],
) -> None:
    """Write multiple DataFrames to DuckDB tables in given schema."""
    for table, df in frames.items():
        write_dataframe(con, schema, table, df)


def drop_table_if_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> None:
    """Drop a table if it exists."""
    con.execute(f"DROP TABLE IF EXISTS {qualified_table(schema, table)}")


def get_table_summary(con: duckdb.DuckDBPyConnection, schema: str) -> dict[str, int]:
    """Get row counts for all tables in a schema."""
    table_names = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [schema],
    ).fetchall()
    summary: dict[str, int] = {}
    for (table_name,) in table_names:
        count = con.execute(
            f"SELECT COUNT(*) FROM {qualified_table(schema, table_name)}"
        ).fetchone()[0]
        summary[f"{schema}.{table_name}"] = count
    return summary
