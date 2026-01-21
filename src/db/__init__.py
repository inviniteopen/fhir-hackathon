"""Database helpers for DuckDB interactions."""

from .duckdb_io import connect_db, count_rows, ensure_schema, write_lazyframe

__all__ = ["connect_db", "count_rows", "ensure_schema", "write_lazyframe"]
