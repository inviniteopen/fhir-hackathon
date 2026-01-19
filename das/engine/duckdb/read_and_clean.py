import duckdb

from .functions.string import (
    lowercase_columns,
    nullify_string_columns,
    trim_string_columns,
)


def _read_from_parquet(dataset_path: str) -> duckdb.DuckDBPyRelation:
    return duckdb.read_parquet(dataset_path)


def _clean(df: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Cleans the given dataframe (lower-case columns, trim, nullify empty strings)."""
    df = lowercase_columns(df)
    df = trim_string_columns(df)
    df = nullify_string_columns(df)
    return df


def read_from_parquet_and_clean(dataset_path: str) -> duckdb.DuckDBPyRelation:
    return _clean(_read_from_parquet(dataset_path))
