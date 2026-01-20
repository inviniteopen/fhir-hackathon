"""Load FHIR Bundle JSON files into DuckDB tables (one table per resource type)."""

import json
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from src.constants import BRONZE_SCHEMA


def _quote_ident(ident: str) -> str:
    return f'"{ident.replace('"', '""')}"'


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _ensure_bronze_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(BRONZE_SCHEMA)}")


def _collect_resources_by_type(
    bundle_paths: list[Path],
) -> dict[str, list[dict[str, Any]]]:
    """Collect resources by type across one or more bundle files."""
    resources_by_type: dict[str, list[dict[str, Any]]] = {}

    for json_path in bundle_paths:
        bundle_json = json.loads(json_path.read_text())
        bundle_id = bundle_json.get("id")

        for entry in bundle_json.get("entry", []):
            resource = entry.get("resource", {})
            resource_type = resource.get("resourceType")
            if resource_type:
                resources_by_type.setdefault(resource_type, []).append(
                    {
                        "_source_file": json_path.name,
                        "_source_bundle": bundle_id,
                        "_full_url": entry.get("fullUrl"),
                        **resource,
                    }
                )

    return resources_by_type


def load_bundles_to_tables(
    bundle_dir: Path,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Load all FHIR Bundle JSON files from a directory into DuckDB tables.

    Creates one table per resource type (e.g., patient, observation, condition).
    Each resource includes metadata fields:
    - _source_file: name of the source JSON file
    - _source_bundle: bundle ID
    - _full_url: fullUrl from the bundle entry

    Args:
        bundle_dir: Directory containing FHIR Bundle JSON files
        con: Optional existing DuckDB connection. Creates in-memory if not provided.

    Returns:
        DuckDB connection with resource tables created
    """
    if con is None:
        con = duckdb.connect(":memory:")
    _ensure_bronze_schema(con)

    json_files = sorted(bundle_dir.glob("*.json"))
    if not json_files:
        return con

    resources_by_type = _collect_resources_by_type(json_files)

    # Create tables (PyArrow handles schema union for varying fields)
    for resource_type, resources in resources_by_type.items():
        table_name = resource_type.lower()
        _create_table(con, table_name, resources)

    return con


def load_bundle_file(
    bundle_path: Path,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Load a single FHIR Bundle JSON file into DuckDB tables.

    Args:
        bundle_path: Path to a FHIR Bundle JSON file
        con: Optional existing DuckDB connection. Creates in-memory if not provided.

    Returns:
        DuckDB connection with resource tables created/updated
    """
    if con is None:
        con = duckdb.connect(":memory:")
    _ensure_bronze_schema(con)

    resources_by_type = _collect_resources_by_type([bundle_path])

    for resource_type, resources in resources_by_type.items():
        table_name = resource_type.lower()
        _create_table(con, table_name, resources)

    return con


def _create_table(
    con: duckdb.DuckDBPyConnection, table_name: str, resources: list[dict[str, Any]]
) -> None:
    """Create a table from a list of resource dicts using Polars."""
    # Use Polars for better schema inference with heterogeneous dicts
    df = pl.DataFrame(resources)
    temp_name = f"_temp_{table_name}"
    con.register(temp_name, df.to_arrow())
    _ensure_bronze_schema(con)
    bronze_table = _qualified_table(BRONZE_SCHEMA, table_name)
    con.execute(f"CREATE OR REPLACE TABLE {bronze_table} AS SELECT * FROM {temp_name}")
    legacy_table = _qualified_table("main", table_name)
    con.execute(f"DROP TABLE IF EXISTS {legacy_table}")
    con.unregister(temp_name)


def get_table_summary(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Get row counts for all tables in the connection."""
    table_names = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [BRONZE_SCHEMA],
    ).fetchall()
    summary: dict[str, int] = {}
    for (table_name,) in table_names:
        count = con.execute(
            f"SELECT COUNT(*) FROM {_qualified_table(BRONZE_SCHEMA, table_name)}"
        ).fetchone()[0]
        summary[f"{BRONZE_SCHEMA}.{table_name}"] = count
    return summary
