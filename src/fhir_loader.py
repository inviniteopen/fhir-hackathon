"""Load FHIR Bundle JSON files into DuckDB tables (one table per resource type)."""

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa


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

    json_files = sorted(bundle_dir.glob("*.json"))
    if not json_files:
        return con

    # Collect all resources by type across all bundles
    resources_by_type: dict[str, list[dict[str, Any]]] = {}

    for json_path in json_files:
        bundle_json = json.loads(json_path.read_text())
        bundle_id = bundle_json.get("id")

        for entry in bundle_json.get("entry", []):
            resource = entry.get("resource", {})
            resource_type = resource.get("resourceType")
            if resource_type:
                if resource_type not in resources_by_type:
                    resources_by_type[resource_type] = []
                resources_by_type[resource_type].append(
                    {
                        "_source_file": json_path.name,
                        "_source_bundle": bundle_id,
                        "_full_url": entry.get("fullUrl"),
                        **resource,
                    }
                )

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

    bundle_json = json.loads(bundle_path.read_text())
    bundle_id = bundle_json.get("id")

    resources_by_type: dict[str, list[dict[str, Any]]] = {}

    for entry in bundle_json.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        if resource_type:
            if resource_type not in resources_by_type:
                resources_by_type[resource_type] = []
            resources_by_type[resource_type].append(
                {
                    "_source_file": bundle_path.name,
                    "_source_bundle": bundle_id,
                    "_full_url": entry.get("fullUrl"),
                    **resource,
                }
            )

    for resource_type, resources in resources_by_type.items():
        table_name = resource_type.lower()
        _create_table(con, table_name, resources)

    return con


def _create_table(
    con: duckdb.DuckDBPyConnection, table_name: str, resources: list[dict[str, Any]]
) -> None:
    """Create a table from a list of resource dicts using PyArrow."""
    arrow_table = pa.Table.from_pylist(resources)
    temp_name = f"_temp_{table_name}"
    con.register(temp_name, arrow_table)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_name}")
    con.unregister(temp_name)


def get_table_summary(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Get row counts for all tables in the connection."""
    tables = con.execute("SHOW TABLES").fetchdf()
    summary = {}
    for table_name in sorted(tables["name"]):
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        summary[table_name] = count
    return summary
