"""Tests for loading FHIR JSON into DuckDB tables (one table per resource type).

Integration layer: read JSON data into DuckDB dataframes, one table per resource type.
"""

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "EPS"


def _create_table_from_resources(
    con: duckdb.DuckDBPyConnection, table_name: str, resources: list[dict[str, Any]]
) -> None:
    """Create a table from a list of resource dicts using PyArrow."""
    arrow_table = pa.Table.from_pylist(resources)
    con.register(f"_temp_{table_name}", arrow_table)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _temp_{table_name}")
    con.unregister(f"_temp_{table_name}")


def _insert_into_table(
    con: duckdb.DuckDBPyConnection, table_name: str, resources: list[dict[str, Any]]
) -> None:
    """Insert resources into an existing table."""
    arrow_table = pa.Table.from_pylist(resources)
    con.register(f"_temp_{table_name}", arrow_table)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM _temp_{table_name}")
    con.unregister(f"_temp_{table_name}")


class TestFhirToTables:
    """Test loading FHIR Bundle JSON into per-resource-type tables."""

    def test_load_bundle_into_resource_tables(self):
        """Load a FHIR bundle and create separate table per resource type."""
        bundle_json = {
            "resourceType": "Bundle",
            "id": "test-bundle-1",
            "type": "document",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {
                        "resourceType": "Patient",
                        "id": "patient-1",
                        "name": [{"family": "Smith", "given": ["John"]}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:patient-2",
                    "resource": {
                        "resourceType": "Patient",
                        "id": "patient-2",
                        "name": [{"family": "Doe", "given": ["Jane"]}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {
                        "resourceType": "Observation",
                        "id": "obs-1",
                        "status": "final",
                        "code": {"text": "Blood pressure"},
                    },
                },
            ],
        }

        # Group resources by type
        resources_by_type: dict[str, list[dict[str, Any]]] = {}
        for entry in bundle_json.get("entry", []):
            resource = entry.get("resource", {})
            resource_type = resource.get("resourceType")
            if resource_type:
                if resource_type not in resources_by_type:
                    resources_by_type[resource_type] = []
                # Add metadata
                resource_with_meta = {
                    "_source_bundle": bundle_json.get("id"),
                    "_full_url": entry.get("fullUrl"),
                    **resource,
                }
                resources_by_type[resource_type].append(resource_with_meta)

        # Create in-memory DuckDB and load each resource type as a table
        con = duckdb.connect(":memory:")

        for resource_type, resources in resources_by_type.items():
            table_name = resource_type.lower()
            _create_table_from_resources(con, table_name, resources)

        # Verify tables created
        tables = con.execute("SHOW TABLES").fetchdf()
        assert set(tables["name"]) == {"patient", "observation"}

        # Query patient table
        patients = con.execute("SELECT id, _full_url FROM patient").fetchdf()
        assert len(patients) == 2
        assert set(patients["id"]) == {"patient-1", "patient-2"}

        # Query observation table
        observations = con.execute("SELECT id, status FROM observation").fetchdf()
        assert len(observations) == 1
        assert observations.iloc[0]["status"] == "final"

    def test_load_multiple_bundles_append_to_tables(self):
        """Load multiple bundles, appending resources to existing tables."""
        bundles = [
            {
                "resourceType": "Bundle",
                "id": "bundle-1",
                "entry": [
                    {"resource": {"resourceType": "Patient", "id": "p1"}},
                    {"resource": {"resourceType": "Observation", "id": "o1"}},
                ],
            },
            {
                "resourceType": "Bundle",
                "id": "bundle-2",
                "entry": [
                    {"resource": {"resourceType": "Patient", "id": "p2"}},
                    {"resource": {"resourceType": "Condition", "id": "c1"}},
                ],
            },
        ]

        con = duckdb.connect(":memory:")

        for bundle_json in bundles:
            bundle_id = bundle_json.get("id")

            # Group by type
            resources_by_type: dict[str, list[dict[str, Any]]] = {}
            for entry in bundle_json.get("entry", []):
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType")
                if resource_type:
                    if resource_type not in resources_by_type:
                        resources_by_type[resource_type] = []
                    resources_by_type[resource_type].append(
                        {"_source_bundle": bundle_id, **resource}
                    )

            # Insert or append to tables
            for resource_type, resources in resources_by_type.items():
                table_name = resource_type.lower()
                tables = con.execute("SHOW TABLES").fetchdf()

                if table_name in tables["name"].values:
                    _insert_into_table(con, table_name, resources)
                else:
                    _create_table_from_resources(con, table_name, resources)

        # Verify all tables
        tables = con.execute("SHOW TABLES").fetchdf()
        assert set(tables["name"]) == {"patient", "observation", "condition"}

        # Patient table has resources from both bundles
        patients = con.execute("SELECT id, _source_bundle FROM patient").fetchdf()
        assert len(patients) == 2
        assert set(patients["_source_bundle"]) == {"bundle-1", "bundle-2"}

    def test_nested_json_preserved(self):
        """Verify nested JSON structures are preserved in DuckDB."""
        bundle_json = {
            "resourceType": "Bundle",
            "id": "bundle-nested",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": "p1",
                        "name": [
                            {"family": "Smith", "given": ["John", "James"]},
                            {"family": "Smith", "given": ["Johnny"], "use": "nickname"},
                        ],
                        "address": [{"city": "Helsinki", "country": "Finland"}],
                    }
                }
            ],
        }

        con = duckdb.connect(":memory:")

        resources = []
        for entry in bundle_json.get("entry", []):
            resource = entry.get("resource", {})
            resources.append(resource)

        _create_table_from_resources(con, "patient", resources)

        # Query nested data
        result = con.execute("SELECT id, name, address FROM patient").fetchdf()
        assert len(result) == 1

        # Nested arrays preserved
        names = result.iloc[0]["name"]
        assert len(names) == 2
        assert names[0]["family"] == "Smith"
        assert names[0]["given"] == ["John", "James"]


@pytest.mark.skipif(not DATA_DIR.exists(), reason="EPS data not available")
class TestRealEpsData:
    """Test with real European Patient Summary data."""

    def test_load_eps_bundles_to_tables(self):
        """Load all EPS bundles into per-resource-type tables.

        Collects all resources first, then creates tables once to handle
        schema variations across different FHIR resources of the same type.
        """
        json_files = sorted(DATA_DIR.glob("*.json"))
        assert len(json_files) > 0, "Expected JSON files"

        # Collect all resources by type across all bundles
        all_resources_by_type: dict[str, list[dict[str, Any]]] = {}

        for json_path in json_files:
            bundle_json = json.loads(json_path.read_text())
            bundle_id = bundle_json.get("id")

            for entry in bundle_json.get("entry", []):
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType")
                if resource_type:
                    if resource_type not in all_resources_by_type:
                        all_resources_by_type[resource_type] = []
                    all_resources_by_type[resource_type].append(
                        {
                            "_source_file": json_path.name,
                            "_source_bundle": bundle_id,
                            "_full_url": entry.get("fullUrl"),
                            **resource,
                        }
                    )

        # Create tables once with all resources (PyArrow handles schema union)
        con = duckdb.connect(":memory:")

        for resource_type, resources in all_resources_by_type.items():
            table_name = resource_type.lower()
            _create_table_from_resources(con, table_name, resources)

        # Report tables created
        tables = con.execute("SHOW TABLES").fetchdf()
        print(f"\nTables created: {sorted(tables['name'].tolist())}")

        # Count per table
        for table_name in sorted(tables["name"]):
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name}: {count} rows")

        # Verify key tables exist
        assert "patient" in tables["name"].values
        assert "observation" in tables["name"].values

    def test_query_across_resource_tables(self):
        """Demonstrate querying across resource type tables."""
        json_files = sorted(DATA_DIR.glob("*.json"))[:5]  # Sample for speed

        # Collect all resources by type
        all_resources_by_type: dict[str, list[dict[str, Any]]] = {}

        for json_path in json_files:
            bundle_json = json.loads(json_path.read_text())

            for entry in bundle_json.get("entry", []):
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType")
                if resource_type:
                    if resource_type not in all_resources_by_type:
                        all_resources_by_type[resource_type] = []
                    all_resources_by_type[resource_type].append(
                        {"_source_file": json_path.name, **resource}
                    )

        # Create tables
        con = duckdb.connect(":memory:")

        for resource_type, resources in all_resources_by_type.items():
            table_name = resource_type.lower()
            _create_table_from_resources(con, table_name, resources)

        # Example queries

        # 1. Count observations per patient bundle
        obs_per_bundle = con.execute("""
            SELECT _source_file, COUNT(*) as obs_count
            FROM observation
            GROUP BY _source_file
            ORDER BY obs_count DESC
        """).fetchdf()
        print(f"\nObservations per bundle:\n{obs_per_bundle.to_string()}")

        # 2. Resource type distribution
        tables = con.execute("SHOW TABLES").fetchdf()
        print("\nResource distribution:")
        for table_name in sorted(tables["name"]):
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name}: {count}")
