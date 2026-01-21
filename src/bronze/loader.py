"""Load FHIR Bundle JSON files into in-memory dataframes by resource type."""

import json
from pathlib import Path
from typing import Any

import polars as pl

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
                    _annotate_resource(resource, entry, json_path.name, bundle_id)
                )

    return resources_by_type


def _annotate_resource(
    resource: dict[str, Any],
    entry: dict[str, Any],
    source_file: str,
    bundle_id: str | None,
) -> dict[str, Any]:
    """Attach metadata fields to a resource from its bundle entry."""
    return {
        "_source_file": source_file,
        "_source_bundle": bundle_id,
        "_full_url": entry.get("fullUrl"),
        **resource,
    }


def _frames_from_resources(
    resources_by_type: dict[str, list[dict[str, Any]]],
) -> dict[str, pl.DataFrame]:
    return {
        resource_type.lower(): pl.DataFrame(resources)
        for resource_type, resources in resources_by_type.items()
    }


def load_bronze_bundles(
    bundle_dir: Path,
) -> dict[str, pl.DataFrame]:
    """Load all FHIR Bundle JSON files from a directory into dataframes.

    Creates one dataframe per resource type (e.g., patient, observation, condition).
    Each resource includes metadata fields:
    - _source_file: name of the source JSON file
    - _source_bundle: bundle ID
    - _full_url: fullUrl from the bundle entry
    """
    json_files = sorted(bundle_dir.glob("*.json"))
    if not json_files:
        return {}

    resources_by_type = _collect_resources_by_type(json_files)

    return _frames_from_resources(resources_by_type)


def load_bronze_bundle_file(
    bundle_path: Path,
) -> dict[str, pl.DataFrame]:
    """Load a single FHIR Bundle JSON file into dataframes."""
    resources_by_type = _collect_resources_by_type([bundle_path])

    return _frames_from_resources(resources_by_type)
