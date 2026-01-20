"""Transform bronze.observation to silver.observations with flattened/unnested fields.

This keeps a single row per Observation while "unnesting" nested JSON structures into
typed list/struct columns (e.g., code codings, category codings, performers, components)
and also extracting commonly-used scalar fields (primary code, subject, effective time).
"""

from pathlib import Path
from typing import Any

import polars as pl

from src.common.fhir import (
    extract_code_text,
    extract_primary_coding,
    extract_reference,
    extract_reference_id,
    iter_codings,
    iter_dict_list,
)
from src.common.models import OBSERVATION_SCHEMA, Observation


def _extract_effective_datetime(row: dict[str, Any]) -> str | None:
    """Extract effective datetime from various FHIR effective[x] fields."""
    for key in ("effectiveDateTime", "effectiveInstant", "effectiveTime"):
        value = row.get(key)
        if value:
            return str(value)
    effective_period = row.get("effectivePeriod")
    if isinstance(effective_period, dict):
        start = effective_period.get("start")
        end = effective_period.get("end")
        if start and end:
            return f"{start}/{end}"
        if start:
            return str(start)
        if end:
            return str(end)
    return None


def _extract_value_fields(value_obj: dict[str, Any]) -> dict[str, Any]:
    """Extract value[x] fields from an Observation or component."""
    value_type: str | None = None

    value_quantity = value_obj.get("valueQuantity")
    value_cc = value_obj.get("valueCodeableConcept")

    quantity_value: float | None = None
    if isinstance(value_quantity, dict):
        value_type = "quantity"
        raw_value = value_quantity.get("value")
        if isinstance(raw_value, (int, float)):
            quantity_value = float(raw_value)
        elif raw_value is not None:
            try:
                quantity_value = float(raw_value)
            except (TypeError, ValueError):
                quantity_value = None

    cc_system, cc_code, cc_display = extract_primary_coding(value_cc)
    if isinstance(value_cc, dict):
        value_type = value_type or "codeable_concept"

    if value_type is None:
        for key, vtype in (
            ("valueString", "string"),
            ("valueBoolean", "boolean"),
            ("valueInteger", "integer"),
            ("valueDateTime", "datetime"),
        ):
            if value_obj.get(key) is not None:
                value_type = vtype
                break

    return {
        "value_type": value_type,
        "value_quantity_value": quantity_value,
        "value_quantity_unit": value_quantity.get("unit")
        if isinstance(value_quantity, dict)
        else None,
        "value_quantity_system": value_quantity.get("system")
        if isinstance(value_quantity, dict)
        else None,
        "value_quantity_code": value_quantity.get("code")
        if isinstance(value_quantity, dict)
        else None,
        "value_codeable_concept_text": extract_code_text(value_cc),
        "value_codeable_concept_system": cc_system,
        "value_codeable_concept_code": cc_code,
        "value_codeable_concept_display": cc_display,
        "value_string": str(value_obj.get("valueString"))
        if value_obj.get("valueString") is not None
        else None,
        "value_boolean": value_obj.get("valueBoolean")
        if isinstance(value_obj.get("valueBoolean"), bool)
        else None,
        "value_integer": value_obj.get("valueInteger")
        if isinstance(value_obj.get("valueInteger"), int)
        else None,
        "value_datetime": str(value_obj.get("valueDateTime"))
        if value_obj.get("valueDateTime") is not None
        else None,
    }


def transform_observation_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform one bronze Observation row into a single silver row."""
    observation_id = row.get("id")
    code_obj = row.get("code")
    category_list = iter_dict_list(row.get("category"))
    performer_list = iter_dict_list(row.get("performer"))
    component_list = iter_dict_list(row.get("component"))

    code_system, code_code, code_display = extract_primary_coding(code_obj)

    category_obj = category_list[0] if category_list else None
    category_text = (
        str(category_obj.get("text"))
        if isinstance(category_obj, dict) and category_obj.get("text")
        else None
    )
    category_system, category_code, category_display = extract_primary_coding(
        category_obj
    )

    subject_reference = extract_reference(row.get("subject"))

    performer_references: list[str | None] = []
    performer_ids: list[str | None] = []
    for performer in performer_list:
        performer_reference = extract_reference(performer)
        performer_references.append(performer_reference)
        performer_ids.append(extract_reference_id(performer_reference))

    code_codings = [
        {
            "system": coding.get("system"),
            "code": coding.get("code"),
            "display": coding.get("display"),
        }
        for coding in iter_codings(code_obj)
    ]

    category_codings: list[dict[str, Any]] = []
    for idx, cat in enumerate(category_list):
        for coding in iter_codings(cat):
            category_codings.append(
                {
                    "category_index": idx,
                    "system": coding.get("system"),
                    "code": coding.get("code"),
                    "display": coding.get("display"),
                }
            )

    components: list[dict[str, Any]] = []
    for idx, component in enumerate(component_list):
        component_code = component.get("code")
        comp_system, comp_code, comp_display = extract_primary_coding(component_code)
        comp_row: dict[str, Any] = {
            "component_index": idx,
            "code_text": extract_code_text(component_code),
            "code_system": comp_system,
            "code_code": comp_code,
            "code_display": comp_display,
        }
        comp_row.update(_extract_value_fields(component))
        components.append(comp_row)

    silver_row: dict[str, Any] = {
        "id": observation_id,
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "status": row.get("status"),
        "subject_reference": subject_reference,
        "subject_id": extract_reference_id(subject_reference),
        "effective_datetime": _extract_effective_datetime(row),
        "issued": row.get("issued"),
        "category_text": category_text,
        "category_system": category_system,
        "category_code": category_code,
        "category_display": category_display,
        "code_text": extract_code_text(code_obj),
        "code_system": code_system,
        "code_code": code_code,
        "code_display": code_display,
        "performer_references": performer_references,
        "performer_ids": performer_ids,
        "code_codings": code_codings,
        "category_codings": category_codings,
        "components": components,
        "component_count": len(component_list),
        "validation_errors": [],
    }
    silver_row.update(_extract_value_fields(row))
    return silver_row


def load_bronze_observations(db_path: Path) -> pl.LazyFrame:
    """Load bronze.observation from DuckDB as a Polars LazyFrame."""
    return pl.read_database_uri(
        query="SELECT * FROM bronze.observation",
        uri=f"duckdb://{db_path}",
    ).lazy()


def transform_observations(bronze_df: pl.DataFrame) -> Observation:
    """Transform bronze.observation rows into typed silver.observations."""
    silver_rows = [transform_observation_row(row) for row in bronze_df.to_dicts()]
    silver_lf = pl.DataFrame(silver_rows, schema=OBSERVATION_SCHEMA).lazy()
    return Observation.from_df(silver_lf, validate=False)


def get_observation_summary(silver_lf: Observation | pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for silver.observations."""
    return (
        silver_lf.select(
            pl.len().alias("total_observations"),
            Observation.status.drop_nulls().len().alias("with_status"),
            Observation.subject_reference.drop_nulls().len().alias("with_subject"),
            Observation.code_code.drop_nulls().len().alias("with_code"),
            Observation.effective_datetime.drop_nulls()
            .len()
            .alias("with_effective_datetime"),
            (Observation.component_count > 0).sum().alias("with_components"),
            Observation.value_type.drop_nulls().len().alias("with_value"),
            (Observation.performer_references.list.len() > 0)
            .sum()
            .alias("with_performers"),
        )
        .collect()
        .to_dicts()[0]
    )
