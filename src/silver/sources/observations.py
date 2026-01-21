"""Sources Observation: Cleaned bronze with source metadata and flattened columns.

Sources flattens nested FHIR structures into a known column set:
- Source tracking (source_file, source_bundle)
- CodeableConcept and category details
- Normalized references and value[x] fields
- Component and coding arrays

From sources onward, Observation tables are flat. Models can rely on column names
without inspecting nested FHIR structures.
"""

from typing import Any

import polars as pl

from src.common.constants import ObservationValueType
from src.common.fhir import (
    extract_code_text,
    extract_primary_coding,
    extract_reference,
    extract_reference_id,
    iter_codings,
    iter_dict_list,
)
from src.common.models import OBSERVATION_SCHEMA

_SOURCES_SCHEMA = dict(OBSERVATION_SCHEMA)
_SOURCES_SCHEMA.pop("validation_errors", None)


# =============================================================================
# Column extraction functions - focused functions for extracting specific fields
# =============================================================================


def extract_effective_datetime(row: dict[str, Any]) -> str | None:
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


def detect_value_type(value_obj: dict[str, Any]) -> str | None:
    """Identify which value[x] type is present in an observation or component."""
    value_quantity = value_obj.get("valueQuantity")
    if isinstance(value_quantity, dict):
        return ObservationValueType.QUANTITY

    value_cc = value_obj.get("valueCodeableConcept")
    if isinstance(value_cc, dict):
        return ObservationValueType.CODEABLE_CONCEPT

    type_mapping = (
        ("valueString", ObservationValueType.STRING),
        ("valueBoolean", ObservationValueType.BOOLEAN),
        ("valueInteger", ObservationValueType.INTEGER),
        ("valueDateTime", ObservationValueType.DATETIME),
    )
    for key, vtype in type_mapping:
        if value_obj.get(key) is not None:
            return vtype
    return None


def extract_quantity_value(value_quantity: dict[str, Any] | None) -> float | None:
    """Extract numeric value from a FHIR Quantity."""
    if not isinstance(value_quantity, dict):
        return None
    raw_value = value_quantity.get("value")
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if raw_value is not None:
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return None
    return None


def extract_quantity_field(
    value_quantity: dict[str, Any] | None, field: str
) -> str | None:
    """Extract a string field (unit, system, code) from a FHIR Quantity."""
    if isinstance(value_quantity, dict):
        return value_quantity.get(field)
    return None


# =============================================================================
# Composite extraction functions - build complex field structures
# =============================================================================


def extract_value_fields(value_obj: dict[str, Any]) -> dict[str, Any]:
    """Extract all value[x] fields from an Observation or component."""
    value_type = detect_value_type(value_obj)
    value_quantity = value_obj.get("valueQuantity")
    value_cc = value_obj.get("valueCodeableConcept")

    cc_system, cc_code, cc_display = extract_primary_coding(value_cc)

    return {
        "value_type": value_type,
        "value_quantity_value": extract_quantity_value(value_quantity),
        "value_quantity_unit": extract_quantity_field(value_quantity, "unit"),
        "value_quantity_system": extract_quantity_field(value_quantity, "system"),
        "value_quantity_code": extract_quantity_field(value_quantity, "code"),
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


def extract_performer_fields(performer_list: list[dict[str, Any]]) -> dict[str, list]:
    """Extract performer references and IDs from performer array."""
    performer_references: list[str] = []
    performer_ids: list[str] = []
    for performer in performer_list:
        performer_reference = extract_reference(performer)
        if performer_reference is not None:
            performer_references.append(performer_reference)
        performer_id = extract_reference_id(performer_reference)
        if performer_id is not None:
            performer_ids.append(performer_id)
    return {
        "performer_references": performer_references,
        "performer_ids": performer_ids,
    }


def extract_category_fields(
    category_list: list[dict[str, Any]],
) -> dict[str, Any]:
    """Extract category text, primary coding, and all codings from category array."""
    category_obj = category_list[0] if category_list else None
    category_text = (
        str(category_obj.get("text"))
        if isinstance(category_obj, dict) and category_obj.get("text")
        else None
    )
    category_system, category_code, category_display = extract_primary_coding(
        category_obj
    )

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

    return {
        "category_text": category_text,
        "category_system": category_system,
        "category_code": category_code,
        "category_display": category_display,
        "category_codings": category_codings,
    }


def extract_code_codings(code_obj: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Extract all codings from code CodeableConcept."""
    return [
        {
            "system": coding.get("system"),
            "code": coding.get("code"),
            "display": coding.get("display"),
        }
        for coding in iter_codings(code_obj)
    ]


def extract_components(component_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract component array with flattened code and value fields."""
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
        comp_row.update(extract_value_fields(component))
        components.append(comp_row)
    return components


# =============================================================================
# Row transformation function (internal)
# =============================================================================


def _transform_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform one bronze Observation row into a flat sources row."""
    code_obj = row.get("code")
    category_list = iter_dict_list(row.get("category"))
    performer_list = iter_dict_list(row.get("performer"))
    component_list = iter_dict_list(row.get("component"))

    code_system, code_code, code_display = extract_primary_coding(code_obj)
    subject_reference = extract_reference(row.get("subject"))
    category_fields = extract_category_fields(category_list)
    performer_fields = extract_performer_fields(performer_list)

    sources_row: dict[str, Any] = {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "status": row.get("status"),
        "subject_reference": subject_reference,
        "subject_id": extract_reference_id(subject_reference),
        "effective_datetime": extract_effective_datetime(row),
        "issued": row.get("issued"),
        "category_text": category_fields["category_text"],
        "category_system": category_fields["category_system"],
        "category_code": category_fields["category_code"],
        "category_display": category_fields["category_display"],
        "code_text": extract_code_text(code_obj),
        "code_system": code_system,
        "code_code": code_code,
        "code_display": code_display,
        "performer_references": performer_fields["performer_references"],
        "performer_ids": performer_fields["performer_ids"],
        "code_codings": extract_code_codings(code_obj),
        "category_codings": category_fields["category_codings"],
        "components": extract_components(component_list),
        "component_count": len(component_list),
    }
    sources_row.update(extract_value_fields(row))
    return sources_row


# =============================================================================
# Public API: get
# =============================================================================


def get_observation(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Get sources observation from bronze data.

    Returns a flat sources LazyFrame with stable, known columns.
    """
    rows = bronze_df.to_dicts()
    source_rows = [_transform_row(row) for row in rows]
    sources_df = pl.from_dicts(source_rows, schema=_SOURCES_SCHEMA)
    return sources_df.lazy()
