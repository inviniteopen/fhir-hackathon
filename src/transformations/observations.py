"""Transform bronze.observation to silver.observations with flattened/unnested fields.

This keeps a single row per Observation while "unnesting" nested JSON structures into
typed list/struct columns (e.g., code codings, category codings, performers, components)
and also extracting commonly-used scalar fields (primary code, subject, effective time).
"""

from pathlib import Path
from typing import Any, Optional

import polars as pl

from das.engine.polars.typed_dataframe import Col, TypedLazyFrame


class SilverObservation(TypedLazyFrame):
    """Silver layer Observation schema - flattened, cleaned, and validated."""

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    status: Col[Optional[str]]
    subject_reference: Col[Optional[str]]
    subject_id: Col[Optional[str]]
    effective_datetime: Col[Optional[str]]
    issued: Col[Optional[str]]

    category_text: Col[Optional[str]]
    category_system: Col[Optional[str]]
    category_code: Col[Optional[str]]
    category_display: Col[Optional[str]]

    code_text: Col[Optional[str]]
    code_system: Col[Optional[str]]
    code_code: Col[Optional[str]]
    code_display: Col[Optional[str]]

    value_type: Col[Optional[str]]
    value_quantity_value: Col[Optional[float]]
    value_quantity_unit: Col[Optional[str]]
    value_quantity_system: Col[Optional[str]]
    value_quantity_code: Col[Optional[str]]

    value_codeable_concept_text: Col[Optional[str]]
    value_codeable_concept_system: Col[Optional[str]]
    value_codeable_concept_code: Col[Optional[str]]
    value_codeable_concept_display: Col[Optional[str]]

    value_string: Col[Optional[str]]
    value_boolean: Col[Optional[bool]]
    value_integer: Col[Optional[int]]
    value_datetime: Col[Optional[str]]

    performer_references: Col[list[Optional[str]]]
    performer_ids: Col[list[Optional[str]]]
    code_codings: Col[list[dict[str, Any]]]
    category_codings: Col[list[dict[str, Any]]]
    components: Col[list[dict[str, Any]]]
    component_count: Col[int]

    validation_errors: Col[list[str]]


def _iter_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, dict)]


def _iter_codings(codeable_concept: Any) -> list[dict[str, Any]]:
    if not isinstance(codeable_concept, dict):
        return []
    return _iter_dict_list(codeable_concept.get("coding"))


def _primary_coding_fields(
    codeable_concept: Any,
) -> tuple[str | None, str | None, str | None]:
    for coding in _iter_codings(codeable_concept):
        return (
            coding.get("system"),
            coding.get("code"),
            coding.get("display"),
        )
    return None, None, None


def _extract_code_text(codeable_concept: Any) -> str | None:
    if isinstance(codeable_concept, dict):
        text = codeable_concept.get("text")
        return str(text) if text else None
    return None


def _extract_reference(ref_obj: Any) -> str | None:
    if not isinstance(ref_obj, dict):
        return None
    ref = ref_obj.get("reference")
    return str(ref) if ref else None


def _extract_reference_id(reference: str | None) -> str | None:
    if not reference:
        return None
    if reference.startswith("urn:uuid:"):
        return reference.split("urn:uuid:", 1)[1] or None
    if "/" in reference:
        return reference.rsplit("/", 1)[-1] or None
    return reference


def _extract_effective_datetime(row: dict[str, Any]) -> str | None:
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

    cc_system, cc_code, cc_display = _primary_coding_fields(value_cc)
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
        "value_codeable_concept_text": _extract_code_text(value_cc),
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
    category_list = _iter_dict_list(row.get("category"))
    performer_list = _iter_dict_list(row.get("performer"))
    component_list = _iter_dict_list(row.get("component"))

    code_system, code_code, code_display = _primary_coding_fields(code_obj)

    category_obj = category_list[0] if category_list else None
    category_text = (
        str(category_obj.get("text")) if isinstance(category_obj, dict) and category_obj.get("text") else None
    )
    category_system, category_code, category_display = _primary_coding_fields(category_obj)

    subject_reference = _extract_reference(row.get("subject"))

    performer_references: list[str | None] = []
    performer_ids: list[str | None] = []
    for performer in performer_list:
        performer_reference = _extract_reference(performer)
        performer_references.append(performer_reference)
        performer_ids.append(_extract_reference_id(performer_reference))

    code_codings = [
        {
            "system": coding.get("system"),
            "code": coding.get("code"),
            "display": coding.get("display"),
        }
        for coding in _iter_codings(code_obj)
    ]

    category_codings: list[dict[str, Any]] = []
    for idx, cat in enumerate(category_list):
        for coding in _iter_codings(cat):
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
        comp_system, comp_code, comp_display = _primary_coding_fields(component_code)
        comp_row: dict[str, Any] = {
            "component_index": idx,
            "code_text": _extract_code_text(component_code),
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
        "subject_id": _extract_reference_id(subject_reference),
        "effective_datetime": _extract_effective_datetime(row),
        "issued": row.get("issued"),
        "category_text": category_text,
        "category_system": category_system,
        "category_code": category_code,
        "category_display": category_display,
        "code_text": _extract_code_text(code_obj),
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


CODE_CODINGS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("system", pl.String),
            pl.Field("code", pl.String),
            pl.Field("display", pl.String),
        ]
    )
)
CATEGORY_CODINGS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("category_index", pl.Int64),
            pl.Field("system", pl.String),
            pl.Field("code", pl.String),
            pl.Field("display", pl.String),
        ]
    )
)
COMPONENTS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("component_index", pl.Int64),
            pl.Field("code_text", pl.String),
            pl.Field("code_system", pl.String),
            pl.Field("code_code", pl.String),
            pl.Field("code_display", pl.String),
            pl.Field("value_type", pl.String),
            pl.Field("value_quantity_value", pl.Float64),
            pl.Field("value_quantity_unit", pl.String),
            pl.Field("value_quantity_system", pl.String),
            pl.Field("value_quantity_code", pl.String),
            pl.Field("value_codeable_concept_text", pl.String),
            pl.Field("value_codeable_concept_system", pl.String),
            pl.Field("value_codeable_concept_code", pl.String),
            pl.Field("value_codeable_concept_display", pl.String),
            pl.Field("value_string", pl.String),
            pl.Field("value_boolean", pl.Boolean),
            pl.Field("value_integer", pl.Int64),
            pl.Field("value_datetime", pl.String),
        ]
    )
)

SILVER_OBSERVATION_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.String,
    "source_file": pl.String,
    "source_bundle": pl.String,
    "status": pl.String,
    "subject_reference": pl.String,
    "subject_id": pl.String,
    "effective_datetime": pl.String,
    "issued": pl.String,
    "category_text": pl.String,
    "category_system": pl.String,
    "category_code": pl.String,
    "category_display": pl.String,
    "code_text": pl.String,
    "code_system": pl.String,
    "code_code": pl.String,
    "code_display": pl.String,
    "value_type": pl.String,
    "value_quantity_value": pl.Float64,
    "value_quantity_unit": pl.String,
    "value_quantity_system": pl.String,
    "value_quantity_code": pl.String,
    "value_codeable_concept_text": pl.String,
    "value_codeable_concept_system": pl.String,
    "value_codeable_concept_code": pl.String,
    "value_codeable_concept_display": pl.String,
    "value_string": pl.String,
    "value_boolean": pl.Boolean,
    "value_integer": pl.Int64,
    "value_datetime": pl.String,
    "performer_references": pl.List(pl.String),
    "performer_ids": pl.List(pl.String),
    "code_codings": CODE_CODINGS_TYPE,
    "category_codings": CATEGORY_CODINGS_TYPE,
    "components": COMPONENTS_TYPE,
    "component_count": pl.Int64,
    "validation_errors": pl.List(pl.String),
}


def transform_observations(bronze_df: pl.DataFrame) -> SilverObservation:
    """Transform bronze.observation rows into typed silver.observations."""
    silver_rows = [transform_observation_row(row) for row in bronze_df.to_dicts()]
    silver_lf = pl.DataFrame(silver_rows, schema=SILVER_OBSERVATION_SCHEMA).lazy()
    return SilverObservation.from_df(silver_lf, validate=False)


def get_observation_summary(silver_lf: SilverObservation | pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for silver.observations."""
    return (
        silver_lf.select(
            pl.len().alias("total_observations"),
            SilverObservation.status.drop_nulls().len().alias("with_status"),
            SilverObservation.subject_reference.drop_nulls().len().alias("with_subject"),
            SilverObservation.code_code.drop_nulls().len().alias("with_code"),
            SilverObservation.effective_datetime
            .drop_nulls()
            .len()
            .alias("with_effective_datetime"),
            (SilverObservation.component_count > 0).sum().alias("with_components"),
            SilverObservation.value_type.drop_nulls().len().alias("with_value"),
            (SilverObservation.performer_references.list.len() > 0)
            .sum()
            .alias("with_performers"),
        )
        .collect()
        .to_dicts()[0]
    )
