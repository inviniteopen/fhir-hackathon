"""Transform bronze.condition to silver.condition with flattened fields using Polars."""

from pathlib import Path
from typing import Optional

import polars as pl

from das.engine.polars.typed_dataframe import Col, TypedLazyFrame

from ..constants import SILVER_SCHEMA


class SilverCondition(TypedLazyFrame):
    """Silver layer Condition schema - flattened diagnoses/problems."""

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    patient_id: Col[Optional[str]]
    patient_display: Col[Optional[str]]
    category_code: Col[Optional[str]]
    category_display: Col[Optional[str]]
    code_system: Col[Optional[str]]
    code: Col[Optional[str]]
    code_display: Col[Optional[str]]
    code_text: Col[Optional[str]]
    onset_date: Col[Optional[str]]
    abatement_date: Col[Optional[str]]
    asserter_display: Col[Optional[str]]
    validation_errors: Col[list[str]]


def _extract_reference_id(reference: str | None) -> str | None:
    """Extract ID from FHIR reference string (e.g., 'Patient/123' -> '123')."""
    if not reference:
        return None
    if "/" in reference:
        return reference.split("/")[-1]
    if reference.startswith("urn:uuid:"):
        return reference.replace("urn:uuid:", "")
    return reference


def _extract_first_coding(codeable_concept: dict | None) -> dict:
    """Extract first coding from a CodeableConcept."""
    if not codeable_concept:
        return {"system": None, "code": None, "display": None}
    codings = codeable_concept.get("coding", [])
    if codings and isinstance(codings, list) and len(codings) > 0:
        coding = codings[0]
        return {
            "system": coding.get("system"),
            "code": coding.get("code"),
            "display": coding.get("display"),
        }
    return {"system": None, "code": None, "display": None}


def _extract_category(category_list: list[dict] | None) -> dict:
    """Extract category code and display from category array."""
    if not category_list or not isinstance(category_list, list):
        return {"code": None, "display": None}
    for cat in category_list:
        if isinstance(cat, dict):
            coding = _extract_first_coding(cat)
            if coding.get("code"):
                return {"code": coding["code"], "display": coding["display"]}
    return {"code": None, "display": None}


def transform_condition_row(row: dict) -> dict:
    """Transform a single bronze condition row to silver format."""
    # Extract subject (patient) reference
    subject = row.get("subject", {}) or {}
    patient_ref = subject.get("reference")
    patient_display = subject.get("display")

    # Extract category
    category = _extract_category(row.get("category"))

    # Extract code (diagnosis)
    code_obj = row.get("code", {}) or {}
    code_coding = _extract_first_coding(code_obj)

    # Extract asserter
    asserter = row.get("asserter", {}) or {}

    # Handle onset date - may be in onsetDateTime or need extraction from _onsetDateTime
    onset_date = row.get("onsetDateTime")
    if onset_date is None:
        # Try _onsetDateTime extension format (FHIR uses this when value has extensions)
        onset_ext = row.get("_onsetDateTime")
        if isinstance(onset_ext, dict):
            # Value may be in extension or as direct value
            onset_date = onset_ext.get("value")

    # Handle abatement date similarly
    abatement_date = row.get("abatementDateTime")
    if abatement_date is None:
        abatement_ext = row.get("_abatementDateTime")
        if isinstance(abatement_ext, dict):
            abatement_date = abatement_ext.get("value")

    return {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "patient_id": _extract_reference_id(patient_ref),
        "patient_display": patient_display,
        "category_code": category["code"],
        "category_display": category["display"],
        "code_system": code_coding["system"],
        "code": code_coding["code"],
        "code_display": code_coding["display"],
        "code_text": code_obj.get("text"),
        "onset_date": onset_date,
        "abatement_date": abatement_date,
        "asserter_display": asserter.get("display"),
        "validation_errors": [],
    }


SILVER_CONDITION_SCHEMA = {
    "id": pl.String,
    "source_file": pl.String,
    "source_bundle": pl.String,
    "patient_id": pl.String,
    "patient_display": pl.String,
    "category_code": pl.String,
    "category_display": pl.String,
    "code_system": pl.String,
    "code": pl.String,
    "code_display": pl.String,
    "code_text": pl.String,
    "onset_date": pl.String,
    "abatement_date": pl.String,
    "asserter_display": pl.String,
    "validation_errors": pl.List(pl.String),
}


def transform_condition(bronze_df: pl.DataFrame) -> SilverCondition:
    """
    Transform bronze condition DataFrame to silver format.

    Args:
        bronze_df: Polars DataFrame with bronze condition data

    Returns:
        SilverCondition TypedLazyFrame with validated silver condition data
    """
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_condition_row(row) for row in bronze_rows]

    return SilverCondition.from_dicts(silver_rows, SILVER_CONDITION_SCHEMA)


def get_condition_summary(silver_lf: SilverCondition | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for silver condition data."""
    return (
        silver_lf.select(
            pl.len().alias("total_conditions"),
            SilverCondition.patient_id.drop_nulls().len().alias("with_patient_id"),
            SilverCondition.code.drop_nulls().len().alias("with_code"),
            SilverCondition.code_display.drop_nulls().len().alias("with_code_display"),
            SilverCondition.onset_date.drop_nulls().len().alias("with_onset_date"),
            SilverCondition.category_code.drop_nulls().len().alias("with_category"),
        )
        .collect()
        .to_dicts()[0]
    )
