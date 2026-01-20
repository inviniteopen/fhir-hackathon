"""Transform bronze.condition to silver.condition with flattened fields using Polars."""

import polars as pl

from src.common.fhir import (
    extract_category_from_list,
    extract_first_coding_as_dict,
    extract_reference_id,
)
from src.common.models import CONDITION_SCHEMA, Condition


def transform_condition_row(row: dict) -> dict:
    """Transform a single bronze condition row to silver format."""
    # Extract subject (patient) reference
    subject = row.get("subject", {}) or {}
    patient_ref = subject.get("reference")
    patient_display = subject.get("display")

    # Extract category
    category = extract_category_from_list(row.get("category"))

    # Extract code (diagnosis)
    code_obj = row.get("code", {}) or {}
    code_coding = extract_first_coding_as_dict(code_obj)

    # Extract asserter
    asserter = row.get("asserter", {}) or {}

    # Handle onset date - may be in onsetDateTime or need extraction from _onsetDateTime
    onset_date = row.get("onsetDateTime")
    if onset_date is None:
        onset_ext = row.get("_onsetDateTime")
        if isinstance(onset_ext, dict):
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
        "patient_id": extract_reference_id(patient_ref),
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


def transform_condition(bronze_df: pl.DataFrame) -> Condition:
    """Transform bronze condition DataFrame to silver format."""
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_condition_row(row) for row in bronze_rows]
    return Condition.from_dicts(silver_rows, CONDITION_SCHEMA)


def get_condition_summary(silver_lf: Condition | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for silver condition data."""
    return (
        silver_lf.select(
            pl.len().alias("total_conditions"),
            Condition.patient_id.drop_nulls().len().alias("with_patient_id"),
            Condition.code.drop_nulls().len().alias("with_code"),
            Condition.code_display.drop_nulls().len().alias("with_code_display"),
            Condition.onset_date.drop_nulls().len().alias("with_onset_date"),
            Condition.category_code.drop_nulls().len().alias("with_category"),
        )
        .collect()
        .to_dicts()[0]
    )
