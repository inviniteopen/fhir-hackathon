"""Sources Condition: Cleaned bronze with source metadata and flattened columns.

Sources flattens nested FHIR structures into a known column set:
- Source tracking (source_file, source_bundle)
- Patient reference fields (patient_id, patient_display)
- CodeableConcept fields (category/code)
- Onset/abatement dates

From sources onward, Condition tables are flat. Models can rely on column names
without inspecting nested FHIR structures.
"""

from typing import Any

import polars as pl

from src.common.fhir import (
    extract_category_from_list,
    extract_first_coding_as_dict,
    extract_reference_id,
)

_SOURCES_SCHEMA = {
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
}


def extract_patient_id(subject: dict[str, Any] | None) -> str | None:
    """Extract patient ID from subject reference."""
    if not subject:
        return None
    patient_ref = subject.get("reference")
    return extract_reference_id(patient_ref)


def extract_patient_display(subject: dict[str, Any] | None) -> str | None:
    """Extract patient display from subject reference."""
    if not subject:
        return None
    return subject.get("display")


def extract_category_code(category_list: list[dict] | None) -> str | None:
    """Extract category code from condition category array."""
    category = extract_category_from_list(category_list)
    return category["code"]


def extract_category_display(category_list: list[dict] | None) -> str | None:
    """Extract category display from condition category array."""
    category = extract_category_from_list(category_list)
    return category["display"]


def extract_code_system(code_obj: dict[str, Any] | None) -> str | None:
    """Extract code system from condition code."""
    if not code_obj:
        return None
    coding = extract_first_coding_as_dict(code_obj)
    return coding["system"]


def extract_code(code_obj: dict[str, Any] | None) -> str | None:
    """Extract code from condition code."""
    if not code_obj:
        return None
    coding = extract_first_coding_as_dict(code_obj)
    return coding["code"]


def extract_code_display(code_obj: dict[str, Any] | None) -> str | None:
    """Extract code display from condition code."""
    if not code_obj:
        return None
    coding = extract_first_coding_as_dict(code_obj)
    return coding["display"]


def extract_code_text(code_obj: dict[str, Any] | None) -> str | None:
    """Extract code text from condition code."""
    if not code_obj:
        return None
    return code_obj.get("text")


def extract_onset_date(row: dict[str, Any]) -> str | None:
    """Extract onset date from onsetDateTime or _onsetDateTime extension."""
    onset_date = row.get("onsetDateTime")
    if onset_date is not None:
        return onset_date
    onset_ext = row.get("_onsetDateTime")
    if isinstance(onset_ext, dict):
        return onset_ext.get("value")
    return None


def extract_abatement_date(row: dict[str, Any]) -> str | None:
    """Extract abatement date from abatementDateTime or _abatementDateTime extension."""
    abatement_date = row.get("abatementDateTime")
    if abatement_date is not None:
        return abatement_date
    abatement_ext = row.get("_abatementDateTime")
    if isinstance(abatement_ext, dict):
        return abatement_ext.get("value")
    return None


def extract_asserter_display(asserter: dict[str, Any] | None) -> str | None:
    """Extract asserter display from asserter reference."""
    if not asserter:
        return None
    return asserter.get("display")


def _transform_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze condition row to a flat sources row."""
    subject = row.get("subject") or {}
    category_list = row.get("category")
    code_obj = row.get("code") or {}
    asserter = row.get("asserter") or {}

    return {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "patient_id": extract_patient_id(subject),
        "patient_display": extract_patient_display(subject),
        "category_code": extract_category_code(category_list),
        "category_display": extract_category_display(category_list),
        "code_system": extract_code_system(code_obj),
        "code": extract_code(code_obj),
        "code_display": extract_code_display(code_obj),
        "code_text": extract_code_text(code_obj),
        "onset_date": extract_onset_date(row),
        "abatement_date": extract_abatement_date(row),
        "asserter_display": extract_asserter_display(asserter),
    }


def get_condition(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Get sources condition from bronze data.

    Returns a flat sources LazyFrame with stable, known columns.
    """
    rows = bronze_df.to_dicts()
    source_rows = [_transform_row(row) for row in rows]
    sources_df = pl.from_dicts(source_rows, schema=_SOURCES_SCHEMA)
    return sources_df.lazy()
