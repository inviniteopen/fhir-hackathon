"""S2 Condition: Domain-modeled from FHIR source.

S2 transforms FHIR Condition toward a domain-specific analytical model:
- Flattens nested structures (code.coding → code_system, code, code_display)
- Extracts patient reference (subject.reference → patient_id)
- Normalizes onset/abatement dates
- Prepares data for domain-level analytics

This is source-specific modeling - it knows about FHIR structures but transforms
them toward common domain concepts. For unified multi-source models, see S3.
"""

from typing import Any

import polars as pl

from src.common.fhir import (
    extract_category_from_list,
    extract_first_coding_as_dict,
    extract_reference_id,
)
from src.common.models import CONDITION_SCHEMA, Condition

# =============================================================================
# Column extraction functions - focused functions for extracting specific fields
# =============================================================================


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


# =============================================================================
# Row transformation function
# =============================================================================


def transform_condition_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze condition row to S2 domain model."""
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
        "validation_errors": [],
    }


# =============================================================================
# Public API: transform and get functions
# =============================================================================


def transform_condition(bronze_df: pl.DataFrame) -> Condition:
    """Transform bronze condition DataFrame to S2 domain model.

    Args:
        bronze_df: Bronze Condition DataFrame

    Returns:
        Typed Condition LazyFrame with S2 domain model
    """
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_condition_row(row) for row in bronze_rows]
    return Condition.from_dicts(silver_rows, CONDITION_SCHEMA)


def get_condition_summary(silver_lf: Condition | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for S2 condition data.

    Args:
        silver_lf: S2 Condition LazyFrame

    Returns:
        Dictionary with counts for various condition attributes
    """
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
