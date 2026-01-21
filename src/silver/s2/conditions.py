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
from src.common.models import Condition

# =============================================================================
# Column extraction functions - focused functions for extracting specific fields
# =============================================================================


def extract_patient_id(subject: dict[str, Any]) -> str | None:
    """Extract patient ID from subject reference."""
    patient_ref = subject.get("reference")
    return extract_reference_id(patient_ref)


def extract_patient_display(subject: dict[str, Any]) -> str | None:
    """Extract patient display from subject reference."""
    return subject.get("display")


def extract_category_code(category_list: list[dict]) -> str | None:
    """Extract category code from condition category array."""
    category = extract_category_from_list(category_list)
    return category["code"]


def extract_category_display(category_list: list[dict]) -> str | None:
    """Extract category display from condition category array."""
    category = extract_category_from_list(category_list)
    return category["display"]


def extract_code_system(code_obj: dict[str, Any]) -> str | None:
    """Extract code system from condition code."""
    coding = extract_first_coding_as_dict(code_obj)
    return coding["system"]


def extract_code(code_obj: dict[str, Any]) -> str | None:
    """Extract code from condition code."""
    coding = extract_first_coding_as_dict(code_obj)
    return coding["code"]


def extract_code_display(code_obj: dict[str, Any]) -> str | None:
    """Extract code display from condition code."""
    coding = extract_first_coding_as_dict(code_obj)
    return coding["display"]


def extract_code_text(code_obj: dict[str, Any]) -> str | None:
    """Extract code text from condition code."""
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


def extract_asserter_display(asserter: dict[str, Any]) -> str | None:
    """Extract asserter display from asserter reference."""
    return asserter.get("display")


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
    def coalesce_columns(df: pl.DataFrame, *names: str) -> pl.Expr:
        available = [pl.col(name) for name in names if name in df.columns]
        if not available:
            return pl.lit(None)
        if len(available) == 1:
            return available[0]
        return pl.coalesce(available)

    def struct_or_null_fields(df: pl.DataFrame, *names: str) -> pl.Expr:
        fields: list[pl.Expr] = []
        for name in names:
            if name in df.columns:
                fields.append(pl.col(name))
            else:
                fields.append(pl.lit(None).alias(name))
        return pl.struct(fields)

    silver_lf = bronze_df.lazy().select(
        pl.col("id"),
        coalesce_columns(bronze_df, "source_file", "_source_file").alias("source_file"),
        coalesce_columns(bronze_df, "source_bundle", "_source_bundle").alias(
            "source_bundle"
        ),
        pl.col("subject")
        .map_elements(extract_patient_id, return_dtype=pl.String)
        .alias("patient_id"),
        pl.col("subject")
        .map_elements(extract_patient_display, return_dtype=pl.String)
        .alias("patient_display"),
        pl.col("category")
        .map_elements(extract_category_code, return_dtype=pl.String)
        .alias("category_code"),
        pl.col("category")
        .map_elements(extract_category_display, return_dtype=pl.String)
        .alias("category_display"),
        pl.col("code")
        .map_elements(extract_code_system, return_dtype=pl.String)
        .alias("code_system"),
        pl.col("code")
        .map_elements(extract_code, return_dtype=pl.String)
        .alias("code"),
        pl.col("code")
        .map_elements(extract_code_display, return_dtype=pl.String)
        .alias("code_display"),
        pl.col("code")
        .map_elements(extract_code_text, return_dtype=pl.String)
        .alias("code_text"),
        struct_or_null_fields(bronze_df, "onsetDateTime", "_onsetDateTime")
        .map_elements(extract_onset_date, return_dtype=pl.String)
        .alias("onset_date"),
        struct_or_null_fields(bronze_df, "abatementDateTime", "_abatementDateTime")
        .map_elements(extract_abatement_date, return_dtype=pl.String)
        .alias("abatement_date"),
        pl.col("asserter")
        .map_elements(extract_asserter_display, return_dtype=pl.String)
        .alias("asserter_display"),
        pl.lit([]).cast(pl.List(pl.String)).alias("validation_errors"),
    )
    return Condition.from_df(silver_lf)


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
