"""Validation rules for silver.patient data using Polars."""

from dataclasses import dataclass
from typing import Callable

import polars as pl


@dataclass
class ValidationRule:
    """A validation rule with name and check expression."""

    name: str
    check: Callable[[pl.LazyFrame], pl.Expr]
    description: str


# Valid FHIR administrative gender values
VALID_GENDERS = ["male", "female", "other", "unknown"]


def _is_valid_date_format(col: pl.Expr) -> pl.Expr:
    """Check if date string matches YYYY-MM-DD format."""
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}$")


def _is_not_null(col: pl.Expr) -> pl.Expr:
    """Check if value is not null."""
    return col.is_not_null()


def _is_valid_gender(col: pl.Expr) -> pl.Expr:
    """Check if gender is a valid FHIR administrative gender."""
    return col.is_null() | col.is_in(VALID_GENDERS)


def _is_valid_phone(col: pl.Expr) -> pl.Expr:
    """Check if phone has reasonable format (contains digits)."""
    return col.is_null() | col.str.contains(r"\d")


# Define validation rules for patient
PATIENT_VALIDATION_RULES: list[ValidationRule] = [
    ValidationRule(
        name="id_required",
        check=lambda _: _is_not_null(pl.col("id")),
        description="Patient ID must not be null",
    ),
    ValidationRule(
        name="birth_date_format",
        check=lambda _: pl.col("birth_date").is_null()
        | _is_valid_date_format(pl.col("birth_date")),
        description="Birth date must be in YYYY-MM-DD format",
    ),
    ValidationRule(
        name="gender_valid",
        check=lambda _: _is_valid_gender(pl.col("gender")),
        description="Gender must be one of: male, female, other, unknown",
    ),
    ValidationRule(
        name="phone_format",
        check=lambda _: _is_valid_phone(pl.col("phone")),
        description="Phone must contain at least one digit",
    ),
    ValidationRule(
        name="has_name",
        check=lambda _: pl.col("family_name").is_not_null()
        | pl.col("given_names").is_not_null()
        | pl.col("full_name").is_not_null(),
        description="Patient must have at least one name component",
    ),
]


def validate_patient(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Validate silver patient data and populate validation_errors column.

    Args:
        silver_lf: Polars LazyFrame with silver patient data

    Returns:
        Polars LazyFrame with validation_errors populated
    """
    # Build error collection expressions for each rule
    error_exprs = []
    for rule in PATIENT_VALIDATION_RULES:
        # When check fails, add rule name to errors
        error_expr = (
            pl.when(~rule.check(silver_lf))
            .then(pl.lit(rule.name))
            .otherwise(pl.lit(None))
        )
        error_exprs.append(error_expr)

    # Combine all errors into a list, filtering nulls
    return silver_lf.with_columns(
        pl.concat_list(error_exprs)
        .list.eval(pl.element().drop_nulls())
        .alias("validation_errors")
    )


def get_validation_summary(validated_lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Get summary of validation errors.

    Args:
        validated_lf: Polars LazyFrame with validation_errors column

    Returns:
        DataFrame with error counts per rule
    """
    return (
        validated_lf.select(pl.col("validation_errors").list.explode().alias("error"))
        .filter(pl.col("error").is_not_null())
        .group_by("error")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .collect()
    )


def get_valid_patients(validated_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filter to only valid patients (no validation errors)."""
    return validated_lf.filter(pl.col("validation_errors").list.len() == 0)


def get_invalid_patients(validated_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filter to only invalid patients (has validation errors)."""
    return validated_lf.filter(pl.col("validation_errors").list.len() > 0)


def get_validation_report(validated_lf: pl.LazyFrame) -> dict:
    """
    Generate a validation report.

    Returns:
        Dictionary with validation statistics
    """
    df = validated_lf.collect()
    total = len(df)
    valid = df.filter(pl.col("validation_errors").list.len() == 0).height
    invalid = total - valid

    error_summary = get_validation_summary(validated_lf.lazy())

    return {
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "validity_rate": valid / total if total > 0 else 0.0,
        "errors_by_rule": error_summary.to_dicts(),
    }
