"""Validation rules for silver.condition data using Polars."""

from dataclasses import dataclass
from typing import Callable

import polars as pl

from ..transformations.conditions import SilverCondition


@dataclass
class ValidationRule:
    """A validation rule with name and check expression."""

    name: str
    check: Callable[[pl.LazyFrame], pl.Expr]
    description: str


# Valid SNOMED CT system URL
SNOMED_SYSTEM = "http://snomed.info/sct"

# Valid condition category codes
VALID_CATEGORY_CODES = [
    "problem-list-item",
    "encounter-diagnosis",
]


def _is_valid_date_format(col: pl.Expr) -> pl.Expr:
    """Check if date string matches YYYY-MM-DD format."""
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}$")


def _is_not_null(col: pl.Expr) -> pl.Expr:
    """Check if value is not null."""
    return col.is_not_null()


# Define validation rules for condition
CONDITION_VALIDATION_RULES: list[ValidationRule] = [
    ValidationRule(
        name="id_required",
        check=lambda _: _is_not_null(SilverCondition.id),
        description="Condition ID must not be null",
    ),
    ValidationRule(
        name="code_required",
        check=lambda _: _is_not_null(SilverCondition.code),
        description="Condition must have a diagnosis code",
    ),
    ValidationRule(
        name="code_display_required",
        check=lambda _: _is_not_null(SilverCondition.code_display),
        description="Condition must have a diagnosis display name",
    ),
    ValidationRule(
        name="patient_id_required",
        check=lambda _: _is_not_null(SilverCondition.patient_id),
        description="Condition must be linked to a patient",
    ),
    ValidationRule(
        name="onset_date_format",
        check=lambda _: SilverCondition.onset_date.is_null()
        | _is_valid_date_format(SilverCondition.onset_date),
        description="Onset date must be in YYYY-MM-DD format",
    ),
    ValidationRule(
        name="abatement_date_format",
        check=lambda _: SilverCondition.abatement_date.is_null()
        | _is_valid_date_format(SilverCondition.abatement_date),
        description="Abatement date must be in YYYY-MM-DD format",
    ),
    ValidationRule(
        name="valid_code_system",
        check=lambda _: SilverCondition.code_system.is_null()
        | (SilverCondition.code_system == SNOMED_SYSTEM),
        description="Code system should be SNOMED CT",
    ),
]


def validate_condition(silver_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Validate silver condition data and populate validation_errors column.

    Args:
        silver_lf: Polars LazyFrame with silver condition data

    Returns:
        Polars LazyFrame with validation_errors populated
    """
    error_exprs = []
    for rule in CONDITION_VALIDATION_RULES:
        error_expr = (
            pl.when(~rule.check(silver_lf))
            .then(pl.lit(rule.name))
            .otherwise(pl.lit(None))
        )
        error_exprs.append(error_expr)

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


def get_valid_conditions(validated_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filter to only valid conditions (no validation errors)."""
    return validated_lf.filter(pl.col("validation_errors").list.len() == 0)


def get_invalid_conditions(validated_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filter to only invalid conditions (has validation errors)."""
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
