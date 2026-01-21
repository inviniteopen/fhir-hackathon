"""Condition model: Domain-modeled from sources.

Sources now provides flat, known columns for conditions. Models add
model-level fields (like validation errors), run validations, and ensure
the typed schema.
"""

from dataclasses import dataclass
from typing import Callable

import polars as pl

from src.common.models import CONDITION_SCHEMA, Condition


@dataclass
class ValidationRule:
    """A validation rule with name and check expression."""

    name: str
    check: Callable[[pl.LazyFrame], pl.Expr]
    description: str


SNOMED_SYSTEM = "http://snomed.info/sct"

VALID_CATEGORY_CODES = [
    "problem-list-item",
    "encounter-diagnosis",
]


def _is_valid_date_format(col: pl.Expr) -> pl.Expr:
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}$")


def _is_not_null(col: pl.Expr) -> pl.Expr:
    return col.is_not_null()


CONDITION_VALIDATION_RULES: list[ValidationRule] = [
    ValidationRule(
        name="id_required",
        check=lambda _: _is_not_null(Condition.id),
        description="Condition ID must not be null",
    ),
    ValidationRule(
        name="code_required",
        check=lambda _: _is_not_null(Condition.code),
        description="Condition must have a diagnosis code",
    ),
    ValidationRule(
        name="code_display_required",
        check=lambda _: _is_not_null(Condition.code_display),
        description="Condition must have a diagnosis display name",
    ),
    ValidationRule(
        name="patient_id_required",
        check=lambda _: _is_not_null(Condition.patient_id),
        description="Condition must be linked to a patient",
    ),
    ValidationRule(
        name="onset_date_format",
        check=lambda _: Condition.onset_date.is_null()
        | _is_valid_date_format(Condition.onset_date),
        description="Onset date must be in YYYY-MM-DD format",
    ),
    ValidationRule(
        name="abatement_date_format",
        check=lambda _: Condition.abatement_date.is_null()
        | _is_valid_date_format(Condition.abatement_date),
        description="Abatement date must be in YYYY-MM-DD format",
    ),
    ValidationRule(
        name="valid_code_system",
        check=lambda _: Condition.code_system.is_null()
        | (Condition.code_system == SNOMED_SYSTEM),
        description="Code system should be SNOMED CT",
    ),
]


def _with_validation_errors(model_lf: pl.LazyFrame) -> pl.LazyFrame:
    error_exprs: list[pl.Expr] = []
    for rule in CONDITION_VALIDATION_RULES:
        error_exprs.append(
            pl.when(~rule.check(model_lf))
            .then(pl.lit(rule.name))
            .otherwise(pl.lit(None))
        )
    return model_lf.with_columns(
        pl.concat_list(error_exprs)
        .list.eval(pl.element().drop_nulls())
        .alias("validation_errors")
    )


def get_condition(sources_lf: pl.LazyFrame) -> Condition:
    """Get condition model by transforming sources data.

    Args:
        sources_lf: Sources Condition LazyFrame (from silver.sources.conditions.get_condition)

    Returns:
        Typed Condition LazyFrame with domain model
    """
    return transform(sources_lf)


def transform(sources_lf: pl.LazyFrame) -> Condition:
    """Transform sources LazyFrame to condition domain model.

    Args:
        sources_lf: Sources Condition LazyFrame (from silver.sources.conditions.get_condition)

    Returns:
        Typed Condition LazyFrame with domain model
    """
    model_lf = _with_validation_errors(sources_lf)
    return Condition.from_df(model_lf.select(list(CONDITION_SCHEMA.keys())), validate=True)


def validate_condition(model_lf: pl.LazyFrame) -> Condition:
    """Validate condition model and populate validation_errors column."""
    validated_lf = _with_validation_errors(model_lf)
    return Condition.from_df(
        validated_lf.select(list(CONDITION_SCHEMA.keys())),
        validate=False,
    )


def get_validation_summary(validated_lf: pl.LazyFrame) -> pl.DataFrame:
    """Get summary of validation errors."""
    return (
        validated_lf.select(pl.col("validation_errors").list.explode().alias("error"))
        .filter(pl.col("error").is_not_null())
        .group_by("error")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .collect()
    )


def get_valid_conditions(validated_lf: pl.LazyFrame) -> Condition:
    """Filter to only valid conditions (no validation errors)."""
    return Condition.from_df(
        validated_lf.filter(pl.col("validation_errors").list.len() == 0).select(
            list(CONDITION_SCHEMA.keys())
        ),
        validate=False,
    )


def get_invalid_conditions(validated_lf: pl.LazyFrame) -> Condition:
    """Filter to only invalid conditions (has validation errors)."""
    return Condition.from_df(
        validated_lf.filter(pl.col("validation_errors").list.len() > 0).select(
            list(CONDITION_SCHEMA.keys())
        ),
        validate=False,
    )


def get_validation_report(validated_lf: pl.LazyFrame) -> dict:
    """Generate a validation report."""
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
