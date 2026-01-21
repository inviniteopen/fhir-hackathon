"""Validation rules for observation model data using Polars."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from src.common.models import Observation


@dataclass
class ValidationRule:
    """A validation rule with name and check expression."""

    name: str
    check: Callable[[Observation], pl.Expr]
    description: str


VALID_OBSERVATION_STATUSES = [
    "registered",
    "preliminary",
    "final",
    "amended",
    "corrected",
    "cancelled",
    "entered-in-error",
    "unknown",
]


def _is_not_null(col: pl.Expr) -> pl.Expr:
    return col.is_not_null()


def _looks_like_date_or_datetime(col: pl.Expr) -> pl.Expr:
    """Basic check: starts with YYYY-MM-DD (covers date and datetime strings)."""
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}")


def _has_any_value(lf: Observation) -> pl.Expr:
    return (
        Observation.value_quantity_value.is_not_null()
        | Observation.value_codeable_concept_code.is_not_null()
        | Observation.value_string.is_not_null()
        | Observation.value_boolean.is_not_null()
        | Observation.value_integer.is_not_null()
        | Observation.value_datetime.is_not_null()
        | (Observation.component_count > 0)
    )


OBSERVATION_VALIDATION_RULES: list[ValidationRule] = [
    ValidationRule(
        name="id_required",
        check=lambda _: _is_not_null(Observation.id),
        description="Observation id must not be null",
    ),
    ValidationRule(
        name="status_required",
        check=lambda _: _is_not_null(Observation.status),
        description="Observation status must not be null",
    ),
    ValidationRule(
        name="status_valid",
        check=lambda _: Observation.status.is_in(VALID_OBSERVATION_STATUSES),
        description=f"Observation status must be one of: {', '.join(VALID_OBSERVATION_STATUSES)}",
    ),
    ValidationRule(
        name="code_present",
        check=lambda _: Observation.code_code.is_not_null()
        | Observation.code_text.is_not_null(),
        description="Observation must have a code (code_code or code_text)",
    ),
    ValidationRule(
        name="subject_present",
        check=lambda _: Observation.subject_reference.is_not_null(),
        description="Observation must have a subject reference",
    ),
    ValidationRule(
        name="effective_format",
        check=lambda _: Observation.effective_datetime.is_null()
        | _looks_like_date_or_datetime(Observation.effective_datetime),
        description="effective_datetime should start with YYYY-MM-DD when present",
    ),
    ValidationRule(
        name="has_value_or_components",
        check=_has_any_value,
        description="Observation should have a value or at least one component",
    ),
    ValidationRule(
        name="quantity_unit_if_value",
        check=lambda _: Observation.value_quantity_value.is_null()
        | Observation.value_quantity_unit.is_not_null(),
        description="If value_quantity_value is present, value_quantity_unit must be present",
    ),
    ValidationRule(
        name="quantity_value_finite",
        check=lambda _: Observation.value_quantity_value.is_null()
        | Observation.value_quantity_value.is_finite(),
        description="If value_quantity_value is present, it must be finite",
    ),
]


def validate_observation(silver_lf: Observation) -> Observation:
    """Validate observation model and populate validation_errors column."""
    error_exprs: list[pl.Expr] = []
    for rule in OBSERVATION_VALIDATION_RULES:
        error_exprs.append(
            pl.when(~rule.check(silver_lf))
            .then(pl.lit(rule.name))
            .otherwise(pl.lit(None))
        )
    validated_lf = silver_lf.with_columns(
        pl.concat_list(error_exprs)
        .list.eval(pl.element().drop_nulls())
        .alias("validation_errors")
    )
    return Observation.from_df(validated_lf, validate=False)


def get_validation_summary(validated_lf: Observation) -> pl.DataFrame:
    """Get summary of validation errors."""
    return (
        validated_lf.select(Observation.validation_errors.list.explode().alias("error"))
        .filter(pl.col("error").is_not_null())
        .group_by("error")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .collect()
    )


def get_valid_observations(validated_lf: Observation) -> Observation:
    """Filter to only valid observations (no validation errors)."""
    return Observation.from_df(
        validated_lf.filter(Observation.validation_errors.list.len() == 0),
        validate=False,
    )


def get_invalid_observations(validated_lf: Observation) -> Observation:
    """Filter to only invalid observations (has validation errors)."""
    return Observation.from_df(
        validated_lf.filter(Observation.validation_errors.list.len() > 0),
        validate=False,
    )


def get_validation_report(validated_lf: Observation) -> dict:
    """Generate a validation report."""
    df = validated_lf.collect()
    total = len(df)
    valid = df.filter(pl.col("validation_errors").list.len() == 0).height
    invalid = total - valid

    error_summary = get_validation_summary(validated_lf)

    return {
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "validity_rate": valid / total if total > 0 else 0.0,
        "errors_by_rule": error_summary.to_dicts(),
    }
