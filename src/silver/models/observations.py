"""Observation model: Domain-modeled from sources.

Sources now provides flat, known columns for observations. Models add
model-level fields (like validation errors), run validations, and ensure
the typed schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from src.common.models import OBSERVATION_SCHEMA, Observation


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
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}")


def _has_any_value(_: Observation) -> pl.Expr:
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


def _with_validation_errors(model_lf: pl.LazyFrame) -> pl.LazyFrame:
    error_exprs: list[pl.Expr] = []
    for rule in OBSERVATION_VALIDATION_RULES:
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


def get_observation(sources_lf: pl.LazyFrame) -> Observation:
    """Get observation model by transforming sources data.

    Args:
        sources_lf: Sources Observation LazyFrame (from silver.sources.observations.get_observation)

    Returns:
        Typed Observation LazyFrame with domain model
    """
    return transform(sources_lf)


def transform(sources_lf: pl.LazyFrame) -> Observation:
    """Transform sources LazyFrame to observation domain model.

    Args:
        sources_lf: Sources Observation LazyFrame (from silver.sources.observations.get_observation)

    Returns:
        Typed Observation LazyFrame with domain model
    """
    model_lf = _with_validation_errors(sources_lf)
    return Observation.from_df(
        model_lf.select(list(OBSERVATION_SCHEMA.keys())),
        validate=False,
    )
