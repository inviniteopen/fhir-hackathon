"""Patient model: Domain-modeled from sources.

Sources now provides flat, known columns for patients. Models add
model-level fields (like validation errors), run validations, and ensure
the typed schema.
"""

from dataclasses import dataclass
from typing import Callable

import polars as pl

from src.common.models import PATIENT_SCHEMA, Patient


@dataclass
class ValidationRule:
    """A validation rule with name and check expression."""

    name: str
    check: Callable[[pl.LazyFrame], pl.Expr]
    description: str


VALID_GENDERS = ["male", "female", "other", "unknown"]


def _is_valid_date_format(col: pl.Expr) -> pl.Expr:
    return col.str.contains(r"^\d{4}-\d{2}-\d{2}$")


def _is_not_null(col: pl.Expr) -> pl.Expr:
    return col.is_not_null()


def _is_valid_gender(col: pl.Expr) -> pl.Expr:
    return col.is_null() | col.is_in(VALID_GENDERS)


def _is_valid_phone(col: pl.Expr) -> pl.Expr:
    return col.is_null() | col.str.contains(r"\d")


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


def _with_validation_errors(model_lf: pl.LazyFrame) -> pl.LazyFrame:
    error_exprs: list[pl.Expr] = []
    for rule in PATIENT_VALIDATION_RULES:
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


def get_patient(sources_lf: pl.LazyFrame) -> Patient:
    """Get patient model by transforming sources data.

    Args:
        sources_lf: Sources Patient LazyFrame (from silver.sources.patients.get_patient)

    Returns:
        Typed Patient LazyFrame with domain model
    """
    return transform(sources_lf)


def transform(sources_lf: pl.LazyFrame) -> Patient:
    """Transform sources LazyFrame to patient domain model.

    Args:
        sources_lf: Sources Patient LazyFrame (from silver.sources.patients.get_patient)

    Returns:
        Typed Patient LazyFrame with domain model
    """
    model_lf = _with_validation_errors(sources_lf)
    return Patient.from_df(model_lf.select(list(PATIENT_SCHEMA.keys())), validate=True)


def validate_patient(model_lf: pl.LazyFrame) -> Patient:
    """Validate patient model and populate validation_errors column."""
    validated_lf = _with_validation_errors(model_lf)
    return Patient.from_df(
        validated_lf.select(list(PATIENT_SCHEMA.keys())),
        validate=False,
    )


