"""Patient model: Domain-modeled from sources.

Sources now provides flat, known columns for patients. Models simply add
model-level fields (like validation errors) and ensure the typed schema.
"""

import polars as pl

from src.common.models import PATIENT_SCHEMA, Patient


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
    model_lf = sources_lf.with_columns(
        pl.lit([]).cast(pl.List(pl.String)).alias("validation_errors")
    )
    return Patient.from_df(
        model_lf.select(list(PATIENT_SCHEMA.keys())),
        validate=True,
    )
