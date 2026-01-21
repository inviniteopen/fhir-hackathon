"""Condition model: Domain-modeled from sources.

Sources now provides flat, known columns for conditions. Models simply add
model-level fields (like validation errors) and ensure the typed schema.
"""

import polars as pl

from src.common.models import CONDITION_SCHEMA, Condition


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
    model_lf = sources_lf.with_columns(
        pl.lit([]).cast(pl.List(pl.String)).alias("validation_errors")
    )
    return Condition.from_df(
        model_lf.select(list(CONDITION_SCHEMA.keys())),
        validate=True,
    )
