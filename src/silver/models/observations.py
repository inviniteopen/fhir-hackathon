"""Observation model: Domain-modeled from sources.

Sources now provides flat, known columns for observations. Models simply add
model-level fields (like validation errors) and ensure the typed schema.
"""

import polars as pl

from src.common.models import OBSERVATION_SCHEMA, Observation


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
    model_lf = sources_lf.with_columns(
        pl.lit([]).cast(pl.List(pl.String)).alias("validation_errors")
    )
    return Observation.from_df(
        model_lf.select(list(OBSERVATION_SCHEMA.keys())),
        validate=True,
    )
