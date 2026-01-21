"""Validation reporting helpers."""

from typing import Any

import polars as pl


def _as_lazyframe(validated_lf: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    if isinstance(validated_lf, pl.DataFrame):
        return validated_lf.lazy()
    return validated_lf


def get_validation_summary(validated_lf: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """Get summary of validation errors."""
    return (
        _as_lazyframe(validated_lf)
        .select(pl.col("validation_errors").list.explode().alias("error"))
        .filter(pl.col("error").is_not_null())
        .group_by("error")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .collect()
    )


def get_validation_report(validated_lf: pl.LazyFrame | pl.DataFrame) -> dict[str, Any]:
    """Generate a validation report."""
    if isinstance(validated_lf, pl.DataFrame):
        df = validated_lf
    else:
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
