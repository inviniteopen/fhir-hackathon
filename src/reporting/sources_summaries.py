"""Summary helpers for Silver sources dataframes."""

from __future__ import annotations

import polars as pl


def get_patient_summary(sources_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for sources patients."""
    return (
        sources_lf.select(
            pl.len().alias("total_patients"),
            pl.col("name").drop_nulls().len().alias("with_name"),
            pl.col("birthDate").drop_nulls().len().alias("with_birth_date"),
            pl.col("gender").drop_nulls().len().alias("with_gender"),
            pl.col("telecom").drop_nulls().len().alias("with_telecom"),
            pl.col("address").drop_nulls().len().alias("with_address"),
        )
        .collect()
        .to_dicts()[0]
    )


def get_condition_summary(sources_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for sources conditions."""
    return (
        sources_lf.select(
            pl.len().alias("total_conditions"),
            pl.col("subject").drop_nulls().len().alias("with_subject"),
            pl.col("code").drop_nulls().len().alias("with_code"),
            pl.col("category").drop_nulls().len().alias("with_category"),
            pl.col("onsetDateTime").drop_nulls().len().alias("with_onset"),
            pl.col("clinicalStatus").drop_nulls().len().alias("with_clinical_status"),
        )
        .collect()
        .to_dicts()[0]
    )


def get_observation_summary(sources_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for sources observations."""
    return (
        sources_lf.select(
            pl.len().alias("total_observations"),
            pl.col("status").drop_nulls().len().alias("with_status"),
            pl.col("subject").drop_nulls().len().alias("with_subject"),
            pl.col("code").drop_nulls().len().alias("with_code"),
            pl.col("effectiveDateTime").drop_nulls().len().alias("with_effective"),
            pl.col("valueQuantity").drop_nulls().len().alias("with_value_quantity"),
            pl.col("component").drop_nulls().len().alias("with_components"),
        )
        .collect()
        .to_dicts()[0]
    )
