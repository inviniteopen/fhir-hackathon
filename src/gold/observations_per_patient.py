"""Gold layer aggregation: observations per patient using pure Polars."""

from __future__ import annotations

from datetime import date

import duckdb
import polars as pl

from src.common.models import Observation, Patient
from src.common.sql import quote_ident
from src.constants import GOLD_SCHEMA


def build_observations_per_patient(
    patient_lf: Patient,
    observation_lf: Observation,
    *,
    as_of: date | None = None,
) -> pl.LazyFrame:
    """
    Build observations per patient aggregation from silver LazyFrames.

    Aggregates observation counts per patient (identified by `subject_id`)
    and computes patient age (in years) from `birth_date` as of `as_of`
    (defaults to today's date).

    Returns a LazyFrame with columns:
    - patient_id: str
    - observation_count: int
    - birth_date: date (nullable)
    - patient_age_years: int (nullable)
    """
    as_of_date = as_of or date.today()

    # Join patients with observations (left join to keep patients with no observations)
    joined = patient_lf.join(
        observation_lf.select(
            Observation.id.alias("observation_id"), Observation.subject_id
        ),
        left_on=Patient.id,
        right_on=Observation.subject_id,
        how="left",
    )

    # Group by patient and count observations
    aggregated = joined.group_by(Patient.id, Patient.birth_date).agg(
        pl.col("observation_id").drop_nulls().count().alias("observation_count")
    )

    # Parse birth_date and calculate age
    result = (
        aggregated.with_columns(
            pl.col("birth_date")
            .str.to_date(format="%Y-%m-%d", strict=False)
            .alias("birth_date_parsed")
        )
        .with_columns(
            pl.when(pl.col("birth_date_parsed").is_not_null())
            .then(
                (pl.lit(as_of_date) - pl.col("birth_date_parsed"))
                .dt.total_days()
                .floordiv(365)
                .cast(pl.Int64)
            )
            .otherwise(None)
            .alias("patient_age_years")
        )
        .select(
            pl.col("id").alias("patient_id"),
            pl.col("observation_count"),
            pl.col("birth_date_parsed").alias("birth_date"),
            pl.col("patient_age_years"),
        )
    )

    return result


def save_observations_per_patient(
    con: duckdb.DuckDBPyConnection,
    gold_lf: pl.LazyFrame,
) -> None:
    """Save observations per patient LazyFrame to DuckDB gold schema."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(GOLD_SCHEMA)}")

    gold_df = gold_lf.collect()
    con.register("gold_observations_per_patient_temp", gold_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {quote_ident(GOLD_SCHEMA)}.observations_per_patient "
        "AS SELECT * FROM gold_observations_per_patient_temp"
    )
    con.unregister("gold_observations_per_patient_temp")


# Keep the old function signature for backwards compatibility during transition
def create_observations_per_patient(
    con: duckdb.DuckDBPyConnection,
    *,
    patient_lf: Patient | None = None,
    observation_lf: Observation | None = None,
    as_of: date | None = None,
) -> None:
    """
    Create `gold.observations_per_patient` from silver LazyFrames.

    This is a convenience function that builds and saves in one step.
    """
    if patient_lf is None or observation_lf is None:
        raise ValueError("patient_lf and observation_lf are required")

    gold_lf = build_observations_per_patient(patient_lf, observation_lf, as_of=as_of)
    save_observations_per_patient(con, gold_lf)
