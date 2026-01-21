"""ETL orchestration for bronze, silver, and gold layers."""

from pathlib import Path
from typing import Callable

import duckdb
import polars as pl

from src.bronze import load_bronze_bundles
from src.common.models import Condition, Observation, Patient
from src.constants import Schema
from src.db.duckdb_io import drop_table_if_exists, get_table_summary, write_dataframes
from src.gold import build_observations_per_patient
from src.silver.models.conditions import get_condition as get_condition_model
from src.silver.models.observations import get_observation as get_observation_model
from src.silver.models.patients import get_patient as get_patient_model
from src.silver.sources.conditions import get_condition as get_condition_source
from src.silver.sources.observations import get_observation as get_observation_source
from src.silver.sources.patients import get_patient as get_patient_source


def run_bronze(bundle_dir: Path, con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Load bronze tables and return a summary."""
    frames = load_bronze_bundles(bundle_dir)
    write_dataframes(con, Schema.BRONZE, frames)
    for table in frames:
        drop_table_if_exists(con, "main", table)
    return get_table_summary(con, Schema.BRONZE)


def _build_silver_frame(
    con: duckdb.DuckDBPyConnection,
    table: str,
    source_fn: Callable[[pl.DataFrame], pl.LazyFrame],
    model_fn: Callable[[pl.LazyFrame], pl.LazyFrame],
) -> pl.LazyFrame:
    bronze_df = con.execute(f"SELECT * FROM {Schema.BRONZE}.{table}").pl()
    return model_fn(source_fn(bronze_df))


def run_silver(
    con: duckdb.DuckDBPyConnection,
) -> tuple[Patient, Condition, Observation]:
    """Build silver layer LazyFrames."""
    patient_lf = _build_silver_frame(
        con,
        "patient",
        get_patient_source,
        get_patient_model,
    )
    condition_lf = _build_silver_frame(
        con,
        "condition",
        get_condition_source,
        get_condition_model,
    )
    observation_lf = _build_silver_frame(
        con,
        "observation",
        get_observation_source,
        get_observation_model,
    )
    return patient_lf, condition_lf, observation_lf


def run_gold(patient_lf: Patient, observation_lf: Observation) -> pl.LazyFrame:
    """Build gold layer LazyFrames."""
    return build_observations_per_patient(patient_lf, observation_lf)
