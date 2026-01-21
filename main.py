"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

import duckdb
import polars as pl

from src.bronze import get_table_summary, load_bundles_to_tables
from src.constants import Schema
from src.gold import create_observations_per_patient
from src.reporting.etl_reporting import (
    print_bronze_summary,
    print_gold_summary,
    print_silver_summary,
)
from src.silver.models.conditions import get_condition as get_condition_model
from src.silver.models.observations import get_observation as get_observation_model
from src.silver.models.patients import get_patient as get_patient_model
from src.silver.sources.conditions import get_condition as get_condition_source
from src.silver.sources.observations import get_observation as get_observation_source
from src.silver.sources.patients import get_patient as get_patient_source

DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "EPS"
DEFAULT_DB_PATH = Path(__file__).resolve().parent / "fhir.duckdb"


def save_silver_tables(
    con: duckdb.DuckDBPyConnection,
    patient_lf: pl.LazyFrame,
    condition_lf: pl.LazyFrame,
    observation_lf: pl.LazyFrame,
) -> None:
    """Save silver LazyFrames to DuckDB for debugging purposes."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.SILVER}")

    patient_df = patient_lf.collect()
    con.register("silver_patient_temp", patient_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {Schema.SILVER}.patient AS SELECT * FROM silver_patient_temp"
    )
    con.unregister("silver_patient_temp")

    condition_df = condition_lf.collect()
    con.register("silver_condition_temp", condition_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {Schema.SILVER}.condition AS SELECT * FROM silver_condition_temp"
    )
    con.unregister("silver_condition_temp")

    observation_df = observation_lf.collect()
    con.register("silver_observation_temp", observation_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {Schema.SILVER}.observation AS SELECT * FROM silver_observation_temp"
    )
    con.unregister("silver_observation_temp")


def build_silver_frame(
    con: duckdb.DuckDBPyConnection,
    table: str,
    source_fn: Callable[[pl.DataFrame], pl.LazyFrame],
    model_fn: Callable[[pl.LazyFrame], pl.LazyFrame],
) -> pl.LazyFrame:
    """Load bronze table and return its silver model LazyFrame."""
    bronze_df = con.execute(f"SELECT * FROM {Schema.BRONZE}.{table}").pl()
    return model_fn(source_fn(bronze_df))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load FHIR Bundle JSON files into DuckDB tables for analysis."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=DEFAULT_INPUT_DIR,
        type=Path,
        help=f"Directory containing FHIR Bundle JSON files. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to DuckDB database file. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Write intermediate silver tables to DB for debugging",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input directory does not exist: {args.input}")

    # Load bundles to bronze layer
    con = duckdb.connect(str(args.db))
    load_bundles_to_tables(args.input, con)

    # Print bronze summary
    summary = get_table_summary(con)
    print_bronze_summary(summary)

    # Transform bronze → sources → models (in-memory only by default)
    print()
    print("Transforming to silver layer...")

    # Patient transformation: bronze → sources → models
    patient_lf = build_silver_frame(
        con,
        "patient",
        get_patient_source,
        get_patient_model,
    )

    # Condition transformation: bronze → sources → models
    condition_lf = build_silver_frame(
        con,
        "condition",
        get_condition_source,
        get_condition_model,
    )

    # Observation transformation: bronze → sources → models
    observation_lf = build_silver_frame(
        con,
        "observation",
        get_observation_source,
        get_observation_model,
    )

    # Optionally save silver tables for debugging
    if args.debug:
        print("  (debug mode: writing silver tables to DB)")
        save_silver_tables(
            con,
            patient_lf,
            condition_lf,
            observation_lf,
        )

    # Build gold layer from silver LazyFrames

    print()
    print("Building gold layer...")
    create_observations_per_patient(
        con,
        patient_lf=patient_lf,
        observation_lf=observation_lf,
    )

    # Print silver summary (computed from in-memory LazyFrames)
    print_silver_summary(patient_lf, condition_lf, observation_lf)

    observations_per_patient = con.execute(
        f"SELECT COUNT(*) FROM {Schema.GOLD}.observations_per_patient"
    ).fetchone()[0]
    print_gold_summary(Schema.GOLD, observations_per_patient)

    con.close()

    print()
    print(f"Database saved to: {args.db}")


if __name__ == "__main__":
    main()
