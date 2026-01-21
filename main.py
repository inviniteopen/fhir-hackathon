"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

import duckdb
import polars as pl

from src.bronze import get_table_summary, load_bundles_to_tables
from src.constants import Schema
from src.db.duckdb_io import connect_db, count_rows, write_lazyframe
from src.gold import build_observations_per_patient
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
    write_lazyframe(con, Schema.SILVER, "patient", patient_lf)
    write_lazyframe(con, Schema.SILVER, "condition", condition_lf)
    write_lazyframe(con, Schema.SILVER, "observation", observation_lf)


def build_silver_frame(
    con: duckdb.DuckDBPyConnection,
    table: str,
    source_fn: Callable[[pl.DataFrame], pl.LazyFrame],
    model_fn: Callable[[pl.LazyFrame], pl.LazyFrame],
) -> pl.LazyFrame:
    """Load bronze table and return its silver model LazyFrame."""
    bronze_df = con.execute(f"SELECT * FROM {Schema.BRONZE}.{table}").pl()
    return model_fn(source_fn(bronze_df))


def build_gold_frame(
    patient_lf: pl.LazyFrame,
    observation_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Build gold observations per patient LazyFrame."""
    return build_observations_per_patient(patient_lf, observation_lf)


def save_gold_tables(con: duckdb.DuckDBPyConnection, gold_lf: pl.LazyFrame) -> None:
    """Save gold tables to DuckDB."""
    write_lazyframe(con, Schema.GOLD, "observations_per_patient", gold_lf)


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
    con = connect_db(args.db)
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
    gold_observations_lf = build_gold_frame(
        patient_lf,
        observation_lf,
    )
    save_gold_tables(con, gold_observations_lf)

    # Print silver summary (computed from in-memory LazyFrames)
    print_silver_summary(patient_lf, condition_lf, observation_lf)

    observations_per_patient = count_rows(
        con,
        Schema.GOLD,
        "observations_per_patient",
    )
    print_gold_summary(Schema.GOLD, observations_per_patient)

    con.close()

    print()
    print(f"Database saved to: {args.db}")


if __name__ == "__main__":
    main()
