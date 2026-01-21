"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import polars as pl

from src.db.duckdb_io import connect_db, count_rows, write_lazyframe
from src.etl.pipeline import run_bronze, run_gold, run_silver
from src.constants import Schema
from src.reporting.etl_reporting import (
    print_bronze_summary,
    print_gold_summary,
    print_silver_summary,
)

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
    summary = run_bronze(args.input, con)
    print_bronze_summary(summary)

    # Transform bronze → sources → models (in-memory only by default)
    print()
    print("Transforming to silver layer...")

    patient_lf, condition_lf, observation_lf = run_silver(con)

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
    gold_observations_lf = run_gold(patient_lf, observation_lf)
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
