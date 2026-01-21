"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import polars as pl

from src.bronze import get_table_summary, load_bundles_to_tables
from src.constants import Schema
from src.gold import create_observations_per_patient
from src.reporting.models_summaries import (
    get_condition_summary,
    get_observation_summary,
    get_patient_summary,
)
from src.silver.models.conditions import get_condition as get_condition_model
from src.silver.models.observations import get_observation as get_observation_model
from src.silver.models.patients import get_patient as get_patient_model
from src.silver.sources.conditions import get_condition as get_condition_source
from src.silver.sources.observations import get_observation as get_observation_source
from src.silver.sources.patients import get_patient as get_patient_source
from src.validations.conditions import (
    get_validation_report as get_condition_validation_report,
)
from src.validations.conditions import (
    validate_condition,
)
from src.validations.observations import (
    get_validation_report as get_observation_validation_report,
)
from src.validations.observations import (
    validate_observation,
)
from src.validations.patients import get_validation_report, validate_patient

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
    total_resources = sum(summary.values())

    print(f"Loaded {len(summary)} resource types ({total_resources} total resources)")
    print()
    print("Bronze tables:")
    for table_name, count in summary.items():
        print(f"  {table_name}: {count}")

    # Transform bronze → sources → models (in-memory only by default)
    print()
    print("Transforming to silver layer...")

    # Patient transformation: bronze → sources → models
    bronze_patient_df = con.execute(f"SELECT * FROM {Schema.BRONZE}.patient").pl()
    sources_patient_lf = get_patient_source(bronze_patient_df)
    models_patient_lf = get_patient_model(sources_patient_lf)
    validated_patient_lf = validate_patient(models_patient_lf)

    # Condition transformation: bronze → sources → models
    bronze_condition_df = con.execute(f"SELECT * FROM {Schema.BRONZE}.condition").pl()
    sources_condition_lf = get_condition_source(bronze_condition_df)
    models_condition_lf = get_condition_model(sources_condition_lf)
    validated_condition_lf = validate_condition(models_condition_lf)

    # Observation transformation: bronze → sources → models
    bronze_observation_df = con.execute(
        f"SELECT * FROM {Schema.BRONZE}.observation"
    ).pl()
    sources_observation_lf = get_observation_source(bronze_observation_df)
    models_observation_lf = get_observation_model(sources_observation_lf)
    validated_observation_lf = validate_observation(models_observation_lf)

    # Optionally save silver tables for debugging
    if args.debug:
        print("  (debug mode: writing silver tables to DB)")
        save_silver_tables(
            con,
            validated_patient_lf,
            validated_condition_lf,
            validated_observation_lf,
        )

    # Build gold layer from silver LazyFrames

    print()
    print("Building gold layer...")
    create_observations_per_patient(
        con,
        patient_lf=validated_patient_lf,
        observation_lf=validated_observation_lf,
    )

    # Print silver summary (computed from in-memory LazyFrames)
    patient_summary = get_patient_summary(validated_patient_lf)
    validation_report = get_validation_report(validated_patient_lf)
    condition_summary = get_condition_summary(validated_condition_lf)
    condition_validation_report = get_condition_validation_report(
        validated_condition_lf
    )
    observation_summary = get_observation_summary(validated_observation_lf)
    observation_validation_report = get_observation_validation_report(
        validated_observation_lf
    )

    print()
    print("Silver layer (in-memory):")
    print(f"  patient: {patient_summary['total_patients']}")
    print(f"  condition: {condition_summary['total_conditions']}")
    print(f"  observation: {observation_summary['total_observations']}")

    gold_patients = con.execute(
        f"SELECT COUNT(*) FROM {Schema.GOLD}.observations_per_patient"
    ).fetchone()[0]
    print()
    print("Gold tables:")
    print(f"  {Schema.GOLD}.observations_per_patient: {gold_patients}")

    print()
    print("Patient data quality:")
    for field, count in patient_summary.items():
        if field != "total_patients":
            pct = count / patient_summary["total_patients"] * 100
            print(f"  {field}: {count} ({pct:.0f}%)")

    print()
    print("Patient validation results:")
    print(f"  Valid: {validation_report['valid_records']}")
    print(f"  Invalid: {validation_report['invalid_records']}")
    print(f"  Validity rate: {validation_report['validity_rate']:.1%}")
    if validation_report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in validation_report["errors_by_rule"]:
            print(f"    {err['error']}: {err['count']}")

    print()
    print("Condition data quality:")
    for field, count in condition_summary.items():
        if field != "total_conditions":
            pct = count / condition_summary["total_conditions"] * 100
            print(f"  {field}: {count} ({pct:.0f}%)")

    print()
    print("Condition validation results:")
    print(f"  Valid: {condition_validation_report['valid_records']}")
    print(f"  Invalid: {condition_validation_report['invalid_records']}")
    print(f"  Validity rate: {condition_validation_report['validity_rate']:.1%}")
    if condition_validation_report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in condition_validation_report["errors_by_rule"]:
            print(f"    {err['error']}: {err['count']}")

    print()
    print("Observation data quality:")
    for field, count in observation_summary.items():
        if field != "total_observations":
            pct = count / observation_summary["total_observations"] * 100
            print(f"  {field}: {count} ({pct:.0f}%)")

    print()
    print("Observation validation results:")
    print(f"  Valid: {observation_validation_report['valid_records']}")
    print(f"  Invalid: {observation_validation_report['invalid_records']}")
    print(f"  Validity rate: {observation_validation_report['validity_rate']:.1%}")
    if observation_validation_report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in observation_validation_report["errors_by_rule"]:
            print(f"    {err['error']}: {err['count']}")

    con.close()

    print()
    print(f"Database saved to: {args.db}")


if __name__ == "__main__":
    main()
