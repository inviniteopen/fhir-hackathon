"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src.constants import BRONZE_SCHEMA, SILVER_SCHEMA
from src.fhir_loader import get_table_summary, load_bundles_to_tables
from src.transformations.patients import (
    get_patient_summary,
    transform_patient,
)
from src.transformations.observations import (
    get_observation_summary,
    transform_observations,
)
from src.validations.observations import (
    get_validation_report as get_observation_validation_report,
    validate_observation,
)
from src.validations.patients import get_validation_report, validate_patient

DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "EPS"
DEFAULT_DB_PATH = Path(__file__).resolve().parent / "fhir.duckdb"


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

    # Transform bronze to silver
    print()
    print("Transforming to silver layer...")

    # Patient transformation
    bronze_patient_df = con.execute(f"SELECT * FROM {BRONZE_SCHEMA}.patient").pl()
    silver_patient_lf = transform_patient(bronze_patient_df)
    validated_patient_lf = validate_patient(silver_patient_lf)

    # Observation transformation (silver.* tables)
    bronze_observation_df = con.execute(
        f"SELECT * FROM {BRONZE_SCHEMA}.observation"
    ).pl()
    silver_observations_lf = transform_observations(bronze_observation_df)
    validated_observation_lf = validate_observation(silver_observations_lf)

    # Save to silver schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

    validated_patient_df = validated_patient_lf.collect()
    con.register("silver_patient_temp", validated_patient_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {SILVER_SCHEMA}.patient AS SELECT * FROM silver_patient_temp"
    )
    con.unregister("silver_patient_temp")

    validated_observation_df = validated_observation_lf.collect()
    con.register("silver_observation_temp", validated_observation_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {SILVER_SCHEMA}.observations AS SELECT * FROM silver_observation_temp"
    )
    con.unregister("silver_observation_temp")

    # Print silver summary
    patient_summary = get_patient_summary(validated_patient_lf)
    validation_report = get_validation_report(validated_patient_lf)
    observation_summary = get_observation_summary(validated_observation_lf)
    observation_validation_report = get_observation_validation_report(
        validated_observation_lf
    )

    print()
    print("Silver tables:")
    print(f"  {SILVER_SCHEMA}.patient: {patient_summary['total_patients']}")
    print(
        f"  {SILVER_SCHEMA}.observations: {observation_summary['total_observations']}"
    )

    print()
    print("Patient data quality:")
    for field, count in patient_summary.items():
        if field != "total_patients":
            pct = count / patient_summary["total_patients"] * 100
            print(f"  {field}: {count} ({pct:.0f}%)")

    print()
    print("Validation results:")
    print(f"  Valid: {validation_report['valid_records']}")
    print(f"  Invalid: {validation_report['invalid_records']}")
    print(f"  Validity rate: {validation_report['validity_rate']:.1%}")
    if validation_report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in validation_report["errors_by_rule"]:
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
    print(
        f"  Validity rate: {observation_validation_report['validity_rate']:.1%}"
    )
    if observation_validation_report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in observation_validation_report["errors_by_rule"]:
            print(f"    {err['error']}: {err['count']}")

    con.close()

    print()
    print(f"Database saved to: {args.db}")


if __name__ == "__main__":
    main()
