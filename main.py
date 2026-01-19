"""Load FHIR Bundle JSON files into DuckDB tables for analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from src.fhir_loader import get_table_summary, load_bundles_to_tables

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

    # Load bundles
    con = duckdb.connect(str(args.db))
    load_bundles_to_tables(args.input, con)

    # Print summary
    summary = get_table_summary(con)
    total_resources = sum(summary.values())

    print(f"Loaded {len(summary)} resource types ({total_resources} total resources)")
    print()
    print("Tables:")
    for table_name, count in summary.items():
        print(f"  {table_name}: {count}")

    print()
    print(f"Database saved to: {args.db}")


if __name__ == "__main__":
    main()
