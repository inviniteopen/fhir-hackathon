"""ETL reporting helpers for CLI output."""

from typing import Any

import polars as pl

from src.reporting.models_summaries import (
    get_condition_summary,
    get_observation_summary,
    get_patient_summary,
)
from src.reporting.validation_reports import get_validation_report


def print_bronze_summary(summary: dict[str, int]) -> None:
    """Print bronze table counts."""
    total_resources = sum(summary.values())

    print(f"Loaded {len(summary)} resource types ({total_resources} total resources)")
    print()
    print("Bronze tables:")
    for table_name, count in summary.items():
        print(f"  {table_name}: {count}")


def _print_quality(title: str, summary: dict[str, int], total_key: str) -> None:
    total = summary.get(total_key, 0)
    print()
    print(f"{title} data quality:")
    for field, count in summary.items():
        if field == total_key:
            continue
        pct = (count / total * 100) if total else 0
        print(f"  {field}: {count} ({pct:.0f}%)")


def _print_validation(title: str, report: dict[str, Any]) -> None:
    print()
    print(f"{title} validation results:")
    print(f"  Valid: {report['valid_records']}")
    print(f"  Invalid: {report['invalid_records']}")
    print(f"  Validity rate: {report['validity_rate']:.1%}")
    if report["errors_by_rule"]:
        print("  Errors by rule:")
        for err in report["errors_by_rule"]:
            print(f"    {err['error']}: {err['count']}")


def print_silver_summary(
    patient_lf: pl.LazyFrame,
    condition_lf: pl.LazyFrame,
    observation_lf: pl.LazyFrame,
) -> None:
    """Print silver layer counts, quality, and validation summaries."""
    patient_summary = get_patient_summary(patient_lf)
    condition_summary = get_condition_summary(condition_lf)
    observation_summary = get_observation_summary(observation_lf)

    patient_report = get_validation_report(patient_lf)
    condition_report = get_validation_report(condition_lf)
    observation_report = get_validation_report(observation_lf)

    print()
    print("Silver layer (in-memory):")
    print(f"  patient: {patient_summary['total_patients']}")
    print(f"  condition: {condition_summary['total_conditions']}")
    print(f"  observation: {observation_summary['total_observations']}")

    _print_quality("Patient", patient_summary, "total_patients")
    _print_validation("Patient", patient_report)

    _print_quality("Condition", condition_summary, "total_conditions")
    _print_validation("Condition", condition_report)

    _print_quality("Observation", observation_summary, "total_observations")
    _print_validation("Observation", observation_report)


def print_gold_summary(observations_per_patient_lf: pl.LazyFrame) -> None:
    """Print gold table counts."""
    observations_per_patient = (
        observations_per_patient_lf.select(pl.len().alias("total"))
        .collect()["total"][0]
    )
    print()
    print("Gold tables:")
    print(f"  observations_per_patient: {observations_per_patient}")
