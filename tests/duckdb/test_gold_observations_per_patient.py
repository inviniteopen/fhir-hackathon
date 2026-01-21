from __future__ import annotations

from datetime import date

import duckdb
import polars as pl

from src.common.models import (
    OBSERVATION_SCHEMA,
    PATIENT_SCHEMA,
    Observation,
    Patient,
)
from src.constants import Schema
from src.db.duckdb_io import write_lazyframe
from src.gold import build_observations_per_patient


def test_build_observations_per_patient_counts_and_age() -> None:
    """Test that observations are counted per patient and age is calculated correctly."""
    # Create patient LazyFrame
    patient_data = [
        {"id": "p1", "birth_date": "2000-01-01"},
        {"id": "p2", "birth_date": None},
        {"id": "p3", "birth_date": "not-a-date"},
    ]
    # Add required fields with None values
    for row in patient_data:
        for key in PATIENT_SCHEMA:
            if key not in row:
                row[key] = None if key != "validation_errors" else []

    patient_lf = Patient.from_dicts(patient_data, PATIENT_SCHEMA)

    # Create observation LazyFrame
    observation_data = [
        {"id": "o1", "subject_id": "p1"},
        {"id": "o2", "subject_id": "p1"},
        {"id": "o3", "subject_id": "p1"},
        {"id": "o4", "subject_id": "p2"},
        {"id": "o5", "subject_id": "unknown"},  # No matching patient
        {"id": "o6", "subject_id": None},  # Null subject
    ]
    # Add required fields with None values
    for row in observation_data:
        for key in OBSERVATION_SCHEMA:
            if key not in row:
                if key == "validation_errors":
                    row[key] = []
                elif key == "component_count":
                    row[key] = 0
                elif key in (
                    "performer_references",
                    "performer_ids",
                    "code_codings",
                    "category_codings",
                    "components",
                ):
                    row[key] = []
                else:
                    row[key] = None

    observation_lf = pl.DataFrame(observation_data, schema=OBSERVATION_SCHEMA).lazy()
    observation_lf = Observation.from_df(observation_lf, validate=False)

    # Build gold aggregation
    gold_lf = build_observations_per_patient(
        patient_lf, observation_lf, as_of=date(2025, 1, 1)
    )

    # Collect and sort for comparison
    result = gold_lf.collect().sort("patient_id")

    assert result["patient_id"].to_list() == ["p1", "p2", "p3"]
    assert result["observation_count"].to_list() == [3, 1, 0]
    assert result["birth_date"].to_list() == [date(2000, 1, 1), None, None]
    assert result["patient_age_years"].to_list() == [25, None, None]


def test_save_observations_per_patient_to_db() -> None:
    """Test that gold LazyFrame is saved to DuckDB correctly."""
    con = duckdb.connect(":memory:")

    # Create a simple gold LazyFrame
    gold_df = pl.DataFrame(
        {
            "patient_id": ["p1", "p2"],
            "observation_count": [5, 3],
            "birth_date": [date(2000, 1, 1), None],
            "patient_age_years": [25, None],
        }
    )
    gold_lf = gold_df.lazy()

    # Save to DB
    write_lazyframe(con, Schema.GOLD, "observations_per_patient", gold_lf)

    # Verify
    rows = con.execute(
        """
        SELECT patient_id, observation_count, birth_date, patient_age_years
        FROM gold.observations_per_patient
        ORDER BY patient_id
        """
    ).fetchall()

    assert rows == [
        ("p1", 5, date(2000, 1, 1), 25),
        ("p2", 3, None, None),
    ]
