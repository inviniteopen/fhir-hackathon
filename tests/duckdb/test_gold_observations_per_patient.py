from __future__ import annotations

from datetime import date

import duckdb

from src.gold import create_observations_per_patient


def test_create_observations_per_patient_counts_and_age() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")
    con.execute(
        """
        CREATE TABLE silver.patient (
            id VARCHAR,
            birth_date VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE silver.observation (
            id VARCHAR,
            subject_id VARCHAR
        )
        """
    )

    con.execute(
        """
        INSERT INTO silver.patient (id, birth_date) VALUES
            ('p1', '2000-01-01'),
            ('p2', NULL),
            ('p3', 'not-a-date')
        """
    )
    con.execute(
        """
        INSERT INTO silver.observation (id, subject_id) VALUES
            ('o1', 'p1'),
            ('o2', 'p1'),
            ('o3', 'p1'),
            ('o4', 'p2'),
            ('o5', 'unknown'),
            ('o6', NULL)
        """
    )

    create_observations_per_patient(con, as_of=date(2025, 1, 1))

    rows = con.execute(
        """
        SELECT patient_id, observation_count, birth_date, patient_age_years
        FROM gold.observations_per_patient
        ORDER BY patient_id
        """
    ).fetchall()

    assert rows == [
        ("p1", 3, date(2000, 1, 1), 25),
        ("p2", 1, None, None),
        ("p3", 0, None, None),
    ]
