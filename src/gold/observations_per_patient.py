"""Gold layer aggregation: observations per patient."""

from __future__ import annotations

from datetime import date

import duckdb

from src.constants import GOLD_SCHEMA, SILVER_SCHEMA


def _quote_ident(ident: str) -> str:
    return f'"{ident.replace(chr(34), chr(34) * 2)}"'


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _create_gold_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(GOLD_SCHEMA)}")


def create_observations_per_patient(
    con: duckdb.DuckDBPyConnection, *, as_of: date | None = None
) -> None:
    """
    Create `gold.observations_per_patient` from silver tables.

    Aggregates observations counts per patient (identified by `silver.observation.subject_id`)
    and computes patient age (in years) from `silver.patient.birth_date` as of `as_of`
    (defaults to DuckDB's CURRENT_DATE).
    """
    _create_gold_schema(con)

    as_of_sql = "CURRENT_DATE" if as_of is None else f"DATE '{as_of.isoformat()}'"
    gold_table = _qualified_table(GOLD_SCHEMA, "observations_per_patient")
    silver_patient = _qualified_table(SILVER_SCHEMA, "patient")
    silver_observation = _qualified_table(SILVER_SCHEMA, "observation")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT
            p.id AS patient_id,
            COUNT(o.id) AS observation_count,
            TRY_CAST(p.birth_date AS DATE) AS birth_date,
            CASE
                WHEN TRY_CAST(p.birth_date AS DATE) IS NULL THEN NULL
                ELSE DATE_DIFF('year', TRY_CAST(p.birth_date AS DATE), {as_of_sql})
            END AS patient_age_years
        FROM {silver_patient} AS p
        LEFT JOIN {silver_observation} AS o
            ON o.subject_id = p.id
        GROUP BY p.id, p.birth_date
        """
    )
