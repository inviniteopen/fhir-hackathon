"""Transform bronze.patient to silver.patient with flattened fields using Polars."""

from pathlib import Path
from typing import Optional

import polars as pl

from das.engine.polars.typed_dataframe import Col, TypedLazyFrame
from src.constants import SILVER_SCHEMA


class SilverPatient(TypedLazyFrame):
    """Silver layer Patient schema - flattened and cleaned."""

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    family_name: Col[Optional[str]]
    given_names: Col[Optional[str]]
    full_name: Col[Optional[str]]
    birth_date: Col[Optional[str]]
    gender: Col[Optional[str]]
    phone: Col[Optional[str]]
    address_line: Col[Optional[str]]
    city: Col[Optional[str]]
    postal_code: Col[Optional[str]]
    country: Col[Optional[str]]
    nationality_code: Col[Optional[str]]
    identifier_eci: Col[Optional[str]]
    identifier_mr: Col[Optional[str]]
    validation_errors: Col[list[str]]


def _extract_from_name_list(name_list: list[dict] | None, field: str) -> str | None:
    """Extract a field from the FHIR name array."""
    if not name_list:
        return None
    for name_obj in name_list:
        if isinstance(name_obj, dict):
            value = name_obj.get(field)
            if value is not None:
                if isinstance(value, list):
                    return " ".join(str(v) for v in value)
                return str(value) if value else None
    return None


def _extract_phone(telecom_list: list[dict] | None) -> str | None:
    """Extract phone number from FHIR telecom array."""
    if not telecom_list:
        return None
    for telecom in telecom_list:
        if isinstance(telecom, dict) and telecom.get("system") == "phone":
            return telecom.get("value")
    return None


def _extract_address_field(address_list: list[dict] | None, field: str) -> str | None:
    """Extract a field from the first FHIR address."""
    if not address_list or not isinstance(address_list, list):
        return None
    for addr in address_list:
        if isinstance(addr, dict):
            value = addr.get(field)
            if value:
                if isinstance(value, list):
                    return ", ".join(str(v) for v in value)
                return str(value)
    return None


def _extract_nationality(extension_list: list[dict] | None) -> str | None:
    """Extract nationality code from FHIR extension array."""
    if not extension_list:
        return None
    for ext in extension_list:
        if isinstance(ext, dict):
            if (
                ext.get("url")
                == "http://hl7.org/fhir/StructureDefinition/patient-nationality"
            ):
                value_cc = ext.get("valueCodeableConcept", {})
                codings = value_cc.get("coding", [])
                if codings and isinstance(codings, list) and len(codings) > 0:
                    return codings[0].get("code")
    return None


def _extract_identifier(identifier_list: list[dict] | None, system: str) -> str | None:
    """Extract identifier value by system from FHIR identifier array."""
    if not identifier_list:
        return None
    for ident in identifier_list:
        if isinstance(ident, dict) and ident.get("system") == system:
            return ident.get("value")
    return None


def transform_patient_row(row: dict) -> dict:
    """Transform a single bronze patient row to silver format."""
    name_list = row.get("name")
    telecom_list = row.get("telecom")
    address_list = row.get("address")
    extension_list = row.get("extension")
    identifier_list = row.get("identifier")

    return {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "family_name": _extract_from_name_list(name_list, "family"),
        "given_names": _extract_from_name_list(name_list, "given"),
        "full_name": _extract_from_name_list(name_list, "text"),
        "birth_date": row.get("birthDate"),
        "gender": row.get("gender"),
        "phone": _extract_phone(telecom_list),
        "address_line": _extract_address_field(address_list, "line"),
        "city": _extract_address_field(address_list, "city"),
        "postal_code": _extract_address_field(address_list, "postalCode"),
        "country": _extract_address_field(address_list, "country"),
        "nationality_code": _extract_nationality(extension_list),
        "identifier_eci": _extract_identifier(
            identifier_list, "http://ec.europa.eu/identifier/eci"
        ),
        "identifier_mr": _extract_identifier(
            identifier_list, "http://local.setting.eu/identifier"
        ),
        "validation_errors": [],
    }


def load_bronze_patient(db_path: Path) -> pl.LazyFrame:
    """
    Load bronze.patient table from DuckDB as Polars LazyFrame.

    Args:
        db_path: Path to DuckDB database file

    Returns:
        Polars LazyFrame with bronze patient data
    """
    return pl.read_database_uri(
        query="SELECT * FROM bronze.patient",
        uri=f"duckdb://{db_path}",
    ).lazy()


def transform_patient(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """
    Transform bronze patient DataFrame to silver format.

    Args:
        bronze_df: Polars DataFrame with bronze patient data

    Returns:
        Polars LazyFrame with silver patient data
    """
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_patient_row(row) for row in bronze_rows]

    schema = {
        "id": pl.String,
        "source_file": pl.String,
        "source_bundle": pl.String,
        "family_name": pl.String,
        "given_names": pl.String,
        "full_name": pl.String,
        "birth_date": pl.String,
        "gender": pl.String,
        "phone": pl.String,
        "address_line": pl.String,
        "city": pl.String,
        "postal_code": pl.String,
        "country": pl.String,
        "nationality_code": pl.String,
        "identifier_eci": pl.String,
        "identifier_mr": pl.String,
        "validation_errors": pl.List(pl.String),
    }

    return pl.DataFrame(silver_rows, schema=schema).lazy()


def save_silver_patient(silver_lf: pl.LazyFrame, db_path: Path) -> int:
    """
    Save silver patient LazyFrame to DuckDB.

    Args:
        silver_lf: Polars LazyFrame with silver patient data
        db_path: Path to DuckDB database file

    Returns:
        Number of rows saved
    """
    import duckdb

    silver_df = silver_lf.collect()

    con = duckdb.connect(str(db_path))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    con.register("silver_patient_temp", silver_df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TABLE {SILVER_SCHEMA}.patient AS SELECT * FROM silver_patient_temp"
    )
    con.unregister("silver_patient_temp")
    row_count = con.execute(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.patient").fetchone()[
        0
    ]
    con.close()

    return row_count


def get_patient_summary(silver_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for silver patient data."""
    return (
        silver_lf.select(
            pl.len().alias("total_patients"),
            pl.col("family_name").drop_nulls().len().alias("with_family_name"),
            pl.col("given_names").drop_nulls().len().alias("with_given_names"),
            pl.col("birth_date").drop_nulls().len().alias("with_birth_date"),
            pl.col("gender").drop_nulls().len().alias("with_gender"),
            pl.col("phone").drop_nulls().len().alias("with_phone"),
            pl.col("city").drop_nulls().len().alias("with_city"),
            pl.col("nationality_code").drop_nulls().len().alias("with_nationality"),
        )
        .collect()
        .to_dicts()[0]
    )
