"""Transform bronze.patient to silver.patient with flattened fields using Polars."""

from pathlib import Path
from typing import Any

import polars as pl

from src.common.fhir import (
    extract_address_field,
    extract_extension_value,
    extract_from_name_list,
    extract_identifier,
    extract_telecom,
)
from src.common.models import PATIENT_SCHEMA, Patient
from src.constants import SILVER_SCHEMA


def transform_patient_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze patient row to silver format."""
    name_list = row.get("name")
    telecom_list = row.get("telecom")
    address_list = row.get("address")
    extension_list = row.get("extension")
    identifier_list = row.get("identifier")

    # Extract nationality from extension
    nationality_code = extract_extension_value(
        extension_list,
        "http://hl7.org/fhir/StructureDefinition/patient-nationality",
        ["valueCodeableConcept", "coding", 0, "code"],
    )
    # Fallback for list access in extension
    if nationality_code is None:
        for ext in extension_list or []:
            if (
                isinstance(ext, dict)
                and ext.get("url")
                == "http://hl7.org/fhir/StructureDefinition/patient-nationality"
            ):
                value_cc = ext.get("valueCodeableConcept", {})
                codings = value_cc.get("coding", [])
                if codings and isinstance(codings, list) and len(codings) > 0:
                    nationality_code = codings[0].get("code")
                    break

    return {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "family_name": extract_from_name_list(name_list, "family"),
        "given_names": extract_from_name_list(name_list, "given"),
        "full_name": extract_from_name_list(name_list, "text"),
        "birth_date": row.get("birthDate"),
        "gender": row.get("gender"),
        "phone": extract_telecom(telecom_list, "phone"),
        "address_line": extract_address_field(address_list, "line"),
        "city": extract_address_field(address_list, "city"),
        "postal_code": extract_address_field(address_list, "postalCode"),
        "country": extract_address_field(address_list, "country"),
        "nationality_code": nationality_code,
        "identifier_eci": extract_identifier(
            identifier_list, "http://ec.europa.eu/identifier/eci"
        ),
        "identifier_mr": extract_identifier(
            identifier_list, "http://local.setting.eu/identifier"
        ),
        "validation_errors": [],
    }


def load_bronze_patient(db_path: Path) -> pl.LazyFrame:
    """Load bronze.patient table from DuckDB as Polars LazyFrame."""
    return pl.read_database_uri(
        query="SELECT * FROM bronze.patient",
        uri=f"duckdb://{db_path}",
    ).lazy()


def transform_patient(bronze_df: pl.DataFrame) -> Patient:
    """Transform bronze patient DataFrame to silver format."""
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_patient_row(row) for row in bronze_rows]
    return Patient.from_dicts(silver_rows, PATIENT_SCHEMA)


def save_silver_patient(silver_lf: pl.LazyFrame, db_path: Path) -> int:
    """Save silver patient LazyFrame to DuckDB."""
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


def get_patient_summary(silver_lf: Patient | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for silver patient data."""
    return (
        silver_lf.select(
            pl.len().alias("total_patients"),
            Patient.family_name.drop_nulls().len().alias("with_family_name"),
            Patient.given_names.drop_nulls().len().alias("with_given_names"),
            Patient.birth_date.drop_nulls().len().alias("with_birth_date"),
            Patient.gender.drop_nulls().len().alias("with_gender"),
            Patient.phone.drop_nulls().len().alias("with_phone"),
            Patient.city.drop_nulls().len().alias("with_city"),
            Patient.nationality_code.drop_nulls().len().alias("with_nationality"),
        )
        .collect()
        .to_dicts()[0]
    )
