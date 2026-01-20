"""S2 Patient: Domain-modeled from FHIR source.

S2 transforms FHIR Patient toward a domain-specific analytical model:
- Flattens nested structures (name[] → family_name, given_names, etc.)
- Extracts primary values from complex types (telecom → phone)
- Normalizes addresses and identifiers
- Prepares data for domain-level analytics

This is source-specific modeling - it knows about FHIR structures but transforms
them toward common domain concepts. For unified multi-source models, see S3.
"""

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


def transform_patient_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze patient row to S2 domain model."""
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


def transform_patient(bronze_df: pl.DataFrame) -> Patient:
    """Transform bronze patient DataFrame to S2 domain model."""
    bronze_rows = bronze_df.to_dicts()
    silver_rows = [transform_patient_row(row) for row in bronze_rows]
    return Patient.from_dicts(silver_rows, PATIENT_SCHEMA)


def get_patient_summary(silver_lf: Patient | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for S2 patient data."""
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
