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

from src.common.constants import ExtensionUrl, IdentifierSystem
from src.common.fhir import (
    extract_address_field,
    extract_extension_value,
    extract_from_name_list,
    extract_identifier,
    extract_telecom,
)
from src.common.models import PATIENT_SCHEMA, Patient

# =============================================================================
# Column extraction functions - focused functions for extracting specific fields
# =============================================================================


def extract_family_name(name_list: list[dict] | None) -> str | None:
    """Extract family name from FHIR name array."""
    return extract_from_name_list(name_list, "family")


def extract_given_names(name_list: list[dict] | None) -> str | None:
    """Extract given names from FHIR name array."""
    return extract_from_name_list(name_list, "given")


def extract_full_name(name_list: list[dict] | None) -> str | None:
    """Extract full name text from FHIR name array."""
    return extract_from_name_list(name_list, "text")


def extract_phone(telecom_list: list[dict] | None) -> str | None:
    """Extract phone number from FHIR telecom array."""
    return extract_telecom(telecom_list, "phone")


def extract_city(address_list: list[dict] | None) -> str | None:
    """Extract city from FHIR address array."""
    return extract_address_field(address_list, "city")


def extract_postal_code(address_list: list[dict] | None) -> str | None:
    """Extract postal code from FHIR address array."""
    return extract_address_field(address_list, "postalCode")


def extract_country(address_list: list[dict] | None) -> str | None:
    """Extract country from FHIR address array."""
    return extract_address_field(address_list, "country")


def extract_address_line(address_list: list[dict] | None) -> str | None:
    """Extract address line from FHIR address array."""
    return extract_address_field(address_list, "line")


def extract_nationality_code(extension_list: list[dict] | None) -> str | None:
    """Extract nationality code from patient nationality extension."""
    nationality_code = extract_extension_value(
        extension_list,
        ExtensionUrl.NATIONALITY,
        ["valueCodeableConcept", "coding", 0, "code"],
    )
    if nationality_code is not None:
        return nationality_code

    # Fallback for list access in extension
    for ext in extension_list or []:
        if isinstance(ext, dict) and ext.get("url") == ExtensionUrl.NATIONALITY:
            value_cc = ext.get("valueCodeableConcept", {})
            codings = value_cc.get("coding", [])
            if codings and isinstance(codings, list) and len(codings) > 0:
                return codings[0].get("code")
    return None


def extract_identifier_eci(identifier_list: list[dict] | None) -> str | None:
    """Extract ECI identifier from FHIR identifier array."""
    return extract_identifier(identifier_list, IdentifierSystem.ECI)


def extract_identifier_mr(identifier_list: list[dict] | None) -> str | None:
    """Extract MR identifier from FHIR identifier array."""
    return extract_identifier(identifier_list, IdentifierSystem.MR)


# =============================================================================
# Row transformation function (internal)
# =============================================================================


def _transform_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze patient row to S2 domain model."""
    name_list = row.get("name")
    telecom_list = row.get("telecom")
    address_list = row.get("address")
    extension_list = row.get("extension")
    identifier_list = row.get("identifier")

    return {
        "id": row.get("id"),
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        "family_name": extract_family_name(name_list),
        "given_names": extract_given_names(name_list),
        "full_name": extract_full_name(name_list),
        "birth_date": row.get("birthDate"),
        "gender": row.get("gender"),
        "phone": extract_phone(telecom_list),
        "address_line": extract_address_line(address_list),
        "city": extract_city(address_list),
        "postal_code": extract_postal_code(address_list),
        "country": extract_country(address_list),
        "nationality_code": extract_nationality_code(extension_list),
        "identifier_eci": extract_identifier_eci(identifier_list),
        "identifier_mr": extract_identifier_mr(identifier_list),
        "validation_errors": [],
    }


# =============================================================================
# Public API: get, transform
# =============================================================================


def get_patient(source_df: pl.DataFrame) -> Patient:
    """Get S2 patient by transforming source data.

    Args:
        source_df: Bronze Patient DataFrame

    Returns:
        Typed Patient LazyFrame with S2 domain model
    """
    return transform(source_df)


def transform(source_df: pl.DataFrame) -> Patient:
    """Transform source DataFrame to S2 patient model.

    Args:
        source_df: Bronze Patient DataFrame

    Returns:
        Typed Patient LazyFrame with S2 domain model
    """
    bronze_rows = source_df.to_dicts()
    silver_rows = [_transform_row(row) for row in bronze_rows]
    return Patient.from_dicts(silver_rows, PATIENT_SCHEMA)
