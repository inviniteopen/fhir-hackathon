"""Sources Patient: Cleaned bronze with source metadata and flattened columns.

Sources flattens nested FHIR structures into a known column set:
- Source tracking (source_file, source_bundle)
- Demographics and identifiers
- Contact and address fields

From sources onward, Patient tables are flat. Models can rely on column names
without inspecting nested FHIR structures.
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

_SOURCES_SCHEMA = {
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
}


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


def _transform_row(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze patient row to a flat sources row."""
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
    }


def get_patient(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Get sources patient from bronze data.

    Returns a flat sources LazyFrame with stable, known columns.
    """
    rows = bronze_df.to_dicts()
    source_rows = [_transform_row(row) for row in rows]
    sources_df = pl.from_dicts(source_rows, schema=_SOURCES_SCHEMA)
    return sources_df.lazy()
