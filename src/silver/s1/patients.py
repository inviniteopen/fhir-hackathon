"""S1 Patient: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (name, address, telecom, extension, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

from typing import Any

import polars as pl


def transform_patient_row(row: dict[str, Any]) -> dict[str, Any]:
    """Clean a bronze Patient row, preserving FHIR structure."""
    return {
        # Source tracking
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        # Core FHIR fields (structure preserved)
        "id": row.get("id"),
        "resourceType": row.get("resourceType"),
        "identifier": row.get("identifier"),  # List of Identifier
        "active": row.get("active"),
        "name": row.get("name"),  # List of HumanName
        "telecom": row.get("telecom"),  # List of ContactPoint
        "gender": row.get("gender"),
        "birthDate": row.get("birthDate"),
        "deceasedBoolean": row.get("deceasedBoolean"),
        "deceasedDateTime": row.get("deceasedDateTime"),
        "address": row.get("address"),  # List of Address
        "maritalStatus": row.get("maritalStatus"),  # CodeableConcept
        "multipleBirthBoolean": row.get("multipleBirthBoolean"),
        "multipleBirthInteger": row.get("multipleBirthInteger"),
        "photo": row.get("photo"),  # List of Attachment
        "contact": row.get("contact"),  # List of contact persons
        "communication": row.get("communication"),  # List of languages
        "generalPractitioner": row.get("generalPractitioner"),  # List of Reference
        "managingOrganization": row.get("managingOrganization"),  # Reference
        "link": row.get("link"),  # List of links to other patients
        "extension": row.get("extension"),  # List of Extension
    }


def transform_patient(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze patients to S1 (cleaned, same structure)."""
    silver_rows = [transform_patient_row(row) for row in bronze_df.to_dicts()]
    return pl.DataFrame(silver_rows).lazy()


def get_patient_summary(silver_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for S1 patients."""
    return (
        silver_lf.select(
            pl.len().alias("total_patients"),
            pl.col("name").drop_nulls().len().alias("with_name"),
            pl.col("birthDate").drop_nulls().len().alias("with_birth_date"),
            pl.col("gender").drop_nulls().len().alias("with_gender"),
            pl.col("telecom").drop_nulls().len().alias("with_telecom"),
            pl.col("address").drop_nulls().len().alias("with_address"),
        )
        .collect()
        .to_dicts()[0]
    )
