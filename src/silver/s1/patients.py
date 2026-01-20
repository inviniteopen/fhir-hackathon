"""S1 Patient: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (name, address, telecom, extension, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

import polars as pl


def transform_patient(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze patients to S1 (cleaned, same structure)."""
    return bronze_df.lazy().select(
        # Source tracking
        pl.col("_source_file").alias("source_file"),
        pl.col("_source_bundle").alias("source_bundle"),
        # Core FHIR fields (structure preserved)
        pl.col(
            "id",
            "resourceType",
            "identifier",  # List of Identifier
            "active",
            "name",  # List of HumanName
            "telecom",  # List of ContactPoint
            "gender",
            "birthDate",
            "deceasedBoolean",
            "deceasedDateTime",
            "address",  # List of Address
            "maritalStatus",  # CodeableConcept
            "multipleBirthBoolean",
            "multipleBirthInteger",
            "photo",  # List of Attachment
            "contact",  # List of contact persons
            "communication",  # List of languages
            "generalPractitioner",  # List of Reference
            "managingOrganization",  # Reference
            "link",  # List of links to other patients
            "extension",  # List of Extension
        ),
    )


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
