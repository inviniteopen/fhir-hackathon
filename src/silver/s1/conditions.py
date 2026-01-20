"""S1 Condition: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (code, subject, category, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

import polars as pl


def transform_condition(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze conditions to S1 (cleaned, same structure)."""
    return bronze_df.lazy().select(
        # Source tracking
        pl.col("_source_file").alias("source_file"),
        pl.col("_source_bundle").alias("source_bundle"),
        # Core FHIR fields (structure preserved)
        pl.col(
            "id",
            "resourceType",
            "identifier",  # List of Identifier
            "clinicalStatus",  # CodeableConcept
            "verificationStatus",  # CodeableConcept
            "category",  # List of CodeableConcept
            "severity",  # CodeableConcept
            "code",  # CodeableConcept
            "bodySite",  # List of CodeableConcept
            "subject",  # Reference(Patient)
            "encounter",  # Reference(Encounter)
            # onset[x]
            "onsetDateTime",
            "onsetAge",
            "onsetPeriod",
            "onsetRange",
            "onsetString",
            # abatement[x]
            "abatementDateTime",
            "abatementAge",
            "abatementPeriod",
            "abatementRange",
            "abatementString",
            "recordedDate",
            "recorder",  # Reference
            "asserter",  # Reference
            "stage",  # List of stage assessments
            "evidence",  # List of evidence
            "note",  # List of Annotation
        ),
    )


def get_condition_summary(silver_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for S1 conditions."""
    return (
        silver_lf.select(
            pl.len().alias("total_conditions"),
            pl.col("subject").drop_nulls().len().alias("with_subject"),
            pl.col("code").drop_nulls().len().alias("with_code"),
            pl.col("category").drop_nulls().len().alias("with_category"),
            pl.col("onsetDateTime").drop_nulls().len().alias("with_onset"),
            pl.col("clinicalStatus").drop_nulls().len().alias("with_clinical_status"),
        )
        .collect()
        .to_dicts()[0]
    )
