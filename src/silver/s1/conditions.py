"""S1 Condition: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (code, subject, category, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

from typing import Any

import polars as pl


def transform_condition_row(row: dict[str, Any]) -> dict[str, Any]:
    """Clean a bronze Condition row, preserving FHIR structure."""
    return {
        # Source tracking
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        # Core FHIR fields (structure preserved)
        "id": row.get("id"),
        "resourceType": row.get("resourceType"),
        "identifier": row.get("identifier"),  # List of Identifier
        "clinicalStatus": row.get("clinicalStatus"),  # CodeableConcept
        "verificationStatus": row.get("verificationStatus"),  # CodeableConcept
        "category": row.get("category"),  # List of CodeableConcept
        "severity": row.get("severity"),  # CodeableConcept
        "code": row.get("code"),  # CodeableConcept
        "bodySite": row.get("bodySite"),  # List of CodeableConcept
        "subject": row.get("subject"),  # Reference(Patient)
        "encounter": row.get("encounter"),  # Reference(Encounter)
        # onset[x]
        "onsetDateTime": row.get("onsetDateTime"),
        "onsetAge": row.get("onsetAge"),
        "onsetPeriod": row.get("onsetPeriod"),
        "onsetRange": row.get("onsetRange"),
        "onsetString": row.get("onsetString"),
        # abatement[x]
        "abatementDateTime": row.get("abatementDateTime"),
        "abatementAge": row.get("abatementAge"),
        "abatementPeriod": row.get("abatementPeriod"),
        "abatementRange": row.get("abatementRange"),
        "abatementString": row.get("abatementString"),
        "recordedDate": row.get("recordedDate"),
        "recorder": row.get("recorder"),  # Reference
        "asserter": row.get("asserter"),  # Reference
        "stage": row.get("stage"),  # List of stage assessments
        "evidence": row.get("evidence"),  # List of evidence
        "note": row.get("note"),  # List of Annotation
    }


def transform_condition(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze conditions to S1 (cleaned, same structure)."""
    silver_rows = [transform_condition_row(row) for row in bronze_df.to_dicts()]
    return pl.DataFrame(silver_rows).lazy()


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
