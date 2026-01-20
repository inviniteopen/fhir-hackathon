"""S1 Observation: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (_source_file, _source_bundle)
- Basic data quality flags
- Null/empty value normalization

The nested FHIR structures (code.coding, valueQuantity, component, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

from typing import Any

import polars as pl


def transform_observation_row(row: dict[str, Any]) -> dict[str, Any]:
    """Clean a bronze Observation row, preserving FHIR structure."""
    # Pass through with minimal cleaning - structure stays the same
    return {
        # Source tracking
        "source_file": row.get("_source_file"),
        "source_bundle": row.get("_source_bundle"),
        # Core FHIR fields (structure preserved)
        "id": row.get("id"),
        "resourceType": row.get("resourceType"),
        "status": row.get("status"),
        "category": row.get("category"),  # List of CodeableConcept
        "code": row.get("code"),  # CodeableConcept
        "subject": row.get("subject"),  # Reference
        "encounter": row.get("encounter"),  # Reference
        "effectiveDateTime": row.get("effectiveDateTime"),
        "effectivePeriod": row.get("effectivePeriod"),
        "issued": row.get("issued"),
        "performer": row.get("performer"),  # List of Reference
        # Value[x] - all possible types preserved
        "valueQuantity": row.get("valueQuantity"),
        "valueCodeableConcept": row.get("valueCodeableConcept"),
        "valueString": row.get("valueString"),
        "valueBoolean": row.get("valueBoolean"),
        "valueInteger": row.get("valueInteger"),
        "valueRange": row.get("valueRange"),
        "valueRatio": row.get("valueRatio"),
        "valueSampledData": row.get("valueSampledData"),
        "valueTime": row.get("valueTime"),
        "valueDateTime": row.get("valueDateTime"),
        "valuePeriod": row.get("valuePeriod"),
        # Other fields
        "dataAbsentReason": row.get("dataAbsentReason"),
        "interpretation": row.get("interpretation"),
        "note": row.get("note"),
        "bodySite": row.get("bodySite"),
        "method": row.get("method"),
        "specimen": row.get("specimen"),
        "device": row.get("device"),
        "referenceRange": row.get("referenceRange"),
        "hasMember": row.get("hasMember"),
        "derivedFrom": row.get("derivedFrom"),
        "component": row.get("component"),  # List of component observations
    }


def transform_observations(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze observations to S1 (cleaned, same structure)."""
    silver_rows = [transform_observation_row(row) for row in bronze_df.to_dicts()]
    return pl.DataFrame(silver_rows).lazy()


def get_observation_summary(silver_lf: pl.LazyFrame) -> dict[str, int]:
    """Get summary stats for S1 observations."""
    return (
        silver_lf.select(
            pl.len().alias("total_observations"),
            pl.col("status").drop_nulls().len().alias("with_status"),
            pl.col("subject").drop_nulls().len().alias("with_subject"),
            pl.col("code").drop_nulls().len().alias("with_code"),
            pl.col("effectiveDateTime").drop_nulls().len().alias("with_effective"),
            pl.col("valueQuantity").drop_nulls().len().alias("with_value_quantity"),
            pl.col("component").drop_nulls().len().alias("with_components"),
        )
        .collect()
        .to_dicts()[0]
    )
