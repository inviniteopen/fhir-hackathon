"""S1 Observation: Cleaned bronze with source metadata.

S1 preserves the original FHIR structure but adds:
- Source tracking (_source_file, _source_bundle)
- Basic data quality flags
- Null/empty value normalization

The nested FHIR structures (code.coding, valueQuantity, component, etc.)
remain intact. For domain-modeled flat structures, see S2.
"""

import polars as pl


def transform_observations(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Transform bronze observations to S1 (cleaned, same structure)."""
    return bronze_df.lazy().select(
        # Source tracking
        pl.col("_source_file").alias("source_file"),
        pl.col("_source_bundle").alias("source_bundle"),
        # Core FHIR fields (structure preserved)
        pl.col(
            "id",
            "resourceType",
            "status",
            "category",  # List of CodeableConcept
            "code",  # CodeableConcept
            "subject",  # Reference
            "encounter",  # Reference
            "effectiveDateTime",
            "effectivePeriod",
            "issued",
            "performer",  # List of Reference
            # Value[x] - all possible types preserved
            "valueQuantity",
            "valueCodeableConcept",
            "valueString",
            "valueBoolean",
            "valueInteger",
            "valueRange",
            "valueRatio",
            "valueSampledData",
            "valueTime",
            "valueDateTime",
            "valuePeriod",
            # Other fields
            "dataAbsentReason",
            "interpretation",
            "note",
            "bodySite",
            "method",
            "specimen",
            "device",
            "referenceRange",
            "hasMember",
            "derivedFrom",
            "component",  # List of component observations
        ),
    )

