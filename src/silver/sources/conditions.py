"""Sources Condition: Cleaned bronze with source metadata.

Sources preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (code, subject, category, etc.)
remain intact. For domain-modeled flat structures, see models.
"""

import polars as pl

# Core fields required by models transformation
_REQUIRED_FIELDS = [
    "id",
    "category",
    "code",
    "subject",
    "onsetDateTime",
    "_onsetDateTime",
    "abatementDateTime",
    "_abatementDateTime",
    "asserter",
]


def get_condition(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Get sources condition from bronze data.

    Selects core FHIR Condition fields needed for models transformation.
    Missing optional fields are added as null columns.
    """
    # Start with source tracking
    exprs = [
        pl.col("_source_file").alias("source_file"),
        pl.col("_source_bundle").alias("source_bundle"),
    ]

    # Add required fields, using null if column doesn't exist
    existing_cols = set(bronze_df.columns)
    for field in _REQUIRED_FIELDS:
        if field in existing_cols:
            exprs.append(pl.col(field))
        else:
            exprs.append(pl.lit(None).alias(field))

    return bronze_df.lazy().select(exprs)
