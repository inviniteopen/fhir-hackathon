"""Sources Patient: Cleaned bronze with source metadata.

Sources preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures (name, address, telecom, extension, etc.)
remain intact. For domain-modeled flat structures, see models.
"""

import polars as pl

# Core fields required by models transformation
_REQUIRED_FIELDS = [
    "id",
    "identifier",
    "name",
    "telecom",
    "gender",
    "birthDate",
    "address",
    "extension",
]


def get_patient(bronze_df: pl.DataFrame) -> pl.LazyFrame:
    """Get sources patient from bronze data.

    Selects core FHIR Patient fields needed for models transformation.
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
