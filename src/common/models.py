"""TypedDataFrame class definitions for FHIR data models.

Silver layer concepts:
- sources: Cleaned bronze - same structure as source, with source metadata added
- models: Domain-modeled - source data transformed toward domain concepts
- domains: Unified model - multiple sources merged into common schema (not yet implemented)

Sources uses dynamic schemas (preserves source structure), so no typed models are defined.
Models are typed to enforce domain schema consistency.
"""

from typing import Any, Optional

import polars as pl

from das.engine.polars.typed_dataframe import Col, TypedLazyFrame


class Patient(TypedLazyFrame):
    """Patient model schema - domain-modeled from FHIR Patient."""

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    family_name: Col[Optional[str]]
    given_names: Col[Optional[str]]
    full_name: Col[Optional[str]]
    birth_date: Col[Optional[str]]
    gender: Col[Optional[str]]
    phone: Col[Optional[str]]
    address_line: Col[Optional[str]]
    city: Col[Optional[str]]
    postal_code: Col[Optional[str]]
    country: Col[Optional[str]]
    nationality_code: Col[Optional[str]]
    identifier_eci: Col[Optional[str]]
    identifier_mr: Col[Optional[str]]
    validation_errors: Col[list[str]]


class Observation(TypedLazyFrame):
    """Observation model schema - domain-modeled from FHIR Observation.

    Flattens nested FHIR structures into typed columns suitable for analytics:
    - code.coding[0] → code_system, code_code, code_display
    - subject.reference → subject_id
    - valueQuantity → value_quantity_value, value_quantity_unit
    - component[] → components list with extracted values
    """

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    status: Col[Optional[str]]
    subject_reference: Col[Optional[str]]
    subject_id: Col[Optional[str]]
    effective_datetime: Col[Optional[str]]
    issued: Col[Optional[str]]

    category_text: Col[Optional[str]]
    category_system: Col[Optional[str]]
    category_code: Col[Optional[str]]
    category_display: Col[Optional[str]]

    code_text: Col[Optional[str]]
    code_system: Col[Optional[str]]
    code_code: Col[Optional[str]]
    code_display: Col[Optional[str]]

    value_type: Col[Optional[str]]
    value_quantity_value: Col[Optional[float]]
    value_quantity_unit: Col[Optional[str]]
    value_quantity_system: Col[Optional[str]]
    value_quantity_code: Col[Optional[str]]

    value_codeable_concept_text: Col[Optional[str]]
    value_codeable_concept_system: Col[Optional[str]]
    value_codeable_concept_code: Col[Optional[str]]
    value_codeable_concept_display: Col[Optional[str]]

    value_string: Col[Optional[str]]
    value_boolean: Col[Optional[bool]]
    value_integer: Col[Optional[int]]
    value_datetime: Col[Optional[str]]

    performer_references: Col[list[Optional[str]]]
    performer_ids: Col[list[Optional[str]]]
    code_codings: Col[list[dict[str, Any]]]
    category_codings: Col[list[dict[str, Any]]]
    components: Col[list[dict[str, Any]]]
    component_count: Col[int]

    validation_errors: Col[list[str]]


class Condition(TypedLazyFrame):
    """Condition model schema - domain-modeled from FHIR Condition."""

    id: Col[str]
    source_file: Col[str]
    source_bundle: Col[str]
    patient_id: Col[Optional[str]]
    patient_display: Col[Optional[str]]
    category_code: Col[Optional[str]]
    category_display: Col[Optional[str]]
    code_system: Col[Optional[str]]
    code: Col[Optional[str]]
    code_display: Col[Optional[str]]
    code_text: Col[Optional[str]]
    onset_date: Col[Optional[str]]
    abatement_date: Col[Optional[str]]
    asserter_display: Col[Optional[str]]
    validation_errors: Col[list[str]]


# Polars schema definitions for DataFrame creation

PATIENT_SCHEMA = {
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
    "validation_errors": pl.List(pl.String),
}


# Observation model schema - domain-modeled
CODE_CODINGS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("system", pl.String),
            pl.Field("code", pl.String),
            pl.Field("display", pl.String),
        ]
    )
)

CATEGORY_CODINGS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("category_index", pl.Int64),
            pl.Field("system", pl.String),
            pl.Field("code", pl.String),
            pl.Field("display", pl.String),
        ]
    )
)

COMPONENTS_TYPE = pl.List(
    pl.Struct(
        [
            pl.Field("component_index", pl.Int64),
            pl.Field("code_text", pl.String),
            pl.Field("code_system", pl.String),
            pl.Field("code_code", pl.String),
            pl.Field("code_display", pl.String),
            pl.Field("value_type", pl.String),
            pl.Field("value_quantity_value", pl.Float64),
            pl.Field("value_quantity_unit", pl.String),
            pl.Field("value_quantity_system", pl.String),
            pl.Field("value_quantity_code", pl.String),
            pl.Field("value_codeable_concept_text", pl.String),
            pl.Field("value_codeable_concept_system", pl.String),
            pl.Field("value_codeable_concept_code", pl.String),
            pl.Field("value_codeable_concept_display", pl.String),
            pl.Field("value_string", pl.String),
            pl.Field("value_boolean", pl.Boolean),
            pl.Field("value_integer", pl.Int64),
            pl.Field("value_datetime", pl.String),
        ]
    )
)

OBSERVATION_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.String,
    "source_file": pl.String,
    "source_bundle": pl.String,
    "status": pl.String,
    "subject_reference": pl.String,
    "subject_id": pl.String,
    "effective_datetime": pl.String,
    "issued": pl.String,
    "category_text": pl.String,
    "category_system": pl.String,
    "category_code": pl.String,
    "category_display": pl.String,
    "code_text": pl.String,
    "code_system": pl.String,
    "code_code": pl.String,
    "code_display": pl.String,
    "value_type": pl.String,
    "value_quantity_value": pl.Float64,
    "value_quantity_unit": pl.String,
    "value_quantity_system": pl.String,
    "value_quantity_code": pl.String,
    "value_codeable_concept_text": pl.String,
    "value_codeable_concept_system": pl.String,
    "value_codeable_concept_code": pl.String,
    "value_codeable_concept_display": pl.String,
    "value_string": pl.String,
    "value_boolean": pl.Boolean,
    "value_integer": pl.Int64,
    "value_datetime": pl.String,
    "performer_references": pl.List(pl.String),
    "performer_ids": pl.List(pl.String),
    "code_codings": CODE_CODINGS_TYPE,
    "category_codings": CATEGORY_CODINGS_TYPE,
    "components": COMPONENTS_TYPE,
    "component_count": pl.Int64,
    "validation_errors": pl.List(pl.String),
}

CONDITION_SCHEMA = {
    "id": pl.String,
    "source_file": pl.String,
    "source_bundle": pl.String,
    "patient_id": pl.String,
    "patient_display": pl.String,
    "category_code": pl.String,
    "category_display": pl.String,
    "code_system": pl.String,
    "code": pl.String,
    "code_display": pl.String,
    "code_text": pl.String,
    "onset_date": pl.String,
    "abatement_date": pl.String,
    "asserter_display": pl.String,
    "validation_errors": pl.List(pl.String),
}
