"""Summary helpers for Silver models dataframes."""

from __future__ import annotations

import polars as pl

from src.common.models import Condition, Observation, Patient


def get_patient_summary(models_lf: Patient | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for patient model data."""
    return (
        models_lf.select(
            pl.len().alias("total_patients"),
            Patient.family_name.drop_nulls().len().alias("with_family_name"),
            Patient.given_names.drop_nulls().len().alias("with_given_names"),
            Patient.birth_date.drop_nulls().len().alias("with_birth_date"),
            Patient.gender.drop_nulls().len().alias("with_gender"),
            Patient.phone.drop_nulls().len().alias("with_phone"),
            Patient.city.drop_nulls().len().alias("with_city"),
            Patient.nationality_code.drop_nulls().len().alias("with_nationality"),
        )
        .collect()
        .to_dicts()[0]
    )


def get_condition_summary(models_lf: Condition | pl.LazyFrame) -> dict[str, int]:
    """Get summary statistics for condition model data."""
    return (
        models_lf.select(
            pl.len().alias("total_conditions"),
            Condition.patient_id.drop_nulls().len().alias("with_patient_id"),
            Condition.code.drop_nulls().len().alias("with_code"),
            Condition.code_display.drop_nulls().len().alias("with_code_display"),
            Condition.onset_date.drop_nulls().len().alias("with_onset_date"),
            Condition.category_code.drop_nulls().len().alias("with_category"),
        )
        .collect()
        .to_dicts()[0]
    )


def get_observation_summary(
    models_lf: Observation | pl.LazyFrame,
) -> dict[str, int]:
    """Get summary statistics for observation model data."""
    return (
        models_lf.select(
            pl.len().alias("total_observations"),
            Observation.status.drop_nulls().len().alias("with_status"),
            Observation.subject_reference.drop_nulls().len().alias("with_subject"),
            Observation.code_code.drop_nulls().len().alias("with_code"),
            Observation.effective_datetime.drop_nulls()
            .len()
            .alias("with_effective_datetime"),
            (Observation.component_count > 0).sum().alias("with_components"),
            Observation.value_type.drop_nulls().len().alias("with_value"),
            (Observation.performer_references.list.len() > 0)
            .sum()
            .alias("with_performers"),
        )
        .collect()
        .to_dicts()[0]
    )
