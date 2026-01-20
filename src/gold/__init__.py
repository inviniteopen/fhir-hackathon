"""Gold layer - aggregations built from silver tables."""

from src.gold.observations_per_patient import create_observations_per_patient

__all__ = [
    "create_observations_per_patient",
]
