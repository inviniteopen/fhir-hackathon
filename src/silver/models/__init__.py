"""Silver models layer - domain-modeled transformations.

Models transforms sources data toward domain-specific analytical models:
- Flattens nested structures
- Extracts primary values from complex types
- Normalizes references and identifiers
- Prepares data for domain-level analytics

Input: Sources LazyFrame (cleaned bronze with source metadata)
Output: Typed LazyFrame with flattened domain model
"""

from src.silver.models.conditions import get_condition
from src.silver.models.observations import get_observation
from src.silver.models.patients import get_patient

__all__ = [
    "get_condition",
    "get_observation",
    "get_patient",
]
