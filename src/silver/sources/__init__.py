"""Silver sources layer - cleaned bronze with source metadata.

Sources preserves the original FHIR structure but adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization

The nested FHIR structures remain intact. For domain-modeled flat structures, see models.
"""

from src.silver.sources.conditions import get_condition
from src.silver.sources.observations import get_observation
from src.silver.sources.patients import get_patient

__all__ = [
    "get_condition",
    "get_observation",
    "get_patient",
]
