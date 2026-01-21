"""Silver sources layer - cleaned bronze with source metadata.

Sources adds:
- Source tracking (source_file, source_bundle)
- Null/empty value normalization
- Stable, known columns for select resources that need flattening

Most sources preserve the original FHIR structure; some (e.g., Condition) are
flattened to provide a stable contract for downstream models.
"""

from src.silver.sources.conditions import get_condition
from src.silver.sources.observations import get_observation
from src.silver.sources.patients import get_patient

__all__ = [
    "get_condition",
    "get_observation",
    "get_patient",
]
