"""Common FHIR data extraction utilities.

These functions handle the common patterns for extracting data from FHIR JSON structures
such as Reference, CodeableConcept, Identifier, and other FHIR data types.
"""

from dataclasses import dataclass
from typing import Any


def iter_dict_list(value: Any) -> list[dict[str, Any]]:
    """Safely iterate over a list that should contain dicts.

    Returns only dict items from the list, filtering out any non-dict values.
    Returns empty list if value is not a list.
    """
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, dict)]


def iter_codings(codeable_concept: Any) -> list[dict[str, Any]]:
    """Iterate over coding entries in a CodeableConcept."""
    if not isinstance(codeable_concept, dict):
        return []
    return iter_dict_list(codeable_concept.get("coding"))


@dataclass
class Coding:
    """Represents a FHIR Coding with system, code, and display."""

    system: str | None
    code: str | None
    display: str | None

    def as_tuple(self) -> tuple[str | None, str | None, str | None]:
        return self.system, self.code, self.display

    def as_dict(self) -> dict[str, str | None]:
        return {"system": self.system, "code": self.code, "display": self.display}


def extract_first_coding(codeable_concept: Any) -> Coding:
    """Extract the first coding from a CodeableConcept.

    Args:
        codeable_concept: A FHIR CodeableConcept dict or None

    Returns:
        Coding dataclass with system, code, display (all None if not found)
    """
    for coding in iter_codings(codeable_concept):
        return Coding(
            system=coding.get("system"),
            code=coding.get("code"),
            display=coding.get("display"),
        )
    return Coding(None, None, None)


def extract_primary_coding(
    codeable_concept: Any,
) -> tuple[str | None, str | None, str | None]:
    """Extract the first coding from a CodeableConcept as tuple."""
    return extract_first_coding(codeable_concept).as_tuple()


def extract_first_coding_as_dict(
    codeable_concept: dict | None,
) -> dict[str, str | None]:
    """Extract first coding from a CodeableConcept as dict."""
    return extract_first_coding(codeable_concept).as_dict()


def extract_code_text(codeable_concept: Any) -> str | None:
    """Extract the text field from a CodeableConcept."""
    if isinstance(codeable_concept, dict):
        text = codeable_concept.get("text")
        return str(text) if text else None
    return None


def extract_reference(ref_obj: Any) -> str | None:
    """Extract reference string from a FHIR Reference object."""
    if not isinstance(ref_obj, dict):
        return None
    ref = ref_obj.get("reference")
    return str(ref) if ref else None


def extract_reference_id(reference: str | None) -> str | None:
    """Extract the ID portion from a FHIR reference string.

    Handles various reference formats:
    - "Patient/123" -> "123"
    - "urn:uuid:abc-def" -> "abc-def"
    - "123" -> "123"
    """
    if not reference:
        return None
    if reference.startswith("urn:uuid:"):
        return reference.split("urn:uuid:", 1)[1] or None
    if "/" in reference:
        return reference.rsplit("/", 1)[-1] or None
    return reference


def _find_in_list_by_field(
    items: list[dict] | None, match_field: str, match_value: str, return_field: str
) -> str | None:
    """Find an item in a list where match_field equals match_value, return return_field."""
    if not items:
        return None
    for item in items:
        if isinstance(item, dict) and item.get(match_field) == match_value:
            return item.get(return_field)
    return None


def extract_identifier(identifier_list: list[dict] | None, system: str) -> str | None:
    """Extract identifier value by system from FHIR identifier array."""
    return _find_in_list_by_field(identifier_list, "system", system, "value")


def extract_telecom(telecom_list: list[dict] | None, system: str) -> str | None:
    """Extract telecom value by system from FHIR telecom array."""
    return _find_in_list_by_field(telecom_list, "system", system, "value")


def _extract_field_from_list(
    items: list[dict] | None, field: str, list_separator: str = ", "
) -> str | None:
    """Extract a field from the first dict in a list, handling list values."""
    if not items:
        return None
    for item in items:
        if isinstance(item, dict):
            value = item.get(field)
            if value is not None:
                if isinstance(value, list):
                    return list_separator.join(str(v) for v in value)
                return str(value) if value else None
    return None


def extract_address_field(address_list: list[dict] | None, field: str) -> str | None:
    """Extract a field from the first FHIR address."""
    return _extract_field_from_list(address_list, field, ", ")


def extract_from_name_list(name_list: list[dict] | None, field: str) -> str | None:
    """Extract a field from the FHIR name array."""
    return _extract_field_from_list(name_list, field, " ")


def extract_extension_value(
    extension_list: list[dict] | None,
    url: str,
    value_path: list[str | int],
) -> Any:
    """Extract a value from a FHIR extension by URL and path."""
    if not extension_list:
        return None
    for ext in extension_list:
        if isinstance(ext, dict) and ext.get("url") == url:
            value: Any = ext
            for key in value_path:
                if isinstance(value, dict):
                    value = value.get(key)
                elif isinstance(value, list) and isinstance(key, int):
                    value = value[key] if 0 <= key < len(value) else None
                else:
                    return None
            return value
    return None


def extract_category_from_list(
    category_list: list[dict] | None,
) -> dict[str, str | None]:
    """Extract category code and display from category array."""
    for cat in iter_dict_list(category_list):
        coding = extract_first_coding(cat)
        if coding.code:
            return {"code": coding.code, "display": coding.display}
    return {"code": None, "display": None}
