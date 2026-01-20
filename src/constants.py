"""Shared constants for FHIR data processing."""

from enum import StrEnum


class Schema(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
