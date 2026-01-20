"""FHIR constants for the silver layer transforms.

Contains standardized values, system URIs, and other constants used across
the FHIR data transformations.
"""


class ObservationStatus:
    """FHIR Observation status values."""

    REGISTERED = "registered"
    PRELIMINARY = "preliminary"
    FINAL = "final"
    AMENDED = "amended"
    CORRECTED = "corrected"
    CANCELLED = "cancelled"
    ENTERED_IN_ERROR = "entered-in-error"
    UNKNOWN = "unknown"


class ConditionClinicalStatus:
    """FHIR Condition clinical status values."""

    ACTIVE = "active"
    RECURRENCE = "recurrence"
    RELAPSE = "relapse"
    INACTIVE = "inactive"
    REMISSION = "remission"
    RESOLVED = "resolved"


class ConditionVerificationStatus:
    """FHIR Condition verification status values."""

    UNCONFIRMED = "unconfirmed"
    PROVISIONAL = "provisional"
    DIFFERENTIAL = "differential"
    CONFIRMED = "confirmed"
    REFUTED = "refuted"
    ENTERED_IN_ERROR = "entered-in-error"


class Gender:
    """FHIR AdministrativeGender values."""

    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    UNKNOWN = "unknown"


class ObservationValueType:
    """Observation value[x] type identifiers."""

    QUANTITY = "quantity"
    CODEABLE_CONCEPT = "codeable_concept"
    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    DATETIME = "datetime"


class FhirSystem:
    """Common FHIR system URIs."""

    LOINC = "http://loinc.org"
    SNOMED_CT = "http://snomed.info/sct"
    ICD_10 = "http://hl7.org/fhir/sid/icd-10"
    ICD_10_CM = "http://hl7.org/fhir/sid/icd-10-cm"
    UCUM = "http://unitsofmeasure.org"
    OBSERVATION_CATEGORY = "http://terminology.hl7.org/CodeSystem/observation-category"
    CONDITION_CATEGORY = "http://terminology.hl7.org/CodeSystem/condition-category"


class ObservationCategory:
    """Common observation category codes."""

    VITAL_SIGNS = "vital-signs"
    LABORATORY = "laboratory"
    IMAGING = "imaging"
    PROCEDURE = "procedure"
    SURVEY = "survey"
    EXAM = "exam"
    THERAPY = "therapy"
    ACTIVITY = "activity"
    SOCIAL_HISTORY = "social-history"


class IdentifierSystem:
    """Identifier system URIs used in the project."""

    ECI = "http://ec.europa.eu/identifier/eci"
    MR = "http://local.setting.eu/identifier"


class ExtensionUrl:
    """Common FHIR extension URLs."""

    NATIONALITY = "http://hl7.org/fhir/StructureDefinition/patient-nationality"
