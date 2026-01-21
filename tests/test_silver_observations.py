import polars as pl

from src.silver.models.observations import get_observation as get_observation_model
from src.silver.sources.observations import get_observation as get_observation_source
from src.validations.observations import validate_observation


def test_get_observation_flattens_and_unnests() -> None:
    """Test models observation transformation via sources."""
    # Bronze data (as loaded from FHIR bundles)
    bronze_row = {
        "resourceType": "Observation",
        "id": "obs-1",
        "_source_file": "Bundle-1.json",
        "_source_bundle": "bundle-1",
        "status": "final",
        "subject": {"reference": "Patient/p1"},
        "effectiveDateTime": "2024-01-02T03:04:05Z",
        "encounter": None,
        "issued": None,
        "performer": [{"reference": "Practitioner/pr1"}],
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Vital Signs",
                    }
                ]
            }
        ],
        "code": {
            "text": "Blood pressure panel",
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "85354-9",
                    "display": "Blood pressure panel with all children optional",
                }
            ],
        },
        "valueQuantity": {
            "value": 120,
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]",
        },
        "valueCodeableConcept": None,
        "valueString": None,
        "valueBoolean": None,
        "valueInteger": None,
        "valueRange": None,
        "valueRatio": None,
        "valueSampledData": None,
        "valueTime": None,
        "valueDateTime": None,
        "valuePeriod": None,
        "effectivePeriod": None,
        "dataAbsentReason": None,
        "interpretation": None,
        "note": None,
        "bodySite": None,
        "method": None,
        "specimen": None,
        "device": None,
        "referenceRange": None,
        "hasMember": None,
        "derivedFrom": None,
        "component": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "8480-6",
                            "display": "Systolic blood pressure",
                        }
                    ]
                },
                "valueQuantity": {"value": 120, "unit": "mmHg"},
            }
        ],
    }

    # Transform: bronze → sources → models
    bronze_df = pl.DataFrame([bronze_row])
    sources_lf = get_observation_source(bronze_df)
    obs = get_observation_model(sources_lf).collect()
    assert obs.height == 1
    row = obs.row(0, named=True)
    assert row["id"] == "obs-1"
    assert row["code_code"] == "85354-9"
    assert row["subject_id"] == "p1"
    assert row["value_type"] == "quantity"
    assert row["component_count"] == 1

    code_codings = row["code_codings"]
    assert len(code_codings) == 1
    assert code_codings[0]["code"] == "85354-9"

    category_codings = row["category_codings"]
    assert len(category_codings) == 1
    assert category_codings[0]["code"] == "vital-signs"

    assert row["performer_ids"] == ["pr1"]

    components = row["components"]
    assert len(components) == 1
    assert components[0]["code_code"] == "8480-6"
    assert components[0]["value_type"] == "quantity"


def test_validate_observation_populates_errors() -> None:
    """Test validation catches missing/invalid fields via sources → models flow."""
    bronze_row = {
        "resourceType": "Observation",
        "id": "obs-bad",
        "_source_file": "Bundle-1.json",
        "_source_bundle": "bundle-1",
        "status": "not-a-status",
        "code": {"text": None, "coding": []},
        "subject": None,
        "encounter": None,
        "effectiveDateTime": None,
        "effectivePeriod": None,
        "issued": None,
        "performer": None,
        "category": None,
        "valueQuantity": None,
        "valueCodeableConcept": None,
        "valueString": None,
        "valueBoolean": None,
        "valueInteger": None,
        "valueRange": None,
        "valueRatio": None,
        "valueSampledData": None,
        "valueTime": None,
        "valueDateTime": None,
        "valuePeriod": None,
        "dataAbsentReason": None,
        "interpretation": None,
        "note": None,
        "bodySite": None,
        "method": None,
        "specimen": None,
        "device": None,
        "referenceRange": None,
        "hasMember": None,
        "derivedFrom": None,
        "component": None,
    }

    # Transform: bronze → sources → models
    bronze_df = pl.DataFrame([bronze_row])
    sources_lf = get_observation_source(bronze_df)
    models_lf = get_observation_model(sources_lf)
    validated = validate_observation(models_lf).collect()
    errors = validated.item(0, "validation_errors")
    assert "status_valid" in errors
    assert "code_present" in errors
    assert "subject_present" in errors
