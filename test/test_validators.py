import pytest

from src.ingestion.validators import validate_records


VALID_RECORD = {
    "anio": 2025,
    "vehiculo": "Automóvil",
    "siniestros": 100,
    "fallecidos": 5,
    "graves": 10,
    "menos_graves": 15,
    "leves": 70
}

def test_valid_record():

    records = [VALID_RECORD]

    result = validate_records(records)

    assert result is None

def test_negative_siniestros():

    invalid_record = {
        **VALID_RECORD,
        "siniestros": -100
    }

    with pytest.raises(ValueError):
        validate_records([invalid_record])

def test_empty_dataset():

    with pytest.raises(ValueError):
        validate_records([])

def test_missing_field():

    invalid_record = VALID_RECORD.copy()

    del invalid_record["fallecidos"]

    with pytest.raises(ValueError):
        validate_records([invalid_record])


def test_duplicate_records():

    records = [
        VALID_RECORD,
        VALID_RECORD.copy()
    ]

    with pytest.raises(ValueError):
        validate_records(records)

def test_invalid_data_type():

    invalid_record = {
        **VALID_RECORD,
        "siniestros": "100"
    }

    with pytest.raises(ValueError):
        validate_records([invalid_record])