import json
from uuid import uuid4
from unittest.mock import MagicMock

import pytest

from src.ingestion.run_ingestion import calculate_source_hash
from src.storage import load_bigquery


@pytest.fixture
def snapshot_path(tmp_path):
    """Crea un snapshot válido para las pruebas."""

    records = [{
        "anio": 2025,
        "vehiculo": "Automóvil",
        "siniestros": 100,
        "fallecidos": 5,
        "graves": 10,
        "menos_graves": 15,
        "leves": 70
    }]

    snapshot = {
        "run_id": str(uuid4()),
        "row_count": len(records),
        "source_hash": calculate_source_hash(records),
        "data": records
    }

    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(snapshot, ensure_ascii=False),
        encoding="utf-8"
    )

    return path


def test_successful_bigquery_load(snapshot_path, monkeypatch):
    """Comprueba que se ejecuta una carga completa."""

    mock_client = MagicMock()

    mock_client.get_table.return_value.num_rows = 1

    monkeypatch.setattr(
        load_bigquery.bigquery,
        "Client",
        lambda **kwargs: mock_client
    )

    load_bigquery.load_snapshot_bigquery(str(snapshot_path))

    mock_client.load_table_from_json.assert_called_once()

    args, kwargs = mock_client.load_table_from_json.call_args

    assert len(args[0]) == 1

    assert kwargs["job_config"].write_disposition == (
        load_bigquery.bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    mock_client.load_table_from_json.return_value.result.assert_called_once()


def test_invalid_hash_rejected(snapshot_path, monkeypatch):
    """Comprueba que un snapshot alterado no se carga."""

    snapshot = json.loads(
        snapshot_path.read_text(encoding="utf-8")
    )

    snapshot["data"][0]["siniestros"] = 999

    snapshot_path.write_text(
        json.dumps(snapshot),
        encoding="utf-8"
    )

    mock_client = MagicMock()

    monkeypatch.setattr(
        load_bigquery.bigquery,
        "Client",
        lambda **kwargs: mock_client
    )

    with pytest.raises(ValueError, match="Snapshot hash mismatch"):
        load_bigquery.load_snapshot_bigquery(str(snapshot_path))

    mock_client.load_table_from_json.assert_not_called()


def test_loaded_row_count_mismatch(snapshot_path, monkeypatch):
    """Comprueba que detectamos una carga incompleta."""

    mock_client = MagicMock()

    mock_client.get_table.return_value.num_rows = 0

    monkeypatch.setattr(
        load_bigquery.bigquery,
        "Client",
        lambda **kwargs: mock_client
    )

    with pytest.raises(ValueError, match="Row count mismatch"):
        load_bigquery.load_snapshot_bigquery(str(snapshot_path))