import json
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from google.api_core.exceptions import PreconditionFailed

from src.ingestion.run_ingestion import calculate_source_hash
from src.storage import merge_bigquery, upload_gcs


@pytest.fixture
def snapshot_path(tmp_path):
    """Create a valid snapshot for testing."""

    records = [
        {
            "anio": 2025,
            "vehiculo": "Automóvil",
            "siniestros": 100,
            "fallecidos": 5,
            "graves": 10,
            "menos_graves": 15,
            "leves": 70,
        }
    ]

    snapshot = {
        "run_id": str(uuid4()),
        "row_count": len(records),
        "source_hash": calculate_source_hash(records),
        "data": records,
    }

    path = tmp_path / "snapshot.json"

    path.write_text(
        json.dumps(snapshot, ensure_ascii=False),
        encoding="utf-8",
    )

    return path


def test_read_snapshot_from_gcs(snapshot_path, monkeypatch):
    """Read a JSON snapshot using a GCS URI."""

    expected = json.loads(
        snapshot_path.read_text(encoding="utf-8")
    )

    mock_client = MagicMock()
    mock_blob = mock_client.bucket.return_value.blob.return_value

    mock_blob.download_as_text.return_value = json.dumps(expected)

    monkeypatch.setattr(
        merge_bigquery.storage,
        "Client",
        lambda **kwargs: mock_client,
    )

    result = merge_bigquery.read_snapshot(
        "gs://test-bucket/snapshots/snapshot.json"
    )

    assert result == expected

    mock_client.bucket.assert_called_once_with("test-bucket")

    mock_client.bucket.return_value.blob.assert_called_once_with(
        "snapshots/snapshot.json"
    )

    mock_blob.download_as_text.assert_called_once_with(
        encoding="utf-8"
    )


def test_upload_new_snapshot(snapshot_path, monkeypatch):
    """Upload a new snapshot without allowing overwrites."""

    monkeypatch.setenv("GCS_BUCKET", "test-bucket")

    mock_client = MagicMock()
    mock_blob = mock_client.bucket.return_value.blob.return_value

    monkeypatch.setattr(
        upload_gcs.storage,
        "Client",
        lambda **kwargs: mock_client,
    )

    result = upload_gcs.upload_snapshot_gcs(str(snapshot_path))

    assert result == "gs://test-bucket/snapshots/snapshot.json"

    mock_blob.upload_from_filename.assert_called_once_with(
        str(snapshot_path),
        content_type="application/json",
        if_generation_match=0,
    )


def test_existing_snapshot_is_accepted(snapshot_path, monkeypatch):
    """Accept an existing object when its metadata matches."""

    monkeypatch.setenv("GCS_BUCKET", "test-bucket")

    snapshot = json.loads(
        snapshot_path.read_text(encoding="utf-8")
    )

    mock_client = MagicMock()
    mock_blob = mock_client.bucket.return_value.blob.return_value

    mock_blob.upload_from_filename.side_effect = PreconditionFailed(
        "Object already exists"
    )

    def reload_metadata():
        mock_blob.metadata = {
            "run_id": snapshot["run_id"],
            "source_hash": snapshot["source_hash"],
        }

    mock_blob.reload.side_effect = reload_metadata

    monkeypatch.setattr(
        upload_gcs.storage,
        "Client",
        lambda **kwargs: mock_client,
    )

    result = upload_gcs.upload_snapshot_gcs(str(snapshot_path))

    assert result == "gs://test-bucket/snapshots/snapshot.json"

    mock_blob.reload.assert_called_once()


def test_existing_snapshot_with_wrong_metadata(
    snapshot_path,
    monkeypatch,
):
    """Reject an existing object with different metadata."""

    monkeypatch.setenv("GCS_BUCKET", "test-bucket")

    mock_client = MagicMock()
    mock_blob = mock_client.bucket.return_value.blob.return_value

    mock_blob.upload_from_filename.side_effect = PreconditionFailed(
        "Object already exists"
    )

    def reload_metadata():
        mock_blob.metadata = {
            "run_id": "different-run",
            "source_hash": "different-hash",
        }

    mock_blob.reload.side_effect = reload_metadata

    monkeypatch.setattr(
        upload_gcs.storage,
        "Client",
        lambda **kwargs: mock_client,
    )

    with pytest.raises(
        ValueError,
        match="Existing GCS object does not match snapshot",
    ):
        upload_gcs.upload_snapshot_gcs(str(snapshot_path))