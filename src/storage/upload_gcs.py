import json
import os
import sys

from pathlib import Path
from uuid import UUID

from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed

from src.ingestion.validators import validate_records
from src.ingestion.run_ingestion import calculate_source_hash


PROJECT_ID = "chile-road-safety-pipeline"


def upload_snapshot_gcs(snapshot_path: str) -> str:

    # 1. Leer snapshot
    path = Path(snapshot_path).resolve()

    snapshot = json.loads(
        path.read_text(encoding="utf-8")
    )

    records = snapshot["data"]

    # 2. Validar información
    validate_records(records)

    if len(records) != snapshot["row_count"]:
        raise ValueError("Snapshot row count mismatch")

    if calculate_source_hash(records) != snapshot["source_hash"]:
        raise ValueError("Snapshot hash mismatch")

    run_id = str(UUID(snapshot["run_id"]))

    # 3. Configurar Cloud Storage
    bucket_name = os.environ["GCS_BUCKET"]

    client = storage.Client(project=PROJECT_ID)

    bucket = client.bucket(bucket_name)

    object_name = (
        os.environ.get("GCS_OBJECT_NAME")
        or f"snapshots/{path.name}"
    )

    if not object_name.startswith("snapshots/"):
        raise ValueError("GCS object must be inside snapshots/")

    blob = bucket.blob(object_name)

    blob.metadata = {
        "run_id": run_id,
        "source_hash": snapshot["source_hash"],
    }

    # 4. Subir archivo sin sobrescribir snapshots existentes
    try:
        blob.upload_from_filename(
            str(path),
            content_type="application/json",
            if_generation_match=0,
        )

        print("Snapshot uploaded successfully!")

    except PreconditionFailed:

        # El archivo ya existe. Verificar que corresponde
        # al mismo snapshot antes de considerarlo exitoso.
        blob.reload()

        metadata = blob.metadata or {}

        if (
            metadata.get("run_id") != run_id
            or metadata.get("source_hash") != snapshot["source_hash"]
        ):
            raise ValueError(
                "Existing GCS object does not match snapshot"
            )

        print("Snapshot already exists in GCS. Skipping upload.")

    gcs_uri = f"gs://{bucket_name}/{object_name}"

    print(f"GCS URI: {gcs_uri}")
    print(f"Records: {len(records)}")

    return gcs_uri


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python -m src.storage.upload_gcs "
            "<snapshot_path>"
        )

    upload_snapshot_gcs(sys.argv[1])