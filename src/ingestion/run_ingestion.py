import json
import hashlib

from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.api_client import fetch_all_records
from src.ingestion.validators import validate_records


def calculate_source_hash(records: list[dict]) -> str:

    ordered_records = sorted(
        records,
        key=lambda record: (
            record["anio"],
            record["vehiculo"].strip().casefold()
        )
    )

    canonical_json = json.dumps(
        ordered_records,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def main():

    # 1. Extraer
    records = fetch_all_records()

    # 2. Validar
    validate_records(records)

    # 3. Generar metadatos
    run_id = str(uuid4())

    extracted_at = datetime.now(timezone.utc)

    source_hash = calculate_source_hash(records)

    # 4. Preparar directorio
    output_dir = Path("data/raw")

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    # 5. Generar nombre
    timestamp = extracted_at.strftime("%Y%m%d_%H%M%S_%f")

    output_path = output_dir / f"siniestros_{timestamp}_{run_id[:8]}.json"

    # 6. Preparar snapshot
    snapshot = {
        "run_id": run_id,
        "source": "Observatorio Logistico - C015",
        "extracted_at": extracted_at.isoformat(),
        "row_count": len(records),
        "source_hash": source_hash,
        "data": records
    }

    # 7. Guardar snapshot
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            snapshot,
            file,
            ensure_ascii=False,
            indent=2
        )

    print(f"Run ID: {run_id}")
    print(f"Records: {len(records)}")
    print(f"SHA-256: {source_hash}")
    print(f"Snapshot saved: {output_path}")
    return output_path


if __name__ == "__main__":
    main()