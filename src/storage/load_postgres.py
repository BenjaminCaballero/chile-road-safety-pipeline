import json
import os
import sys

from datetime import datetime
from pathlib import Path
from uuid import UUID

import psycopg
from dotenv import load_dotenv

from src.ingestion.validators import validate_records
from src.ingestion.run_ingestion import calculate_source_hash


def load_snapshot(snapshot_path: str) -> None:

    # 1. Leer snapshot JSON
    with open(snapshot_path, "r", encoding="utf-8") as file:
        snapshot = json.load(file)

    records = snapshot["data"]

    # 2. Validar antes de modificar PostgreSQL
    validate_records(records)

    if len(records) != snapshot["row_count"]:
        raise ValueError("Snapshot row count mismatch")

    calculated_hash = calculate_source_hash(records)

    if calculated_hash != snapshot["source_hash"]:
        raise ValueError("Snapshot hash mismatch")

    # 3. Recuperar metadatos
    run_id = UUID(snapshot["run_id"])
    source = snapshot["source"]
    extracted_at = datetime.fromisoformat(
        snapshot["extracted_at"]
    )

    source_hash = snapshot["source_hash"]
    row_count = len(records)

    # 4. Leer configuración
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    # 5. Conectar y ejecutar una transacción
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10
    ) as conn:

        with conn.cursor() as cur:

            # Registrar ejecución si todavía no existe
            cur.execute(
                """
                INSERT INTO raw.ingestion_runs (
                    run_id,
                    source,
                    extracted_at,
                    source_hash,
                    row_count
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    run_id,
                    source,
                    extracted_at,
                    source_hash,
                    row_count
                )
            )

            # Comprobar consistencia si el run_id ya existía
            cur.execute(
                """
                SELECT source_hash, row_count
                FROM raw.ingestion_runs
                WHERE run_id = %s
                """,
                (run_id,)
            )

            stored_hash, stored_count = cur.fetchone()

            if (
                stored_hash != source_hash
                or stored_count != row_count
            ):
                raise ValueError(
                    "Run ID already exists with different metadata"
                )

            # Reemplazar datos vigentes
            cur.execute("DELETE FROM raw.vehicle_safety")

            # Preparar registros
            rows = [
                (
                    record["anio"],
                    record["vehiculo"],
                    record["siniestros"],
                    record["fallecidos"],
                    record["graves"],
                    record["menos_graves"],
                    record["leves"],
                    run_id
                )
                for record in records
            ]

            # Insertar registros
            cur.executemany(
                """
                INSERT INTO raw.vehicle_safety (
                    anio,
                    vehiculo,
                    siniestros,
                    fallecidos,
                    graves,
                    menos_graves,
                    leves,
                    run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows
            )

            # Verificar cantidad almacenada
            cur.execute(
                "SELECT COUNT(*) FROM raw.vehicle_safety"
            )

            loaded_count = cur.fetchone()[0]

            if loaded_count != row_count:
                raise ValueError(
                    "Loaded row count mismatch"
                )

    print(f"Run ID: {run_id}")
    print(f"Successfully loaded {loaded_count} records")


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python -m src.storage.load_postgres "
            "<snapshot_path>"
        )

    load_snapshot(sys.argv[1])