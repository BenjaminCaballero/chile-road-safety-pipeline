import json
import sys
from pathlib import Path
from uuid import UUID, uuid4

from google.cloud import bigquery

from src.ingestion.validators import validate_records
from src.ingestion.run_ingestion import calculate_source_hash


PROJECT_ID = "chile-road-safety-pipeline"
DATASET_ID = "road_safety_raw"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.vehicle_safety"


def merge_snapshot_bigquery(snapshot_path: str) -> None:

    # 1. Leer snapshot
    snapshot = json.loads(
        Path(snapshot_path).read_text(encoding="utf-8")
    )

    records = snapshot["data"]

    # 2. Validar antes de modificar BigQuery
    validate_records(records)

    if len(records) != snapshot["row_count"]:
        raise ValueError("Snapshot row count mismatch")

    if calculate_source_hash(records) != snapshot["source_hash"]:
        raise ValueError("Snapshot hash mismatch")

    run_id = str(UUID(snapshot["run_id"]))

    rows = [
        {**record, "run_id": run_id}
        for record in records
    ]

    # 3. Conectar a BigQuery
    client = bigquery.Client(project=PROJECT_ID)

    # Nombre único para evitar compartir staging entre ejecuciones
    staging_table = (
        f"{PROJECT_ID}.{DATASET_ID}."
        f"vehicle_safety_staging_{uuid4().hex}"
    )

    try:
        # 4. Cargar datos validados a staging
        target_schema = client.get_table(TARGET_TABLE).schema

        job_config = bigquery.LoadJobConfig(
            schema=target_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = client.load_table_from_json(
            rows,
            staging_table,
            job_config=job_config,
            location="US",
        )

        load_job.result()

        loaded_count = client.get_table(staging_table).num_rows

        if loaded_count != len(records):
            raise ValueError("Staging row count mismatch")

        # 5. Actualizar o insertar registros
        merge_sql = f"""
        MERGE `{TARGET_TABLE}` AS target
        USING `{staging_table}` AS source

        ON target.anio = source.anio
        AND target.vehiculo = source.vehiculo

        WHEN MATCHED AND (
            target.siniestros IS DISTINCT FROM source.siniestros
            OR target.fallecidos IS DISTINCT FROM source.fallecidos
            OR target.graves IS DISTINCT FROM source.graves
            OR target.menos_graves IS DISTINCT FROM source.menos_graves
            OR target.leves IS DISTINCT FROM source.leves
        )
        THEN UPDATE SET
            siniestros = source.siniestros,
            fallecidos = source.fallecidos,
            graves = source.graves,
            menos_graves = source.menos_graves,
            leves = source.leves,
            run_id = source.run_id

        WHEN NOT MATCHED THEN
            INSERT (
                anio,
                vehiculo,
                siniestros,
                fallecidos,
                graves,
                menos_graves,
                leves,
                run_id
            )
            VALUES (
                source.anio,
                source.vehiculo,
                source.siniestros,
                source.fallecidos,
                source.graves,
                source.menos_graves,
                source.leves,
                source.run_id
            )
        """

        merge_job = client.query(merge_sql, location="US")
        merge_job.result()

        print("BigQuery MERGE completed successfully!")
        print(f"Snapshot records: {len(records)}")
        print(f"Affected rows: {merge_job.num_dml_affected_rows}")
        print(f"Job ID: {merge_job.job_id}")

    finally:
        # 6. Eliminar staging incluso si ocurre un error
        client.delete_table(staging_table, not_found_ok=True)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python -m src.storage.merge_bigquery "
            "<snapshot_path>"
        )

    merge_snapshot_bigquery(sys.argv[1])