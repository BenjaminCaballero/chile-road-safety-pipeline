import json
import sys
from pathlib import Path
from uuid import UUID

from google.cloud import bigquery

from src.ingestion.validators import validate_records
from src.ingestion.run_ingestion import calculate_source_hash


PROJECT_ID = "chile-road-safety-pipeline"
DATASET_ID = "road_safety_raw"
TABLE_ID = "vehicle_safety"


def load_snapshot_bigquery(snapshot_path: str) -> None:

    # 1. Leer snapshot
    with open(snapshot_path, "r", encoding="utf-8") as file:
        snapshot = json.load(file)

    records = snapshot["data"]

    # 2. Validaciones
    validate_records(records)

    if len(records) != snapshot["row_count"]:
        raise ValueError("Snapshot row count mismatch")

    if calculate_source_hash(records) != snapshot["source_hash"]:
        raise ValueError("Snapshot hash mismatch")

    run_id = str(UUID(snapshot["run_id"]))

    # 3. Preparar los registros
    rows = [
        {
            **record,
            "run_id": run_id
        }
        for record in records
    ]

    # 4. Crear cliente BigQuery
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 5. Definir esquema
    schema = [
        bigquery.SchemaField("anio", "INTEGER"),
        bigquery.SchemaField("vehiculo", "STRING"),
        bigquery.SchemaField("siniestros", "INTEGER"),
        bigquery.SchemaField("fallecidos", "INTEGER"),
        bigquery.SchemaField("graves", "INTEGER"),
        bigquery.SchemaField("menos_graves", "INTEGER"),
        bigquery.SchemaField("leves", "INTEGER"),
        bigquery.SchemaField("run_id", "STRING"),
    ]

    # 6. Configurar trabajo de carga
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # 7. Ejecutar carga
    load_job = client.load_table_from_json(
        rows,
        table_id,
        job_config=job_config,
        location="US",
    )

    load_job.result()

    # 8. Verificar resultado
    table = client.get_table(table_id)

    if table.num_rows != len(records):
        raise ValueError(
            f"Row count mismatch: "
            f"expected {len(records)}, "
            f"loaded {table.num_rows}"
        )

    print(f"Table: {table_id}")
    print(f"Loaded records: {table.num_rows}")
    print(f"Job ID: {load_job.job_id}")
    print("BigQuery load completed successfully!")


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python -m src.storage.load_bigquery "
            "<snapshot_path>"
        )

    load_snapshot_bigquery(sys.argv[1])