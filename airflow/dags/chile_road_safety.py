import subprocess

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


# Directorio principal del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Entorno virtual del pipeline
PYTHON = PROJECT_ROOT / ".venv/bin/python"
DBT = PROJECT_ROOT / ".venv/bin/dbt"
DOTENV = PROJECT_ROOT / ".venv/bin/dotenv"

DBT_DIR = PROJECT_ROOT / "dbt"


@dag(
    dag_id="chile_road_safety_bigquery",
    description="Road safety data pipeline using BigQuery and dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "bigquery"],
)
def chile_road_safety_pipeline():

    # TASK 1: Extraction and validation
    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def extract_and_validate() -> str:

        command = """
from src.ingestion.run_ingestion import main

snapshot_path = main()

print("AIRFLOW_SNAPSHOT_PATH=" + str(snapshot_path.resolve()))
"""
        try:
            result = subprocess.run(
                [str(PYTHON), "-c", command],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )

        except subprocess.CalledProcessError as exc:
            print("Extraction failed.")

            if exc.stdout:
                print(exc.stdout, flush=True)

            if exc.stderr:
                print(exc.stderr, flush=True)

            raise

        print(result.stdout)

        # Recuperar la ruta del snapshot generado
        prefix = "AIRFLOW_SNAPSHOT_PATH="

        for line in result.stdout.splitlines():
            if line.startswith(prefix):
                return line.removeprefix(prefix)

        raise RuntimeError(
            "Could not find generated snapshot path"
        )

    # TASK 2: Incremental load
    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def merge_bigquery(snapshot_path: str):

        subprocess.run(
            [
                str(PYTHON),
                "-m",
                "src.storage.merge_bigquery",
                snapshot_path,
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    # TASK 3: dbt transformations and tests
    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def dbt_build():

        subprocess.run(
            [
                str(DOTENV),
                "run",
                "--",
                str(DBT),
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
                "--target",
                "bigquery",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

    # Definir dependencias
    snapshot_path = extract_and_validate()

    load_task = merge_bigquery(snapshot_path)

    load_task >> dbt_build()


chile_road_safety_pipeline()