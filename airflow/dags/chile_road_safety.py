import os
import subprocess

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


# ============================================================
# CONFIGURATION
# ============================================================

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Python environment used by the data pipeline
PYTHON = PROJECT_ROOT / ".venv/bin/python"

# dbt and dotenv executables
DBT = PROJECT_ROOT / ".venv/bin/dbt"
DOTENV = PROJECT_ROOT / ".venv/bin/dotenv"

# dbt project directory
DBT_DIR = PROJECT_ROOT / "dbt"

# Google Cloud Storage bucket
GCS_BUCKET = "chile-road-safety-pipeline-raw-bc-2026"


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="chile_road_safety_bigquery",
    description="End-to-end road safety pipeline using GCS, BigQuery and dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "gcp", "bigquery", "gcs", "dbt"],
)
def chile_road_safety_pipeline():

    # ========================================================
    # TASK 1: EXTRACT AND VALIDATE
    # ========================================================

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
            print("Extraction failed.", flush=True)

            if exc.stdout:
                print(exc.stdout, flush=True)

            if exc.stderr:
                print(exc.stderr, flush=True)

            raise

        print(result.stdout, flush=True)

        # Extract snapshot path from command output
        prefix = "AIRFLOW_SNAPSHOT_PATH="

        for line in result.stdout.splitlines():
            if line.startswith(prefix):
                return line.removeprefix(prefix)

        raise RuntimeError(
            "Could not find generated snapshot path"
        )

    # ========================================================
    # TASK 2: UPLOAD SNAPSHOT TO GOOGLE CLOUD STORAGE
    # ========================================================

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def upload_to_gcs(snapshot_path: str) -> str:

        # Copy existing environment variables
        env = os.environ.copy()

        # Configure GCS bucket for the subprocess
        env["GCS_BUCKET"] = GCS_BUCKET

        subprocess.run(
            [
                str(PYTHON),
                "-m",
                "src.storage.upload_gcs",
                snapshot_path,
            ],
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
        )

        # Return the local path for the next task
        gcs_uri = (
            f"gs://{GCS_BUCKET}/snapshots/{Path(snapshot_path).name}"
        )

        return gcs_uri

    # ========================================================
    # TASK 3: INCREMENTAL LOAD TO BIGQUERY
    # ========================================================

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

    # ========================================================
    # TASK 4: DBT TRANSFORMATIONS AND TESTS
    # ========================================================

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

    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    # 1. Extract and validate data
    snapshot_path = extract_and_validate()

    # 2. Upload snapshot to GCS
    gcs_uri = upload_to_gcs(snapshot_path)

    # 3. Incrementally load data into BigQuery
    load_task = merge_bigquery(gcs_uri)

    # 4. Execute dbt transformations and tests
    load_task >> dbt_build()


# Register the DAG
chile_road_safety_pipeline()