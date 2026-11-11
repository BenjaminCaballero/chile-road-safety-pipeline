import subprocess

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

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

# Google Cloud configuration
PROJECT_ID = "chile-road-safety-pipeline"
GCS_BUCKET = "chile-road-safety-pipeline-raw-bc-2026"

# Cloud Run configuration
CLOUD_RUN_JOB = "road-safety-extract"
REGION = "us-central1"


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="chile_road_safety_bigquery",
    description="End-to-end pipeline using Cloud Run, GCS, BigQuery and dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=[
        "data-engineering",
        "gcp",
        "cloud-run",
        "bigquery",
        "gcs",
        "dbt",
    ],
)
def chile_road_safety_pipeline():

    # ========================================================
    # TASK 1: EXECUTE EXTRACTION IN CLOUD RUN
    # ========================================================

    @task(
        retries=0,
    )
    def execute_cloud_run() -> str:

        # Generate a unique object name for this execution
        object_name = (
            f"snapshots/cloud-run/"
            f"airflow-{uuid4().hex}.json"
        )

        # Define the expected GCS URI
        gcs_uri = f"gs://{GCS_BUCKET}/{object_name}"

        print(
            f"Expected snapshot: {gcs_uri}",
            flush=True,
        )

        # Execute Cloud Run Job and wait for completion
        subprocess.run(
            [
                "gcloud",
                "run",
                "jobs",
                "execute",
                CLOUD_RUN_JOB,
                f"--project={PROJECT_ID}",
                f"--region={REGION}",
                f"--update-env-vars=GCS_OBJECT_NAME={object_name}",
                "--wait",
                "--quiet",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "Cloud Run execution completed successfully!",
            flush=True,
        )

        print(
            f"Snapshot available at: {gcs_uri}",
            flush=True,
        )

        # Pass GCS URI to the next task through XCom
        return gcs_uri

    # ========================================================
    # TASK 2: INCREMENTAL LOAD TO BIGQUERY
    # ========================================================

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def merge_bigquery(gcs_uri: str):

        print(
            f"Loading snapshot from: {gcs_uri}",
            flush=True,
        )

        subprocess.run(
            [
                str(PYTHON),
                "-m",
                "src.storage.merge_bigquery",
                gcs_uri,
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

        print(
            "BigQuery incremental load completed!",
            flush=True,
        )

    # ========================================================
    # TASK 3: DBT TRANSFORMATIONS AND DATA QUALITY TESTS
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

        print(
            "dbt build completed successfully!",
            flush=True,
        )

    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    # 1. Execute extraction and validation in Cloud Run
    gcs_uri = execute_cloud_run()

    # 2. Read snapshot from GCS and MERGE into BigQuery
    load_task = merge_bigquery(gcs_uri)

    # 3. Execute dbt transformations and tests
    load_task >> dbt_build()


# ============================================================
# REGISTER DAG
# ============================================================

chile_road_safety_pipeline()