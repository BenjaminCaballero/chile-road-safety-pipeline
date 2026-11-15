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

# Google Cloud configuration
PROJECT_ID = "chile-road-safety-pipeline"
GCS_BUCKET = "chile-road-safety-pipeline-raw-bc-2026"

# Cloud Run configuration
CLOUD_RUN_JOB = "road-safety-extract"
CLOUD_RUN_MERGE_JOB = "road-safety-merge"
CLOUD_RUN_DBT_JOB = "road-safety-dbt"

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
    # TASK 2: EXECUTE BIGQUERY MERGE IN CLOUD RUN
    # ========================================================

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def execute_cloud_merge(gcs_uri: str):

        print(
            f"Processing snapshot: {gcs_uri}",
            flush=True,
        )

        result = subprocess.run(
            [
                "gcloud",
                "run",
                "jobs",
                "execute",
                CLOUD_RUN_MERGE_JOB,
                f"--project={PROJECT_ID}",
                f"--region={REGION}",
                f"--args={gcs_uri}",
                "--wait",
                "--quiet",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        if result.stdout.strip():
            print(result.stdout.strip(), flush=True)

        if result.stderr.strip():
            print(result.stderr.strip(), flush=True)

        result.check_returncode()

        print(
            "Cloud Run BigQuery MERGE completed successfully!",
            flush=True,
        )

    # ========================================================
    # TASK 3: EXECUTE DBT BUILD IN CLOUD RUN
    # ========================================================

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    def execute_cloud_dbt():

        print(
            "Starting dbt build in Cloud Run...",
            flush=True,
        )

        result = subprocess.run(
            [
                "gcloud",
                "run",
                "jobs",
                "execute",
                CLOUD_RUN_DBT_JOB,
                f"--project={PROJECT_ID}",
                f"--region={REGION}",
                "--wait",
                "--quiet",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        if result.stdout.strip():
            print(result.stdout.strip(), flush=True)

        if result.stderr.strip():
            print(result.stderr.strip(), flush=True)

        result.check_returncode()

        print(
            "Cloud Run dbt build completed successfully!",
            flush=True,
        )

    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    # 1. Extract and upload data using Cloud Run
    gcs_uri = execute_cloud_run()

    # 2. Execute incremental BigQuery MERGE using Cloud Run
    merge_task = execute_cloud_merge(gcs_uri)

    # 3. Execute dbt transformations and tests
    merge_task >> execute_cloud_dbt()

# ============================================================
# REGISTER DAG
# ============================================================

chile_road_safety_pipeline()