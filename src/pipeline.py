import subprocess

from pathlib import Path

from src.ingestion.run_ingestion import main as run_ingestion
from src.storage.load_postgres import load_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def main():

    print("Starting data pipeline...")

    # 1. Extracción, validación y snapshot
    print("\n[1/3] Running ingestion...")

    snapshot_path = run_ingestion()

    # 2. Carga a PostgreSQL
    print("\n[2/3] Loading PostgreSQL...")

    load_snapshot(str(snapshot_path))

    # 3. Transformaciones y pruebas dbt
    print("\n[3/3] Running dbt...")

    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        cwd=PROJECT_ROOT,
        check=True
    )

    print("\nPipeline completed successfully!")


if __name__ == "__main__":
    main()