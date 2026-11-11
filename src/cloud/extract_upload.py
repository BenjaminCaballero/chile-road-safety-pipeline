from src.ingestion.run_ingestion import main
from src.storage.upload_gcs import upload_snapshot_gcs


def main_cloud():

    # 1. Extract and validate data
    snapshot_path = main()

    # 2. Upload snapshot to Cloud Storage
    gcs_uri = upload_snapshot_gcs(str(snapshot_path))

    print(f"Cloud extraction completed: {gcs_uri}")

    return gcs_uri


if __name__ == "__main__":
    main_cloud()