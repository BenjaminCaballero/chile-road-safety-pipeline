# Chile Road Safety Data Pipeline

A batch data-engineering **portfolio and learning project** that explores year-over-year trends in **reported crashes (*siniestros*) by vehicle type** in Chile. It ingests public annual statistics presented by CONASET through Observatorio Logístico, validates and stores source records, and builds the analytical model `mart_vehicle_trends` with dbt.

**Reported crashes (*siniestros*)** is the term used throughout this documentation for the source field `siniestros` and its renamed SQL field `crashes`. Records describe annual vehicle-type aggregates, **not individual crash events**.

| Path | Stack | Status |
| --- | --- | --- |
| **Google Cloud** | Docker, Artifact Registry, Cloud Run Jobs, Cloud Storage, BigQuery, dbt, Apache Airflow | End-to-end execution verified |
| **Local development** | Python, Docker Compose, PostgreSQL, dbt | End-to-end execution verified |

> **Deployment scope:** All three cloud processing stages run in Google Cloud, but **Airflow runs on local Ubuntu**, its DAG is **manually triggered** (`schedule=None`), and **Cloud Composer has not been deployed**. The local PostgreSQL implementation is preserved as a separate execution path. This is a verified portfolio deployment, not a production service.

For the cloud architecture, deployed-resource reference, verification evidence, and operations runbook, see **[Cloud Pipeline — Architecture and Operations](docs/CLOUD_PIPELINE.md)**.

## Contents

- [Local quick start](#local-quick-start)
- [1. Local architecture](#1-local-architecture)
- [2. Technology stack](#2-technology-stack)
- [3. Data source](#3-data-source)
- [4. Pipeline components](#4-pipeline-components)
- [5. Project structure](#5-project-structure)
- [6. Local setup notes](#6-local-setup-notes)
- [7. Testing](#7-testing)
- [8. Example analytical queries](#8-example-analytical-queries)
- [9. Engineering decisions](#9-engineering-decisions)
- [10. Data limitations](#10-data-limitations)
- [11. License and data attribution](#11-license-and-data-attribution)
- [12. Project status and next steps](#12-project-status-and-next-steps)

## Local quick start

**Prerequisites:** Ubuntu Linux, Python 3.12, Docker Engine with the daemon running, Docker Compose V2, and Git. The project was developed on Ubuntu; a fresh-clone installation has **not** been independently verified.

> `scripts/bootstrap.sh` invokes **`sudo docker compose`**; it may request your system password. Docker group membership grants root-equivalent privileges and is not required for the script.

```bash
# Clone the repository. Select the feature branch for the cloud-enabled code.
git clone --branch feature/gcp-airflow \
  https://github.com/BenjaminCaballero/chile-road-safety-pipeline.git
cd chile-road-safety-pipeline

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
# Edit .env and set POSTGRES_PASSWORD for your local database.
bash scripts/bootstrap.sh
dotenv run -- python -m src.pipeline
```

The local runner calls ingestion, transactional PostgreSQL loading, and `dbt build` in sequence. A successful run prints `Pipeline completed successfully!`.

**Google Cloud path:** The repository also includes containerized extraction, BigQuery upsert, and dbt execution. Its documented cloud environment is a **reference deployment relying on the author's existing project, bucket, datasets, IAM bindings, images, and jobs**; cloning the repository does not provision those resources. Follow the linked cloud runbook rather than treating the local setup above as cloud deployment instructions.

## 1. Local architecture

```text
Observatorio Logístico API
          |
Python fetch + validation
          |
JSON snapshot (UUID + SHA-256)
          |
PostgreSQL raw.vehicle_safety + raw.ingestion_runs
          |
dbt: stg_vehicle_safety -> mart_vehicle_trends
          |
     SQL analysis
```

The local PostgreSQL load commits in one transaction. The whole pipeline is **not** atomic: a later dbt failure does not roll back a successful PostgreSQL load.

## 2. Technology stack

| Technology | Purpose | Execution path |
|---|---|---|
| Python 3.12 and Requests | Source API extraction, validation, and snapshot creation | Both |
| PostgreSQL 16 and Psycopg 3 | Transactional relational storage | Local |
| Docker Compose | PostgreSQL development environment | Local |
| Docker | Package extraction, merge, and dbt into container images | Cloud build/deployment |
| Google Artifact Registry | Store and distribute Docker images used by Cloud Run | Cloud |
| Google Cloud Run Jobs | Execute containerized batch steps on demand | Cloud |
| Google Cloud Storage (GCS) | Persist validated JSON snapshots | Cloud |
| Google BigQuery | Store raw and analytical datasets; execute an idempotent upsert (`MERGE`) | Cloud |
| dbt Core 1.12 and dbt-bigquery 1.12 | SQL models and data quality checks | Both (target-specific) |
| Apache Airflow 3 | Orchestrate remote jobs and transfer snapshot URIs via XCom | Orchestration |
| Google Cloud IAM | Assign scoped service-account permissions to each Cloud Run Job | Cloud |
| pytest | Run automated Python tests using fixtures and mocks | Development |
| Git | Source version control | Development |

## 3. Data source

- **Provider:** Observatorio Logístico de Chile
- **Original source:** CONASET (based on crash reports from Carabineros de Chile)
- **Dataset:** *Siniestros de tránsito por tipo de vehículo* (identifier `C015`)
- **Source page:** https://www.observatoriologistico.cl/datos/siniestros-de-transito-por-tipo-de-vehiculo
- **REST API:** `https://www.observatoriologistico.cl/api/v1/datastreams/C015/data`
- **Granularity:** one annual aggregate per year and vehicle category; country-level coverage.

Snapshot-specific counts and dates are recorded only in the cloud guide's **Verification evidence** section; source content can change.

**Original fields:**

| Field | Description |
|---|---|
| `anio` | Year |
| `vehiculo` | Vehicle category |
| `siniestros` | Reported crashes (*siniestros*) |
| `fallecidos` | Fatalities |
| `graves` | Serious injuries |
| `menos_graves` | Less serious injuries |
| `leves` | Minor injuries |

## 4. Pipeline components

### 4.1. Ingestion and validation

`src/ingestion/api_client.py` makes HTTP requests with a timeout, follows API pagination metadata, checks page consistency and HTTP status, and rejects an unexpected total record count.

`src/ingestion/validators.py` rejects empty datasets, missing fields, invalid types, non-positive years, negative counts, empty/whitespace-only vehicle categories, and duplicate `(anio, vehiculo)` keys (case-insensitive after trimming). **Not all validation branches have dedicated unit tests.**

### 4.2. Snapshots and traceability

`src/ingestion/run_ingestion.py` writes a JSON snapshot to `data/raw/` with the following contract:

| Field | Purpose |
|---|---|
| `run_id` | Unique execution identifier (UUID) |
| `source` | Source dataset identifier |
| `extracted_at` | Extraction timestamp (UTC) |
| `row_count` | Number of extracted records |
| `source_hash` | SHA-256 hash of the dataset |
| `data` | Extracted records |

The hash uses a sorted, standardized JSON representation to avoid treating ordering changes as changes in source data. Local snapshots are Git-ignored; cloud runs upload snapshots to GCS.

### 4.3. Local PostgreSQL loading

`src/storage/load_postgres.py` validates the snapshot, checks its count/hash, records metadata in `raw.ingestion_runs`, deletes and reloads `raw.vehicle_safety`, and verifies the final count in a **single database transaction**. The `raw.vehicle_safety` table uses a composite `(anio, vehiculo)` key; an existing `run_id` with inconsistent metadata is rejected.

This is a **transactional full refresh**, not an upsert. Snapshots are retained separately; PostgreSQL stores the current data and ingestion-run metadata.

### 4.4. BigQuery loaders: different responsibilities

- `src/storage/load_bigquery.py` is a **standalone full-replacement loader** using `WRITE_TRUNCATE`. Its mocked tests cover that module; it is **not** the loader called by the current cloud DAG.
- `src/storage/merge_bigquery.py` is the **current cloud upsert**. It reads GCS/local snapshots, stages validated rows, and conditionally updates changed matching keys or inserts missing keys with `MERGE`. The code does not implement source-side deletion propagation. Its SQL is **not covered by dedicated automated tests**.

See the cloud operations guide for staging, rerun, and failure behavior.

### 4.5. SQL transformations with dbt

`dbt/models/staging/stg_vehicle_safety.sql` produces standardized field names and retains `run_id`. The final model is `dbt/models/marts/mart_vehicle_trends.sql`; both are configured as views (PostgreSQL schema `analytics`, BigQuery dataset `road_safety_analytics`).

**Staging field mappings:**

| Original column | Staging column |
|---|---|
| `anio` | `year` |
| `vehiculo` | `vehicle_type` |
| `siniestros` | `crashes` |
| `fallecidos` | `fatalities` |
| `graves` | `serious_injuries` |
| `menos_graves` | `less_serious_injuries` |
| `leves` | `minor_injuries` |

**Actual columns of `mart_vehicle_trends`** (in SQL `SELECT` order):

| Column | Meaning |
|---|---|
| `year` | Observation year |
| `vehicle_type` | Normalized vehicle category |
| `crashes` | Reported crashes for that year/category |
| `fatalities` | Reported fatalities for that year/category |
| `previous_year` | Previous observed year from `LAG(year)` |
| `previous_crashes` | Previous observed reported-crash count from `LAG(crashes)` |
| `crashes_yoy_pct` | Percentage change, rounded to two decimals; `NULL` when years are not consecutive or the prior count is zero |

The model uses `LAG()` partitioned by vehicle category and ordered by year. `crashes_yoy_pct` compares **only consecutive years**, avoiding false comparisons across missing years and division by zero.

## 5. Project structure

```text
src/ingestion/             # API, validation, snapshots
src/storage/               # PostgreSQL, BigQuery full load, MERGE, GCS
src/cloud/extract_upload.py
src/pipeline.py            # Local three-step runner
dbt/models/                # Staging and analytical SQL
dbt/tests/                 # Custom uniqueness test
dbt/profiles.yml           # Local PostgreSQL/BigQuery targets
airflow/dags/chile_road_safety.py
deploy/dbt/profiles.yml    # BigQuery profile for container
test/test_validators.py
test/test_load_bigquery.py
test/test_gcs.py
Dockerfile                 # Extraction image
Dockerfile.merge           # BigQuery MERGE image
Dockerfile.dbt             # dbt image
requirements.txt
requirements-cloud.txt
requirements-merge.txt
requirements-dbt.txt
compose.yaml
sql/01_schema.sql
scripts/bootstrap.sh
.env.example
README.md
```

The cloud guide is delivered as `docs/CLOUD_PIPELINE.md` alongside this README. Generated data, virtual environments, credentials, logs, and dbt build artifacts should not be committed.

## 6. Local setup notes

- `.env.example` contains `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, and `POSTGRES_PORT` for the local Compose/PostgreSQL path. Copy it to `.env`, then set a local password. `.env` is Git-ignored.
- The local database is reached at `127.0.0.1:5433` by the Python loader and dbt profile.
- `scripts/bootstrap.sh` starts Compose with `sudo docker compose up -d --wait` and applies `sql/01_schema.sql` with `psql`.
- Docker group membership grants root-equivalent privileges. Follow the Docker project's security guidance if changing group membership.
- A fresh-clone/local-database bootstrap has not been independently verified; no claim of turnkey installation is made here.

## 7. Testing

```bash
source .venv/bin/activate
python -m pytest -q
# Builds the local PostgreSQL views and executes dbt data tests:
dotenv run -- dbt build --project-dir dbt --profiles-dir dbt
```

**Test coverage (verified from `test/`):**

| Tests | Covered behavior |
|---|---|
| `test_validators.py` | Valid record, negative `siniestros`, empty dataset, missing field, duplicate key, wrong numeric type |
| `test_load_bigquery.py` | Full `WRITE_TRUNCATE` load with mocked client, corrupted snapshot hash, mismatch between expected and **loaded table** row count |
| `test_gcs.py` | Mocked GCS download, create-only upload, existing-object metadata match/mismatch |

The code checks for empty vehicle categories, **but there is no dedicated automated test for that branch**. The suite has no dedicated automated tests for `api_client.py`, `load_postgres.py`, `merge_bigquery.py` SQL/upsert, `extract_upload.py`, or the Airflow DAG. Mocked cloud tests are **not** live cloud integration tests.

**Local verification:** repeated local loads preserved unique `(anio, vehiculo)` records; a manual `ROLLBACK` exercise confirmed PostgreSQL transaction recovery. This was a manual observation, not an automated fault-injection test.

For the recorded test counts, cloud execution logs, GCS verification, and upsert observations, see **Verification evidence** in the cloud guide.

## 8. Example analytical queries

Run locally using `sudo docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'`.

```sql
-- Count analytical rows
SELECT COUNT(*) FROM analytics.mart_vehicle_trends;

-- Vehicle-type trend (reported crashes)
SELECT year, vehicle_type, crashes, crashes_yoy_pct
FROM analytics.mart_vehicle_trends
WHERE vehicle_type = 'Automóvil'
ORDER BY year DESC
LIMIT 3;

-- Largest calculated year-over-year increases
SELECT year, vehicle_type, crashes, crashes_yoy_pct
FROM analytics.mart_vehicle_trends
WHERE crashes_yoy_pct IS NOT NULL
ORDER BY crashes_yoy_pct DESC
LIMIT 10;
```

Interpret growth rates together with the underlying counts. Category counts must not automatically be summed as distinct nationwide crash events.

## 9. Engineering decisions

**Local PostgreSQL:** Docker Compose provides a small local database; a transactional full refresh is straightforward for this source size. It does not depend on cloud provisioning.

**dbt and separate SQL models:** The staging view standardizes names; the analytical mart computes consecutive-year comparisons and tests basic non-null/uniqueness properties.

**JSON snapshots and hashing:** Snapshot metadata provides a way to compare extracted datasets and investigate specific runs; it does not constitute a complete audit/history system.

**Cloud Run Jobs + Artifact Registry:** Three purpose-built images run finite batch processes without a continuously running application server. Artifact Registry stores images, not the data snapshots.

**Cloud Storage:** A common storage location makes validated snapshots accessible between independent jobs. Uploads use a create-only precondition, not a bucket-enforced immutability guarantee.

**BigQuery `MERGE`:** An idempotent upsert avoids rewriting unchanged rows on an identical snapshot, while leaving source deletions unsynchronized.

**Airflow and scoped IAM:** Airflow supplies ordering and the expected GCS URI; dedicated service identities limit cloud resource access. The detailed operational and permission model belongs in the cloud guide.

## 10. Data limitations

The dataset and pipeline implementation have several important limitations.

**Dataset limitations:**

- **Aggregated data:** Each record represents annual statistics for a vehicle category, not an individual accident.
- **No detailed geography:** The dataset cannot support analysis by municipality, accident location, or road segment.
- **Incomplete category coverage:** Some vehicle categories are absent in certain years, and the reasons have not been established.
- **Potential category overlap:** Vehicle categories have not been confirmed as mutually exclusive. Summing their incident counts does not necessarily represent the total number of unique accidents.
- **No causal interpretation:** Changes in reported incidents cannot be attributed to specific causes using this dataset alone.

**Pipeline limitations:**

- **Small dataset:** The project demonstrates batch data engineering practices, not large-scale or distributed processing.
- **Full-source extraction:** Each ingestion run retrieves the complete dataset from the source API. Upsert processing occurs at the BigQuery loading stage, not at the API extraction stage.
- **No deletion synchronization:** The BigQuery `MERGE` operation inserts new records and updates changed records, but does not automatically delete records that disappear from the source dataset.
- **Batch processing:** The pipeline is designed for periodic batch execution rather than real-time or streaming data ingestion.

These limitations should be considered when interpreting analytical results or extending the pipeline to larger datasets and production workloads.

## 11. License and data attribution

### Code license

The source code is released under the MIT License. See [LICENSE](LICENSE) for details.

### Data attribution

The project uses publicly available road safety statistics provided by **Observatorio Logístico de Chile**, originating from CONASET.

Source: https://www.observatoriologistico.cl/datos/siniestros-de-transito-por-tipo-de-vehiculo

The MIT License applies to this repository's code, **not** to the source data. Downloaded datasets and generated snapshots are excluded from the repository. The source page identifies CONASET as the responsible entity but does not state a dataset-specific open-data license. CONASET publishes [website-content usage terms](https://conaset.cl/politica-de-privacidad/); these must not be assumed to grant unrestricted redistribution rights for this dataset. A dataset-specific license permitting redistribution has not been confirmed. Users should review the data provider's terms before redistributing the source data.

## 12. Project status and next steps

**Status:** The local PostgreSQL route and the cloud-processing route were both exercised successfully. The source code, Dockerfiles, dbt models, and orchestration DAG are present; this is not a continuously scheduled or completely reproducible fresh-project deployment.

- [ ] Verify bootstrap and dbt locally from a clean clone and empty database.
- [ ] Add automated tests for currently untested modules and failure scenarios.
- [ ] Pin/document the Airflow development environment and its installation.
- [ ] Add CI, deployment automation, alerting, and backup/retention policies where appropriate.
- [ ] Evaluate Infrastructure as Code and managed orchestration only if their operational benefit justifies the additional cost and complexity.
