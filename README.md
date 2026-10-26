# Chile Road Safety Data Pipeline

An end-to-end batch data engineering project that extracts, validates, stores, and transforms public road safety statistics from Chile using **Python, PostgreSQL, dbt, and Docker**.

The project demonstrates practical data engineering concepts, including REST API ingestion, data validation, ingestion traceability, transactional and idempotent loading, SQL transformations, and automated data quality testing.

The entire pipeline can be executed with a single command, from the source API to analytical models in PostgreSQL.

> **Note:** Python and dbt run on the host machine. Docker Compose is used to run PostgreSQL.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/<YOUR_USERNAME>/chile-road-safety-pipeline.git
cd chile-road-safety-pipeline

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
python -m pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env and set POSTGRES_PASSWORD

# Start PostgreSQL and initialize the database
bash scripts/bootstrap.sh

# Execute the complete pipeline
dotenv run -- python -m src.pipeline
```

If all stages succeed, the pipeline prints:

```text
Pipeline completed successfully!
```

Prerequisites, configuration details, and Docker permission notes are in [Setup Notes](#6-setup-notes). A complete installation from a fresh clone and an empty database has not yet been independently verified.

---

## 1. Architecture

```text
         Observatorio Logístico
              (REST API)
                   |
                   v
           Python Extraction
          (HTTP + Pagination)
                   |
                   v
            Data Validation
                   |
                   v
             JSON Snapshot
          (UUID + SHA-256)
                   |
                   v
               PostgreSQL
               raw schema
                   |
          +--------+--------+
          |                 |
          v                 v
   vehicle_safety      ingestion_runs
          |
          v
        dbt Core
          |
          v
   stg_vehicle_safety
          |
          v
   mart_vehicle_trends
          |
          v
     Analytical Queries
    (Year-over-Year Trends)
```

**Execution flow:**

1. Extract all available records from the REST API.
2. Validate the extracted dataset.
3. Save a JSON snapshot with execution metadata.
4. Load the validated records into PostgreSQL.
5. Execute dbt transformations and data quality tests.

The pipeline follows a **fail-fast approach**: if a stage fails, subsequent stages are not executed.

Database loading is transactional, but the entire pipeline is not atomic. If dbt fails after the PostgreSQL transaction commits, the validated data remains stored in the database.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Data ingestion, validation, and pipeline execution |
| Requests | HTTP communication with the REST API |
| PostgreSQL 16 | Relational data storage |
| Psycopg 3 | Python–PostgreSQL integration |
| dbt Core 1.12 | SQL transformations and data quality tests |
| Docker Compose | Local PostgreSQL environment |
| pytest | Automated Python unit tests |
| Git | Version control |

## 3. Data Source

- **Provider:** Observatorio Logístico de Chile
- **Original source:** CONASET
- **Dataset:** Siniestros de tránsito por tipo de vehículo

- **Dataset:** https://www.observatoriologistico.cl/datos/siniestros-de-transito-por-tipo-de-vehiculo
- **REST API:** `https://www.observatoriologistico.cl/api/v1/datastreams/C015/data`

The dataset contains **aggregated annual road traffic statistics by vehicle type**, not individual accident records.

### Dataset Characteristics

The following characteristics were observed during development in October 2026.

| Characteristic | Value |
|---|---|
| Records | 293 |
| Fields | 7 |
| Period | 2002–2025 |
| Years covered | 24 |
| Vehicle categories observed | 15 |
| Geographic coverage | Chile, national level |
| Granularity | One record per year and vehicle type |

These values may change when the source is updated.

**Original fields:**

| Field | Description |
|---|---|
| `anio` | Year |
| `vehiculo` | Vehicle category |
| `siniestros` | Reported traffic incidents |
| `fallecidos` | Fatalities |
| `graves` | Serious injuries |
| `menos_graves` | Less serious injuries |
| `leves` | Minor injuries |

## 4. Pipeline Components

### 4.1. Data Ingestion

**File:** `src/ingestion/api_client.py`

The ingestion module retrieves records from the Observatorio Logístico REST API.

Features include:

- HTTP GET requests and JSON response parsing.
- Dynamic pagination using API metadata.
- HTTP error detection.
- Pagination consistency checks.
- Verification that the number of retrieved records matches the total reported by the API.

If a request fails or the retrieved record count is inconsistent, the extraction raises an error rather than returning an incomplete dataset.

### 4.2. Data Validation

**File:** `src/ingestion/validators.py`

Before database loading, the pipeline validates:

- Presence of required fields.
- Expected data types.
- Non-negative numeric counts.
- Non-empty vehicle categories.
- Uniqueness of year–vehicle combinations.
- Non-empty datasets.

Invalid datasets are rejected before the loading stage.

### 4.3. Snapshots and Traceability

**File:** `src/ingestion/run_ingestion.py`

Validated records are saved as JSON snapshots containing the extracted data and execution metadata.

| Field | Purpose |
|---|---|
| `run_id` | Unique execution identifier (UUID) |
| `source` | Source dataset identifier |
| `extracted_at` | Extraction timestamp (UTC) |
| `row_count` | Number of extracted records |
| `source_hash` | SHA-256 hash of the dataset |
| `data` | Extracted records |

Before hashing, records are sorted consistently and serialized into a standardized JSON representation.

This allows the pipeline to detect changes in dataset content without treating differences in record ordering as changes.

During verified executions, repeated extractions produced the same SHA-256 hash for the 293 records.

Snapshots are stored in `data/raw/` and excluded from Git.

### 4.4. PostgreSQL Storage

**File:** `src/storage/load_postgres.py`

The project uses a single PostgreSQL database with two ingestion tables.

**`raw.vehicle_safety`**

Stores the currently loaded dataset, including incident and casualty metrics, the associated execution identifier, and a composite primary key on year and vehicle type.

**`raw.ingestion_runs`**

Stores ingestion metadata, including execution UUID, source identifier, timestamps, record count, and source hash.

#### Load Strategy: Full Refresh

Each successful load replaces the current dataset in `raw.vehicle_safety`.

This approach was chosen because:

- The source API exposes the complete available series.
- The dataset is small.
- A full refresh simplifies consistency and recovery.

Previous snapshots remain available locally as JSON files. The database stores the current dataset and the ingestion execution history, not complete historical copies of every dataset version.

#### Transactional Loading

The loading process:

1. Reads and validates the JSON snapshot.
2. Verifies its record count and source hash.
3. Registers the ingestion execution.
4. Replaces the current dataset.
5. Verifies the number of loaded records.
6. Commits the transaction if all operations succeed.

If a database operation raises an error, the transaction is rolled back.

#### Idempotency

The loading strategy prevents duplicate records in the current dataset.

- The composite primary key enforces uniqueness on year–vehicle combinations.
- Re-loading the same snapshot does not duplicate its execution record.
- Running the full pipeline again generates a new `run_id`, even when the source data remains unchanged.
- Identical source hashes indicate identical content under the project's standardized representation.

These behaviors were verified through repeated executions.

### 4.5. SQL Transformations with dbt

**Directory:** `dbt/`

dbt separates analytical SQL transformations from Python ingestion logic.

Both models are materialized as PostgreSQL views.

#### `stg_vehicle_safety`

**File:** `dbt/models/staging/stg_vehicle_safety.sql`

Standardizes source column names and prepares records for analytical transformations.

| Original column | Staging column |
|---|---|
| `anio` | `year` |
| `vehiculo` | `vehicle_type` |
| `siniestros` | `crashes` |
| `fallecidos` | `fatalities` |
| `graves` | `serious_injuries` |
| `menos_graves` | `less_serious_injuries` |
| `leves` | `minor_injuries` |

#### `mart_vehicle_trends`

**File:** `dbt/models/marts/mart_vehicle_trends.sql`

Calculates year-over-year changes in reported incidents by vehicle type.

The model uses the SQL `LAG()` window function to retrieve previous observations within each vehicle category.

The percentage change is calculated as:

```text
((current_crashes - previous_crashes) / previous_crashes) * 100
```

The calculation is performed only when:

- The previous observation belongs to the immediately preceding year.
- The previous incident count is greater than zero.

Otherwise, the result is `NULL`.

This avoids comparisons across non-consecutive years and division by zero.

The final analytical model is available as:

`analytics.mart_vehicle_trends`

## 5. Project Structure

```text
chile-road-safety-pipeline/
│
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── api_client.py
│   │   ├── validators.py
│   │   └── run_ingestion.py
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   └── load_postgres.py
│   │
│   └── pipeline.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml
│   │   │   └── stg_vehicle_safety.sql
│   │   ├── marts/
│   │   │   └── mart_vehicle_trends.sql
│   │   └── schema.yml
│   └── tests/
│       └── assert_unique_year_vehicle.sql
│
├── sql/
│   └── 01_schema.sql
│
├── scripts/
│   └── bootstrap.sh
│
├── test/
│   └── test_validators.py
│
├── data/
│   └── raw/
│
├── compose.yaml
├── requirements.txt
├── .env.example
├── .gitignore
├── LICENSE
└── README.md
```

## 6. Setup Notes

### Prerequisites

The project was developed and tested with:

- Ubuntu Linux
- Python 3.12
- Docker Engine
- Docker Compose V2
- Git

Docker must be installed and running before initializing PostgreSQL.

### Environment Variables

Copy `.env.example` to `.env` and configure:

```dotenv
POSTGRES_USER=road_safety
POSTGRES_PASSWORD=your_local_password
POSTGRES_DB=road_safety
POSTGRES_PORT=5433
```

Replace `your_local_password` with a password for your local environment.

The `.env` file is excluded from version control.

Python connects to PostgreSQL through `127.0.0.1:5433`.

The `dotenv` command is provided by `python-dotenv`. It loads variables from `.env` when running the pipeline.

### Database Initialization

The `scripts/bootstrap.sh` script:

1. Starts PostgreSQL with Docker Compose.
2. Waits until the container is healthy.
3. Executes `sql/01_schema.sql`.

The script can be executed again without recreating existing tables.

### Docker Permissions

The bootstrap script and the `psql` example in [Section 8](#8-example-analytical-queries) use `sudo` to access Docker. Docker's Linux post-installation documentation describes how to configure non-root access, although membership in the `docker` group grants root-equivalent privileges. Use it only on a personal or development machine.

## 7. Testing

### 7.1. Python Unit Tests

Run:

```bash
python -m pytest -q
```

Six unit tests verify:

1. Acceptance of valid records.
2. Rejection of negative incident counts.
3. Rejection of empty datasets.
4. Detection of missing required fields.
5. Detection of duplicate business keys.
6. Rejection of incorrect data types.

**Verified result:** `6 passed`

These tests use controlled sample data and do not require the external API.

### 7.2. dbt Tests

Run:

```bash
dotenv run -- dbt build --project-dir dbt --profiles-dir dbt
```

The dbt project includes:

- 2 SQL models.
- 6 `not_null` tests.
- 1 custom uniqueness test.

The custom test verifies that the analytical mart contains at most one record per year and vehicle category.

**Verified result:**

```text
PASS=9 WARN=0 ERROR=0 TOTAL=9
```

This corresponds to two successfully built models and seven successful data tests.

### 7.3. Idempotency Verification

The pipeline was executed repeatedly against the same source dataset.

| Metric | Verified result |
|---|---:|
| Records in `raw.vehicle_safety` | 293 |
| Unique year–vehicle combinations | 293 |
| Records in `analytics.mart_vehicle_trends` | 293 |

Different pipeline executions generated different UUIDs while preserving the same source hash when the dataset remained unchanged.

Re-loading the same snapshot did not duplicate its execution record.

### 7.4. Transaction Rollback Verification

PostgreSQL rollback behavior was verified manually using a controlled transaction.

The test:

1. Started a transaction.
2. Deleted the 293 records from `raw.vehicle_safety`.
3. Triggered an intentional SQL error.
4. Executed `ROLLBACK`.
5. Confirmed that all 293 records remained available.

This verifies PostgreSQL's transaction rollback behavior.

It does not replace an automated failure-injection test of the Python loading module.

## 8. Example Analytical Queries

Connect to PostgreSQL:

```bash
sudo docker compose exec postgres sh -c \
'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

### Query 1: Verify the Analytical Dataset

```sql
SELECT COUNT(*)
FROM analytics.mart_vehicle_trends;
```

**Verified result:** `293`

### Query 2: Automobile Incident Trends

```sql
SELECT
    year,
    vehicle_type,
    crashes,
    crashes_yoy_pct
FROM analytics.mart_vehicle_trends
WHERE vehicle_type = 'Automóvil'
ORDER BY year DESC
LIMIT 3;
```

Example results from the verified dataset:

| Year | Vehicle type | Crashes | YoY change |
|---|---|---:|---:|
| 2025 | Automóvil | 49,546 | -6.30% |
| 2024 | Automóvil | 52,877 | -7.09% |
| 2023 | Automóvil | 56,915 | -9.48% |

These values represent reported incidents associated with the automobile category.

They should not automatically be interpreted as the number of unique traffic accidents nationwide.

### Query 3: Largest Annual Increases

```sql
SELECT
    year,
    vehicle_type,
    crashes,
    crashes_yoy_pct
FROM analytics.mart_vehicle_trends
WHERE crashes_yoy_pct IS NOT NULL
ORDER BY crashes_yoy_pct DESC
LIMIT 10;
```

This query identifies the largest year-over-year percentage increases in the available data.

Results should be interpreted alongside the underlying counts, since large percentage changes may occur when the previous count is small.

## 9. Engineering Decisions

**PostgreSQL:** A reliable relational database that runs locally through Docker without requiring cloud infrastructure. For a dataset containing a few hundred records, distributed processing would introduce unnecessary complexity.

**dbt:** Separates analytical transformations from Python ingestion logic and provides SQL model dependencies and automated data quality tests.

**JSON snapshots:** Preserve validated source records and execution metadata, supporting debugging, auditing, and reproduction of individual ingestions.

**Transactional full-refresh loading:** Replaces the current dataset within a single database transaction, avoiding partially committed updates.

**Python pipeline runner:** Coordinates ingestion, storage, and dbt execution without introducing unnecessary orchestration infrastructure. The project targets command-based batch processing rather than scheduled or continuous execution.

## 10. Data Limitations

The dataset and implementation have several important limitations.

- **Aggregated data:** Each record represents annual statistics for a vehicle category, not an individual accident.
- **No detailed geography:** The dataset cannot support analysis by municipality, accident location, or road segment.
- **Incomplete category coverage:** Some vehicle categories are absent in certain years, and the reasons have not been established.
- **Potential category overlap:** Vehicle categories have not been confirmed as mutually exclusive. Summing their incident counts does not necessarily represent the total number of unique accidents.
- **No causal interpretation:** Changes in reported incidents cannot be attributed to specific causes using this dataset alone.
- **Small dataset:** The project demonstrates batch data engineering practices, not large-scale or distributed processing.
- **Local execution:** The pipeline does not currently include recurring scheduling or cloud deployment.

## 11. License and Data Attribution

### Code License

The source code is released under the MIT License. See the [LICENSE](LICENSE) file for details.

### Data Attribution

The project uses publicly available road safety statistics provided by **Observatorio Logístico de Chile**, originating from CONASET.

Source:

https://www.observatoriologistico.cl/datos/siniestros-de-transito-por-tipo-de-vehiculo

The MIT License applies to the code in this repository, not to the source data. Downloaded datasets and generated snapshots are excluded from the repository. Users should review the provider's applicable terms before redistributing the source data.

## 12. Project Status and Future Improvements

**MVP: Functionally implemented and verified in the development environment.**

Verified components:

- REST API ingestion with dynamic pagination.
- Python data validation.
- JSON snapshots with UUID and SHA-256 traceability.
- Transactional PostgreSQL loading.
- Basic idempotency verification.
- SQL transformations using dbt.
- Automated Python and dbt tests.
- End-to-end execution through a single command.
- PostgreSQL initialization using Docker Compose.
- Manual PostgreSQL rollback verification.

### Future Improvements

- [ ] Verify the complete setup from a fresh clone and empty PostgreSQL environment.
- [ ] Add GitHub Actions for automated testing.
- [ ] Add integration tests for database loading failures.

The current implementation prioritizes **correctness and maintainability** over unnecessary infrastructure complexity. Reproducibility from a clean environment is the next item to verify.