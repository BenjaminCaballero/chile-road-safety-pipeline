# Chile Road Safety Data Pipeline

An end-to-end data engineering project that extracts, validates, stores, and transforms public road safety statistics from Chile using Python, PostgreSQL, dbt, and Docker.

The project focuses on building a reproducible and reliable batch data pipeline, with data quality checks, ingestion traceability, idempotent loading, and SQL-based analytical transformations.

## Architecture

```text
Observatorio Logístico (REST API)
              |
              v
      Python Extraction
       (Pagination)
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
              v
           dbt Core
              |
       +------+------+
       |             |
       v             v
    Staging      Analytical
     Model          Mart
                       |
                       v
               Year-over-Year
                Trend Analysis
```

The pipeline is executed through a Python entry point that coordinates ingestion, PostgreSQL loading, and dbt transformations.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | API ingestion, validation, and pipeline execution |
| Requests | HTTP communication with the source API |
| PostgreSQL 16 | Relational data storage |
| Psycopg 3 | Python–PostgreSQL integration |
| dbt Core | SQL transformations and data quality tests |
| Docker Compose | Reproducible PostgreSQL environment |
| pytest | Automated Python unit tests |
| Git | Version control |

## Data Source

**Provider:** Observatorio Logístico de Chile  
**Original source:** CONASET  
**Dataset:** Siniestros de tránsito por tipo de vehículo

Source:

https://www.observatoriologistico.cl/datos/siniestros-de-transito-por-tipo-de-vehiculo

The dataset contains annual statistics of road traffic incidents grouped by vehicle type.

During development in October 2026, the API returned:

- 293 records.
- 7 fields per record.
- Years 2002–2025.
- 15 observed vehicle categories.

These values describe the dataset observed during development and may change when the source is updated.

The records are aggregated statistics, not individual accident records.

## Pipeline Components

### 1. Data Ingestion

The ingestion module retrieves data from the Observatorio Logístico REST API.

It supports paginated extraction and verifies that the total number of retrieved records matches the count reported by the API.

HTTP errors and inconsistent pagination metadata cause the extraction to fail.

### 2. Data Validation

Before loading records into PostgreSQL, the pipeline validates:

- Required fields.
- Expected data types.
- Non-negative numeric counts.
- Non-empty vehicle categories.
- Uniqueness of the year–vehicle combination.
- Non-empty datasets.

Invalid datasets are rejected rather than partially loaded.

### 3. Snapshots and Traceability

Validated records are saved as JSON snapshots with metadata including:

- `run_id`: unique execution identifier.
- `extracted_at`: extraction timestamp in UTC.
- `source_hash`: SHA-256 hash of the canonicalized records.
- `row_count`: number of extracted records.
- `source`: source dataset identifier.

Snapshots are stored locally in `data/raw/` and excluded from Git.

### 4. PostgreSQL Storage

The project uses a single PostgreSQL database with two ingestion tables.

**`raw.vehicle_safety`**

Stores the currently loaded dataset, with a composite primary key based on year and vehicle type.

**`raw.ingestion_runs`**

Stores execution metadata, including the source hash, extraction timestamp, record count, and loading timestamp.

Data loading uses PostgreSQL transactions and replaces the current dataset with a validated snapshot.

Repeated loading of the same snapshot does not create duplicate records.

### 5. dbt Transformations

The project contains two analytical models.

**`stg_vehicle_safety`**

Standardizes column names and prepares the source records for analytical transformations.

**`mart_vehicle_trends`**

Calculates year-over-year changes in reported incidents by vehicle type using SQL window functions.

The year-over-year calculation returns `NULL` when the previous observation is not from the immediately preceding year or when the previous count is zero.

Both models are currently materialized as PostgreSQL views.

## Getting Started

### Prerequisites

The development environment has been tested with:

- Ubuntu Linux
- Python 3.12
- Docker Engine
- Docker Compose V2
- Git

Python and dbt run on the host machine, while PostgreSQL runs inside Docker.

### 1. Configure the Python environment

From the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 2. Configure environment variables

Create the local configuration file:

```bash
cp .env.example .env
```

Edit `.env` and configure the PostgreSQL connection:

```dotenv
POSTGRES_USER=road_safety
POSTGRES_PASSWORD=your_local_password
POSTGRES_DB=road_safety
POSTGRES_PORT=5433
```

Do not commit `.env` to Git.

### 3. Initialize PostgreSQL

Run:

```bash
bash scripts/bootstrap.sh
```

This starts PostgreSQL through Docker Compose and executes the SQL schema initialization script.

The bootstrap script uses `sudo` for Docker commands.

### 4. Execute the pipeline

Run:

```bash
dotenv run -- python -m src.pipeline
```

This executes:

1. REST API extraction.
2. Data validation.
3. JSON snapshot creation.
4. Transactional PostgreSQL loading.
5. dbt transformations and data tests.

The pipeline reports success only when all stages complete successfully.

## Testing

### Python Unit Tests

Run:

```bash
python -m pytest -q
```

The project currently includes six unit tests covering valid records, missing fields, invalid types, negative values, duplicate keys, and empty datasets.

### dbt Models and Tests

Run:

```bash
dotenv run -- dbt build --project-dir dbt --profiles-dir dbt
```

The dbt project contains two models and seven data tests.

The tests check required values and uniqueness of the year–vehicle combination in the analytical mart.

During the verified development runs:

- Python: 6 tests passed.
- dbt: 7 tests passed.
- dbt models: 2 built successfully.

## Example Analytical Query

Connect to PostgreSQL:

```bash
sudo docker compose exec postgres sh -c \
'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Execute:

```sql
SELECT
    year,
    vehicle_type,
    crashes,
    previous_crashes,
    crashes_yoy_pct
FROM analytics.mart_vehicle_trends
WHERE vehicle_type = 'Automóvil'
ORDER BY year DESC
LIMIT 10;
```

This query retrieves the recent annual statistics and year-over-year percentage changes for the automobile category.

## Reliability and Design Decisions

**Idempotent loading:** Repeated execution does not duplicate the current dataset.

**Transactional loading:** PostgreSQL operations for replacing the dataset are performed within a database transaction.

**Data quality:** Invalid inputs are rejected before modifying database records.

**Traceability:** Execution metadata and content hashes allow individual runs to be identified and compared.

**Separation of responsibilities:** Python handles ingestion and validation; PostgreSQL handles storage; dbt handles analytical transformations.

**Fail-fast execution:** If a stage fails, the pipeline does not proceed to subsequent stages.

The pipeline does not implement a distributed transaction across PostgreSQL and dbt. If dbt fails after the database load commits, the ingested data remains stored.

## Limitations

This is a portfolio project using a relatively small, annually updated dataset.

It is not intended to demonstrate large-scale or distributed data processing.

Additional limitations include:

- Data is aggregated by year and vehicle type.
- Individual accidents cannot be identified.
- No detailed geographic information is available.
- Vehicle categories are not present in every year; the reasons have not been fully established.
- The dataset does not establish that vehicle categories are mutually exclusive, so summing their incident counts should not automatically be interpreted as the number of unique accidents.
- The pipeline supports command-based batch execution but does not currently include a recurring scheduler.
- Full clean-environment reproduction and controlled transaction-failure testing remain to be verified.

## Project Status

The ingestion, validation, PostgreSQL loading, dbt transformations, automated tests, and end-to-end command execution have been implemented and successfully executed in the development environment.

The project is being finalized for reproducible installation and public release.
