-- Crear esquema para datos originales
CREATE SCHEMA IF NOT EXISTS raw;


-- Registro de ejecuciones
CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    source_hash CHAR(64) NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Estadísticas de siniestros por año y vehículo
CREATE TABLE IF NOT EXISTS raw.vehicle_safety (
    anio INTEGER NOT NULL CHECK (anio > 0),
    vehiculo TEXT NOT NULL CHECK (LENGTH(TRIM(vehiculo)) > 0),

    siniestros INTEGER NOT NULL CHECK (siniestros >= 0),
    fallecidos INTEGER NOT NULL CHECK (fallecidos >= 0),
    graves INTEGER NOT NULL CHECK (graves >= 0),
    menos_graves INTEGER NOT NULL CHECK (menos_graves >= 0),
    leves INTEGER NOT NULL CHECK (leves >= 0),

    run_id UUID NOT NULL REFERENCES raw.ingestion_runs(run_id),

    PRIMARY KEY (anio, vehiculo)
);
