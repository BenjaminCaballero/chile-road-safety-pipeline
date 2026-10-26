#!/usr/bin/env bash

set -euo pipefail

# Ir a la raíz del proyecto
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "Starting PostgreSQL..."

sudo docker compose up -d --wait

echo "Initializing database schema..."

sudo docker compose exec -T postgres sh -c \
'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
< sql/01_schema.sql

echo "Database initialized successfully!"
