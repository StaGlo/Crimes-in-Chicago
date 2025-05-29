#!/usr/bin/env bash
set -euo pipefail

# Configuration (override as needed)
IMAGE="postgres:13"
CONTAINER_NAME="pg-crimes"
DB_NAME="crimes"
DB_USER="postgres"
DB_PASS="changeme"
TABLE_NAME="crime_aggregates"

# --- Pull the Docker image ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Pulling Docker image ${IMAGE}..."
docker pull "${IMAGE}"

# --- Check if the container is already running ---
if ! docker ps --format '{{.Names}}' | grep -qw "${CONTAINER_NAME}"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Starting container ${CONTAINER_NAME}..."
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -e POSTGRES_USER="${DB_USER}" \
        -e POSTGRES_PASSWORD="${DB_PASS}" \
        -e POSTGRES_DB="${DB_NAME}" \
        -p 5432:5432 \
        "${IMAGE}"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') Container ${CONTAINER_NAME} already running."
fi

# --- Wait for PostgreSQL to be ready ---
echo -n "$(date '+%Y-%m-%d %H:%M:%S') Waiting for PostgreSQL to be ready"
until docker exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" &>/dev/null; do
    echo -n "."
    sleep 1
done
echo " ready!"

# --- Drop and recreate the database ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Dropping and recreating database '${DB_NAME}'..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d postgres <<EOF
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME};
EOF

# --- Create the table ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Creating table ${TABLE_NAME} in database ${DB_NAME}..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" <<EOF
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  year_month   VARCHAR(7),
  category     TEXT,
  district     INTEGER,
  total_crimes BIGINT,
  arrests      BIGINT,
  domestics    BIGINT,
  fbi_indexed  BIGINT,
  PRIMARY KEY (year_month, category, district)
);
EOF

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') PostgreSQL setup complete."
