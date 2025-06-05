#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
CONTAINER_NAME="pg-crimes" # Docker container running Postgres
DB_NAME="crimes"           # Database name
DB_USER="postgres"         # Database user
TABLE="crime_aggregates"   # Table to query

echo "$(date '+%Y-%m-%d %H:%M:%S') Connecting to Postgres container '${CONTAINER_NAME}'..."
echo "Displaying all rows from '${TABLE}' in database '${DB_NAME}':"
echo

# --- Execute the SELECT query inside the container ---
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" <<EOF
SELECT * FROM ${TABLE};
EOF

docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" <<EOF
SELECT * FROM crime_anomalies;
EOF

# --- Log completion ---
echo
echo "$(date '+%Y-%m-%d %H:%M:%S') Query complete."
