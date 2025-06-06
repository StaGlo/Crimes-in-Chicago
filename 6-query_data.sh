#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
CONTAINER_NAME="pg-crimes" # Docker container running Postgres
DB_NAME="crimes"           # Database name
DB_USER="postgres"         # Database user
TABLE="crime_aggregates"   # Tables to query
ANOMALIES_TABLE="crime_anomalies"

echo "$(date '+%Y-%m-%d %H:%M:%S') Connecting to Postgres container '${CONTAINER_NAME}'..."

# --- Query aggregates ---
echo "Displaying all rows from '${TABLE}' in database '${DB_NAME}':"
echo

docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" <<EOF
SELECT * FROM ${TABLE};
EOF

# --- Query anomalies table ---
echo "Displaying all rows from '${ANOMALIES_TABLE}' in database '${DB_NAME}':"
echo

docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" <<EOF
SELECT * FROM ${ANOMALIES_TABLE};
EOF

# --- Log completion ---
echo
echo "$(date '+%Y-%m-%d %H:%M:%S') Query complete."
