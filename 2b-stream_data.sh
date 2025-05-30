#!/usr/bin/env bash
set -euo pipefail

# --- Kafka settings ---
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"

# --- Local download directory ---
DOWNLOAD_DIR="/tmp/crime_data"

# --- Stream data into Kafka ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Starting to produce records to Kafka topic '${TOPIC_NAME}'"
for csvfile in "${DOWNLOAD_DIR}/crimes/crimes-in-chicago_result"/*.csv; do
    echo "  Processing file: ${csvfile}"
    # Skip header line, then send each line as a string to Kafka
    tail -n +2 "${csvfile}" | while IFS= read -r line; do
        echo "$line" | kafka-console-producer.sh \
            --broker-list "${BOOTSTRAP_SERVER}" \
            --topic "${TOPIC_NAME}" \
            --property "parse.key=false" \
            --property "parse.headers=false" \
            --property "key.separator=:"

        # Sleep ?
        # sleep 1

    done
done

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished producing all records."
