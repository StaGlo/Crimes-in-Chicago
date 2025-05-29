#!/usr/bin/env bash
set -euo pipefail

# --- Kafka settings ---
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"

# --- HDFS directories ---
HDFS_CHECKPOINTS="/streaming/checkpoints"
HDFS_STATIC_FILE="/streaming/static/IUCR_codes.csv"

# --- Parse delay argument ---
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 [A|C]"
    exit 1
fi

DELAY_OPTION="$1"
if [[ "$DELAY_OPTION" != "A" && "$DELAY_OPTION" != "C" ]]; then
    echo "Invalid delay: '$DELAY_OPTION'. Must be 'A' or 'C'."
    echo "Usage: $0 [A|C]"
    exit 1
fi

# --- Download used driver ---
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# --- Run Spark job as YARN application ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Launching Spark streaming job..."
echo "Delay configuration: $DELAY_OPTION"

spark-submit \
    --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    3-processing_app.py \
    --bootstrap-servers "$BOOTSTRAP_SERVER" \
    --input-topic "$TOPIC_NAME" \
    --static-file "$HDFS_STATIC_FILE" \
    --checkpoint-location "$HDFS_CHECKPOINTS" \
    --delay "$DELAY_OPTION"

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished processing data."
