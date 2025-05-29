#!/usr/bin/env bash
set -euo pipefail

# --- Kafka settings ---
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"

# --- HDFS directories ---
HDFS_CHECKPOINTS="/streaming/checkpoints"
HDFS_STATIC_FILE="/streaming/static/IUCR_codes.csv"

# --- Run Spark job as YARN application ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Launching Spark streaming job..."
spark-submit \
    --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    scripts/3-processing_app.py \
    --bootstrap-servers "$BOOTSTRAP_SERVER" \
    --input-topic "$TOPIC_NAME" \
    --static-file "$HDFS_STATIC_FILE" \
    --checkpoint-location "$HDFS_CHECKPOINTS" \
    --delay A

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished processing data."
