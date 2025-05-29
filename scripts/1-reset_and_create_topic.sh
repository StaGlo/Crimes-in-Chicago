#!/usr/bin/env bash
set -euo pipefail

# --- Kafka settings ---
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"

# --- Local download directory ---
DOWNLOAD_DIR="/tmp/crime_data"

# --- HDFS directories ---
HDFS_DIR="/streaming"
HDFS_STATIC_DIR="${HDFS_DIR}/static"

# --- Prepare workspace ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Cleaning and creating download directory at ${DOWNLOAD_DIR}"
rm -rf "${DOWNLOAD_DIR}"
mkdir -p "${DOWNLOAD_DIR}"

# --- Clean HDFS directory ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Cleaning HDFS directory at ${HDFS_DIR}"
hadoop fs -rm -fr "${HDFS_DIR}"

# --- Create HDFS static directory ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Creating HDFS directory for static files at ${HDFS_STATIC_DIR}"
hadoop fs -mkdir -p "${HDFS_STATIC_DIR}"

# --- Delete existing topic (ignore errors if it doesn't exist) ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Deleting topic '${TOPIC_NAME}' if it exists..."
if kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${TOPIC_NAME}" \
  --delete; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') Deletion command succeeded or topic did not exist"
else
  echo "$(date '+%Y-%m-%d %H:%M:%S') Deletion command failed; continuing anyway"
fi

# --- Create the topic with desired settings ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Creating topic '${TOPIC_NAME}'..."
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --topic "${TOPIC_NAME}" \
  --partitions 3 \
  --replication-factor 2

# --- Log topic creation details ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Topic '${TOPIC_NAME}' created with:"
echo "    Partitions: 3"
echo "    Replication factor: 2"

# --- Describe the topic to confirm creation ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Describing topic '${TOPIC_NAME}':"
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic "${TOPIC_NAME}"

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Environment reset and Kafka topic management script completed."
