#!/usr/bin/env bash
set -euo pipefail

# Fetch the Dataproc cluster name from instance metadata
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

# Define the Kafka bootstrap server address
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"

# Name of the Kafka topic
TOPIC_NAME="crimes-in-chicago-topic"

# Delete existing topic (ignore errors if it doesn't exist)
echo "$(date '+%Y-%m-%d %H:%M:%S') Deleting topic '${TOPIC_NAME}' if it exists..."
if kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${TOPIC_NAME}" \
  --delete; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') Deletion command succeeded or topic did not exist"
else
  echo "$(date '+%Y-%m-%d %H:%M:%S') Deletion command failed; continuing anyway"
fi

# Create the topic with desired settings
echo "$(date '+%Y-%m-%d %H:%M:%S') Creating topic '${TOPIC_NAME}'..."
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --topic "${TOPIC_NAME}" \
  --partitions 3 \
  --replication-factor 2

echo "$(date '+%Y-%m-%d %H:%M:%S') Topic '${TOPIC_NAME}' created with:"
echo "    Partitions: 3"
echo "    Replication factor: 2"

# Describe the topic to confirm creation
echo "$(date '+%Y-%m-%d %H:%M:%S') Describing topic '${TOPIC_NAME}':"
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic "${TOPIC_NAME}"

# Log completion
echo "$(date '+%Y-%m-%d %H:%M:%S') Kafka topic management script completed."
