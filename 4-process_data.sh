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
if [[ $# -ne 3 ]]; then
    echo "Usage: $0 [A|C] [P] [D]"
    exit 1
fi

DELAY_OPTION="$1"
if [[ "$DELAY_OPTION" != "A" && "$DELAY_OPTION" != "C" ]]; then
    echo "Invalid delay: '$DELAY_OPTION'. Must be 'A' or 'C'."
    echo "Usage: $0 [A|C]"
    exit 1
fi

DAYS_OPTION="$2"
if [[ ! "$DAYS_OPTION" =~ ^[0-9]+$ ]]; then
    echo "Invalid days: '$DAYS_OPTION'. Must be a positive integer."
    echo "Usage: $0 [A|C] [P] [D]"
    exit 1
fi

PERCENTAGE_OPTION="$3"
if [[ ! "$PERCENTAGE_OPTION" =~ ^[0-9]+$ ]] || [[ "$PERCENTAGE_OPTION" -lt 0 ]] || [[ "$PERCENTAGE_OPTION" -gt 100 ]]; then
    echo "Invalid percentage: '$PERCENTAGE_OPTION'. Must be an integer between 0 and 100."
    echo "Usage: $0 [A|C] [P] [D]"
    exit 1
fi

# --- Download used driver ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading PostgreSQL JDBC driver..."
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# --- Run Spark job as YARN application ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Launching Spark streaming job..."
echo "Delay configuration: $DELAY_OPTION"

spark-submit \
    --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --driver-class-path postgresql-42.6.0.jar \
    --jars postgresql-42.6.0.jar \
    3-processing_app.py \
    --bootstrap-servers "$BOOTSTRAP_SERVER" \
    --input-topic "$TOPIC_NAME" \
    --static-file "$HDFS_STATIC_FILE" \
    --checkpoint-location "$HDFS_CHECKPOINTS" \
    --delay "$DELAY_OPTION" \
    --window-days "$DAYS_OPTION" \
    --treshold "$PERCENTAGE_OPTION" \

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished processing data."
