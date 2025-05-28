#!/usr/bin/env bash
set -euo pipefail

# Datasets URLs
IUCR_URL="https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv"
CRIMES_URL="https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/crimes-in-chicago_result.zip"

# Local download directory
DOWNLOAD_DIR="/tmp/crime_data"

# Kafka settings
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"

# --- Prepare workspace ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Cleaning and creating download directory at ${DOWNLOAD_DIR}"
rm -rf "${DOWNLOAD_DIR}"
mkdir -p "${DOWNLOAD_DIR}"

# --- Download datasets ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading IUCR codes CSV"
wget -q "${IUCR_URL}" -O "${DOWNLOAD_DIR}/IUCR_codes.csv"

echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading crimes stream ZIP"
wget -q "${CRIMES_URL}" -O "${DOWNLOAD_DIR}/crimes.zip"

# --- Unpack crimes data ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Unzipping crimes data"
unzip -q "${DOWNLOAD_DIR}/crimes.zip" -d "${DOWNLOAD_DIR}/crimes"
