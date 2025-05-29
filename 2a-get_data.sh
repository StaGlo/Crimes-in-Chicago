#!/usr/bin/env bash
set -euo pipefail

# --- Datasets URLs ---
IUCR_URL="https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv"
CRIMES_URL="https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/crimes-in-chicago_result.zip"

# --- Local download directory and HDFS static directory ---
DOWNLOAD_DIR="/tmp/crime_data"
HDFS_STATIC_DIR="/streaming/static"

# --- Download IUCR codes CSV ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading IUCR codes CSV"
wget -q "${IUCR_URL}" -O "${DOWNLOAD_DIR}/IUCR_codes.csv"

# --- Download crimes stream ZIP ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading crimes stream ZIP"
wget -q "${CRIMES_URL}" -O "${DOWNLOAD_DIR}/crimes.zip"

# --- Unzip crimes data ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Unzipping crimes data"
unzip -q "${DOWNLOAD_DIR}/crimes.zip" -d "${DOWNLOAD_DIR}/crimes"

# --- Upload IUCR codes to HDFS ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Uploading IUCR codes CSV to HDFS"
hadoop fs -put -f /tmp/crime_data/IUCR_codes.csv "${HDFS_STATIC_DIR}/IUCR_codes.csv"
echo "$(date '+%Y-%m-%d %H:%M:%S') HDFS ${HDFS_STATIC_DIR} contains:"
hadoop fs -ls "${HDFS_STATIC_DIR}/"

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished downloading and preparing data."
