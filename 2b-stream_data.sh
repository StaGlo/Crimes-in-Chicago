#!/usr/bin/env bash
set -euo pipefail

# --- Install python dependencies ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Installing Python dependencies..."
pip install --no-cache-dir kafka-python

# Run the Python script to produce records
echo "$(date '+%Y-%m-%d %H:%M:%S') Starting to produce records..."
python 2b-streamer.py

# --- Log completion ---
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished producing all records."
