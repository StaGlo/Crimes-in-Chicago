#!/usr/bin/env python3
import os
import subprocess
import sys
import glob
import time
from kafka import KafkaProducer  # type: ignore


def get_cluster_name():
    try:
        result = subprocess.check_output(
            ["/usr/share/google/get_metadata_value", "attributes/dataproc-cluster-name"],
            stderr=subprocess.DEVNULL,
        )
        return result.decode("utf-8").strip()
    except subprocess.CalledProcessError:
        sys.stderr.write("Error: Unable to retrieve Dataproc cluster name from metadata.\n")
        sys.exit(1)


def main():

    # --- Kafka settings ---
    cluster_name = get_cluster_name()
    bootstrap_server = f"{cluster_name}-m:9092"
    topic_name = "crimes-in-chicago-topic"

    # --- Local download directory ---
    download_dir = "/tmp/crime_data"
    csv_dir = os.path.join(download_dir, "crimes", "crimes-in-chicago_result")

    # Check that the directory exists
    if not os.path.isdir(csv_dir):
        sys.stderr.write(f"Error: Directory not found: {csv_dir}\n")
        sys.exit(1)

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server], linger_ms=5)

    print(
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} Starting to produce records to Kafka topic '{topic_name}'"
    )

    # Iterate over all CSV files in the directory
    pattern = os.path.join(csv_dir, "*.csv")
    for csvfile in sorted(glob.glob(pattern)):
        print(f"  Processing file: {csvfile}")
        try:
            with open(csvfile, "r") as f:
                # Skip header line
                header = f.readline()
                for line in f:
                    record = line.rstrip("\n")
                    producer.send(topic_name, value=record.encode("utf-8"))

        except Exception as e:
            sys.stderr.write(f"Error reading {csvfile}: {e}\n")

    # Ensure all buffered messages are sent
    producer.flush()
    producer.close()

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} Finished producing all records.")


if __name__ == "__main__":
    main()
