#!/usr/bin/env bash
set -e

DATASET="simple_flight_delay_features.jsonl.bz2"
DATASET_URL="http://s3.amazonaws.com/agile_data_science/${DATASET}"

echo "Connecting to MinIO..."
until mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1; do
  sleep 3
done

echo "Creating buckets..."
mc mb --ignore-existing local/warehouse
mc mb --ignore-existing local/raw
mc mb --ignore-existing local/checkpoints

echo "Downloading dataset..."
wget -q -O /tmp/${DATASET} ${DATASET_URL}

echo "Uploading dataset to raw bucket..."
mc cp /tmp/${DATASET} local/raw/

echo "Done. Raw bucket contents:"
mc ls local/raw