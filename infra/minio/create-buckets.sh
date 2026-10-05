#!/bin/sh
# Creates the MinIO buckets used as the raw data lake.
# Run once by the `minio-init` one-shot container after MinIO reports healthy.
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb --ignore-existing local/raw
mc mb --ignore-existing local/history

echo "MinIO buckets ready:"
mc ls local
