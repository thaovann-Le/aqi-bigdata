#!/bin/bash
# Creates the Kafka topics used by the ingestion pipeline.
# Run once by the `kafka-init` one-shot container after Kafka reports healthy.
set -euo pipefail

BOOTSTRAP="kafka:9092"
TOPICS=("aqi_raw" "weather_raw")
PARTITIONS=3
REPLICATION=1

for topic in "${TOPICS[@]}"; do
  kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo "Kafka topics ready:"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
