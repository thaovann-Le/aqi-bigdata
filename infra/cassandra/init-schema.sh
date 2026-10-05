#!/bin/bash
# Applies schema.cql once Cassandra is reachable.
# depends_on already waits for the `cassandra` healthcheck, this loop is just
# a small safety margin for inter-container DNS/network readiness.
set -euo pipefail

until cqlsh cassandra -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  echo "Waiting for Cassandra to accept connections..."
  sleep 3
done

cqlsh cassandra -f /scripts/schema.cql
echo "Cassandra schema applied."
