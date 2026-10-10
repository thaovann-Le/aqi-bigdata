#!/usr/bin/env bash
# Chạy một job Spark của phần C với đầy đủ cấu hình (Kafka, Cassandra, MinIO/S3A, múi giờ UTC+7).
#
#   submit.sh <đường dẫn script, tính từ thư mục processing/> [tham số của script...]
#
# Ví dụ (trong container Spark, thư mục processing/ được mount vào /opt/processing):
#   /opt/processing/bin/submit.sh batch/batch_history.py --years 2024,2025
#   /opt/processing/bin/submit.sh batch/batch_aggregate.py
#   /opt/processing/bin/submit.sh streaming/stream_measurements.py --once
#
# Mọi giá trị đều chỉnh được bằng biến môi trường (mặc định khớp docker-compose của nhóm):
#   SPARK_MASTER (spark://spark-master:7077, hoặc local[2] để chạy không cần cụm)
#   CASSANDRA_HOST, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SPARK_PKG_VERSION
set -euo pipefail

if [[ $# -lt 1 ]]; then
  sed -n '2,15p' "$0"
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"     # .../processing
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_PKG_VERSION="${SPARK_PKG_VERSION:-3.5.6}"              # phải khớp phiên bản Spark của image
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_PKG_VERSION},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4"
DRIVER_HOST="${SPARK_DRIVER_HOST:-$(hostname)}"

script="$1"
shift

exec "${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --packages "${PACKAGES}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.jars.ivy="${IVY_DIR:-/tmp/ivy/${DRIVER_HOST}}" \
  --conf spark.driver.host="${DRIVER_HOST}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.cores.max="${SPARK_CORES_MAX:-1}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-8}" \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-cassandra}" \
  --conf spark.cassandra.output.ignoreNulls=true \
  --conf spark.hadoop.fs.s3a.endpoint="${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf spark.hadoop.fs.s3a.access.key="${MINIO_ACCESS_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.secret.key="${MINIO_SECRET_KEY:-minioadmin123}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  "${HERE}/${script}" "$@"
