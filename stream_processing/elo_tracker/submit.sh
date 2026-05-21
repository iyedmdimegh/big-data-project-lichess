#!/usr/bin/env bash
# Run from the repo root:
#   bash stream_processing/elo_tracker/submit.sh
#
# Submits the ELO tracker job to the in-container Spark master. JARs (Kafka,
# Cassandra connector, Postgres JDBC) are baked into the chess-spark image,
# so no --packages are needed.

set -euo pipefail

docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cores.max=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=512m \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoints/elo_tracker \
  /opt/spark-apps/stream_processing/elo_tracker/elo_tracker.py

echo "elo_tracker submitted. Spark UI: http://localhost:8090"
