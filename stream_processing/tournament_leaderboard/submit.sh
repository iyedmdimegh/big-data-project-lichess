#!/usr/bin/env bash
# Run from the repo root:
#   bash stream_processing/tournament_leaderboard/submit.sh

set -euo pipefail

docker exec -d big-data-project-lichess-spark-master-1 sh -c \
  "/opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --conf spark.cores.max=1 \
     --conf spark.executor.cores=1 \
     --conf spark.cassandra.connection.host=cassandra \
     --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
     /opt/spark-apps/stream_processing/tournament_leaderboard/leaderboard.py \
     > /tmp/tournament_leaderboard.log 2>&1"

echo "tournament_leaderboard submitted. Spark UI: http://localhost:8090"
