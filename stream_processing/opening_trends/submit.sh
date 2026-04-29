#!/usr/bin/env bash
# Run from the repo root:
#   bash stream_processing/opening_trends/submit.sh

set -euo pipefail

docker exec -d big-data-project-lichess-spark-master-1 sh -c \
  "/opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --conf spark.cores.max=1 \
     --conf spark.executor.cores=1 \
     /opt/spark-apps/stream_processing/opening_trends/opening_trends.py \
     > /tmp/opening_trends.log 2>&1"

echo "opening_trends submitted. Spark UI: http://localhost:8090"
