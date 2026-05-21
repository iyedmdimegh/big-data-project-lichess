#!/usr/bin/env bash
# Run from the repo root:
#   bash stream_processing/live_featured/submit.sh

set -euo pipefail

docker exec -d big-data-project-lichess-spark-master-1 sh -c \
  "/opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --conf spark.cores.max=1 \
     --conf spark.executor.cores=1 \
     --conf spark.executor.memory=512m \
     /opt/spark-apps/stream_processing/live_featured/live_featured.py \
     > /tmp/live_featured.log 2>&1"

echo "live_featured submitted. Spark UI: http://localhost:8090"
