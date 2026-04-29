#!/usr/bin/env bash
# Run from the repo root:
#   bash batch_processing/kmeans_openings/submit.sh

set -euo pipefail

docker exec big-data-project-lichess-spark-master-1 sh -c \
  "/opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --conf spark.cores.max=4 \
     /opt/spark-apps/batch_processing/kmeans_openings/kmeans_openings.py"
