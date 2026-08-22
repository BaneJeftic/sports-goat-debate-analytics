#!/bin/bash

# Exit immediately if any command returns a non-zero status
set -e

# Set the Docker API version to match the macOS Docker Daemon requirements
export DOCKER_API_VERSION=1.44

echo "=== Starting Spark batch processing: $(date) ==="

# Execute spark-submit inside the de_spark_master container
# Note: Log redirection was removed so Airflow captures stdout/stderr directly
docker exec de_spark_master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.6.0 \
  /app/batch_comments_cleanup.py

echo "=== Processing finished successfully: $(date) ==="