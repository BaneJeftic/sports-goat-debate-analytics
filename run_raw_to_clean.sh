#!/bin/bash

# Define paths and Docker socket for the macOS cron environment
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"
export DOCKER_HOST="unix:///Users/branislav/.docker/run/docker.sock"

# Project path
cd "$(dirname "$0")"

echo "=== Pokretanje Spark obrade (videos): $(date) ===" >> /tmp/spark_batch.log

# Run spark-submit inside the container
/usr/local/bin/docker exec de_spark_master /spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /app/videos-raw-to-clean-data.py >> /tmp/spark_batch.log 2>&1

echo "=== Završeno (videos): $(date) ===" >> /tmp/spark_batch.log