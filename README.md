# sports-goat-debate-analytics
A Data Engineering platform for collecting, processing and analyzing sports discussions from YouTube and Reddit using Apache NiFi, Kafka, Spark, PostgreSQL and Metabase.

# Docker-compose Infrastructure

The docker-compose.yml infrastructure orchestrated for the ingestion and streaming pipeline consists of the following containerized services:

Zookeeper (image: confluentinc/cp-zookeeper:7.5.0) - Distributed coordination service for managing Kafka broker state and cluster metadata.

Apache Kafka (image: confluentinc/cp-kafka:7.5.0) - Event streaming broker responsible for real-time message queuing and forwarding ingested API payloads (mapped to host port 9092).

MinIO Object Storage (image: minio/minio:RELEASE.2023-09-04T19-57-37Z) - High-performance, AWS S3-compatible object storage serving as the primary batch data lake repository (replacing traditional HDFS for lightweight cloud-native deployment). Web console mapped to port 9001, S3 API endpoint mapped to port 9000.

Apache NiFi (image: apache/nifi:1.23.2) - Visual data integration engine executing the main ETL/Ingestion flows: polling API sources, transforming payloads into standardized JSON/Parquet batches, and concurrently branching data streams into Kafka topics (streaming) and MinIO buckets (batch lake). Mapped to HTTPS port 8443.

Volumes & Networks - Shared Docker network for seamless inter-container communication (e.g., NiFi-to-Kafka, NiFi-to-MinIO) and persistent volumes for data retention across container restarts.
