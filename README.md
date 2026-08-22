# sports-goat-debate-analytics
A Data Engineering platform for collecting, processing and analyzing sports discussions (the best footballer of all time) from YouTube using Apache NiFi, Kafka, Spark, PostgreSQL and Metabase.

# Docker-compose Infrastructure

The entire pipeline is fully containerized using Docker Compose. The setup orchestrates 10 core services to support data ingestion, storage, batch/streaming processing, orchestration, and visualization.

| Service | Container Name | Port Mapping | Description & Role |
----------------------------------------------------------------
| **Apache NiFi** | `de_nifi` | `8443:8443` | Data Ingestion engine for fetching YouTube API data and routing flows |
| **MinIO** | `minio` | `9000:9000`, `9001:9001` | S3-compatible object storage serving as the primary Data Lake |
| **Apache Kafka** | `kafka` | `9092:9092` | Event streaming platform for real-time comment processing |
| **Zookeeper** | `de_zookeeper` | `2181:2181` | Centralized service for maintaining Kafka cluster state |
| **Kafka UI** | `kafka-ui` | `8083:8080` | Web interface for monitoring Kafka topics, partitions, and streams |
| **Spark Master** | `de_spark_master` | `8080:8080`, `7077:7077` | Master node for managing and executing PySpark batch processing jobs |
| **Spark Worker** | `de_spark_worker` | `8081:8081` | Worker node providing computational resources for PySpark jobs |
| **Spark Streaming Worker**| `spark_streaming_worker` | — | Dedicated driver executing continuous streaming jobs (`streaming_comments_cleanup.py`) |
| **PostgreSQL** | `postgres` | `5432:5432` | Relational target database for storing cleaned batch & streaming data |
| **Apache Airflow** | `de_airflow` | `1122:8080` | Workflow orchestration tool managing job schedules and dependencies |
| **Metabase** | `metabase` | `3000:3000` | Business intelligence dashboard for presenting visual analytics |

# Infrastructure & Configuration
The ingestion layer utilizes **Apache NiFi** for data orchestration, **MinIO** as an S3-compatible Data Lake for persistent storage, and **Apache Kafka** as a distributed message queue for real-time streaming.

To interact with MinIO, NiFi processors utilize an `AWSCredentialsProviderService` controller service configured with access and secret keys. Communication is established over the internal Docker network via `http://minio:9000`.

---

## Apache NiFi Data Ingestion Workflows

Data ingestion is split into two automated pipelines within Apache NiFi to handle raw video discovery and comment fetching independently.

### Workflow 1: Football GOAT Raw Data Ingestion
This workflow discovers relevant YouTube videos based on specific search criteria and writes the raw responses directly to object storage.

* **Triggering (`CRON Scheduling`):** 
  Executed automatically based on the CRON expression `0 42 16 * * ?` (at 16:42 daily).
* **API Fetching (`InvokeHTTP`):** 
  Sends an HTTP GET request to the YouTube Data API v3 `/search` endpoint using the query parameter `q=football+goat`. The request sets `maxResults=50`, fetching 50 video metadata objects per run, which corresponds to the maximum pagination limit allowed by YouTube per single request.
* **Data Lake Storage (`PutS3Object`):** 
  Receives the raw JSON response payload from the HTTP call and uploads it as an uncompressed object into the `football-goat-videos-raw` MinIO bucket.

### Workflow 2: Football GOAT Comments Ingestion & Dual-Path Routing
This pipeline reads pre-processed video records to extract individual `videoId`s, fetches their top comments, and bifurcates the incoming data into batch and streaming channels.

* **Execution Trigger (`GenerateFlowFile`):**
  A dummy FlowFile is generated on a fixed schedule using CRON (`0 34 19 * * ?`) to kick off the pipeline execution.
* **Data Lake Ingestion (`FetchS3Object`):**
  Retrieves cleaned video metadata stored in Parquet format from the `football-goat-videos-cleaned` MinIO bucket (populated during downstream cleaning jobs).
* **Record Deserialization (`ConvertRecord`):**
  Uses a `ParquetReader` and a `JsonRecordSetWriter` controller service to convert the compressed Parquet byte stream into a readable JSON structure.
* **Item Splitting & Attribute Extraction (`SplitJson` & `EvaluateJsonPath`):**
  * `SplitJson` breaks down the video array into distinct FlowFiles per video.
  * `EvaluateJsonPath` extracts the `$.id` field from each JSON record and assigns it to a custom FlowFile attribute named `videoId`.
* **Dynamic API Request (`InvokeHTTP`):**
  Constructs a dynamic request to the `/commentThreads` endpoint using the extracted attribute (`?videoId=${videoId}&maxResults=100`). It retrieves up to 100 comments per video (the maximum limit supported per call to avoid API page token bugs).
* **Record Unpacking (`SplitJson`):**
  Splits the composite comments JSON response into single FlowFiles containing one comment each.
  ---

#### Dual-Path Ingestion Routing Logic

Once comments are split into individual JSON records, the flow splits into two concurrent execution paths:

##### 1. Streaming Path (Real-Time Pipeline)
* **Processor:** `PublishKafka_1_0`
* **Target Topic:** `comments-streaming-topic`
* **Mechanism:** Immediately publishes individual raw comment FlowFiles to the Kafka broker (`kafka:29092`). This path eliminates storage latency, making comments immediately available for low-latency streaming applications.

##### 2. Batch Path (Data Lake Persistence)
* **Aggregation (`MergeContent`):**
  To prevent the "small files problem" in object stores, individual comment FlowFiles are merged into larger micro-batches based on count (e.g., grouping 100 comments) or time thresholds.
* **Format Conversion (`ConvertRecord`):**
  Converts the aggregated JSON array into columnar **Apache Parquet** format using an `AvroSchemaRegistry` or dynamic schema inference to optimize storage footprint and query performance.
* **Data Lake Persistence (`PutS3Object`):**
  Writes the resulting Parquet files into the `football-goat-comments-raw` MinIO bucket for subsequent batch transformation and historical analysis.
  
## Phase III: Batch Data Processing

Batch processing logic follows a Medallion Architecture (Bronze -> Silver -> Gold) to clean raw JSON payloads from MinIO and load optimized, deduplicated dataset structures into PostgreSQL.

### 1. Raw to Cleaned Video Metadata Pipeline (`videos-raw-to-clean-data.py`)

This PySpark batch job handles the **Bronze-to-Silver** transformation layer for video metadata.

#### Execution Workflow:
* **Raw Extraction:** Connects to MinIO using the native `minio` Python SDK and iterates through all JSON files stored in the `football-goat-videos-raw` bucket.
* **Spark DataFrame Creation:** Parses JSON payloads, appends the ingestion file path metadata, and loads the records into a PySpark DataFrame.
* **Cleaning & Structuring:**
  * Flattens nested JSON structures (`id.videoId`, `snippet.title`, `snippet.channelTitle`, `snippet.publishedAt`).
  * Aliases extracted attributes into normalized snake_case database schema fields (`video_id`, `title`, `channel_title`, `published_at`).
  * Filters out invalid records where `video_id` is missing (`IS NOT NULL`).
  * Deduplicates records on `video_id` to ensure unique video entries.
* **Parquet Serialization & Upload:**
  * Uses `.coalesce(1)` to merge split partition output into a single contiguous Parquet file.
  * Overwrites the cleaned object in the `football-goat-videos-cleaned` MinIO bucket under a standardized key path: `data/latest_cleaned_videos.parquet`.

> **Key Role in Pipeline:** The output file (`latest_cleaned_videos.parquet`) serves as the direct source input for NiFi's **Football GOAT Comments Data Workflow** to extract individual `videoId`s for fetching comments.

> **Architectural Decision Note:** 
Instead of using native Hadoop S3A connectors (`s3a://`) within Spark, raw object retrieval and final file persistence are handled directly via Python's native `minio` SDK alongside PySpark. This design choice was made to bypass the heavy configuration overhead of Hadoop S3A dependencies and AWS SDK versioning in the Spark environment, keeping the setup lightweight, reliable, and perfectly aligned with the scope of this project.
 
### 2. Raw to Cleaned Comments Batch Pipeline (`batch_comment_cleanup.py`)

This PySpark batch application performs the **Silver-to-Gold** processing step for YouTube comments, reading raw Parquet files from MinIO, applying schema flattening and timestamp conversions, and persisting unique records into PostgreSQL.

#### Execution Workflow:
* **Dynamic S3 Object Retrieval:** Uses the `minio` Python SDK to inspect the `football-goat-comments-raw` bucket, dynamically identifies the latest updated Parquet object based on the `last_modified` metadata attribute, and downloads it locally for processing.
* **Schema Flattening & Type Conversion:**
  * Extracts deeply nested comment attributes:
    * `snippet.topLevelComment.id` -> `comment_id`
    * `snippet.videoId` -> `video_id`
    * `snippet.channelId` -> `channel_id`
    * `snippet.topLevelComment.snippet.authorDisplayName` -> `author`
    * `snippet.topLevelComment.snippet.textDisplay` -> `comment_text`
    * `snippet.topLevelComment.snippet.likeCount` -> `like_count`
  * Casts string timestamps (`publishedAt`) into native PySpark `TimestampType` values using `to_timestamp`.
* **In-Memory Deduplication:** Removes internal duplicate records within the current file batch using `.dropDuplicates(["comment_id"])`.
* **Optimized Database-Side Deduplication (Predicate Pushdown):**
  * To eliminate expensive full-table scans of PostgreSQL within Spark, the script extracts the incoming list of `comment_id`s and pushes a targeted predicate filter directly down to PostgreSQL via JDBC:
    `SELECT comment_id FROM cleaned_comments WHERE comment_id IN ('id1', 'id2', ...)`
  * Applies a **Left Anti Join** (`clean_df.join(existing_df, "comment_id", "left_anti")`) between the incoming batch DataFrame and the returned existing IDs, ensuring only truly new records are appended.
* **Target Persistence:** Appends new unique comment records into the PostgreSQL `cleaned_comments` table.

### Workflow Orchestration & Automation (Apache Airflow & Shell Scripts)

To ensure smooth automated batch operations, job execution is orchestrated using **Apache Airflow** configured with the `Europe/Belgrade` timezone (`pendulum.timezone`). Airflow triggers lightweight shell scripts that interact with the Docker Daemon via `BashOperator` tasks.

#### 1. Airflow DAGs Setup

* **Raw-to-Clean Video Pipeline (`1_sports_goat_raw_to_clean`)**:
  * **Schedule:** Executes daily at 19:45 (`45 19 * * *`).
  * **Operator:** `BashOperator` executing `/app/run_raw_to_clean.sh`.
  * **Function:** Triggers the Bronze-to-Silver video metadata cleaning script inside the Spark Master container.
* **Batch Comments Processing Pipeline (`2_sports_goat_run_batch`)**:
  * **Schedule:** Executes daily at 20:00 (`0 20 * * *`) — scheduled 15 minutes after the raw-to-clean workflow to ensure dependencies are fully processed.
  * **Operator:** `BashOperator` executing `/app/run_batch.sh`.
  * **Function:** Triggers the Silver-to-Gold comment cleanup and PostgreSQL deduplicated load script.

#### 2. Containerized Job Execution Scripts

* **`run_raw_to_clean.sh`**:
  Configures the macOS Docker Host socket path and executes `spark-submit` inside the `de_spark_master` container for `videos-raw-to-clean-data.py`, appending stdout/stderr logs to `/tmp/spark_batch.log`.
* **`run_batch.sh`**:
  Sets `DOCKER_API_VERSION=1.44` for API compatibility and runs `spark-submit` pointing to the standalone cluster master (`spark://spark-master:7077`) for `batch_comments_cleanup.py`. Outputs are streamed directly to Airflow's native logging task console.

## Phase IV: Streaming Data Processing

The streaming layer processes comment payloads in real time as they arrive on the Kafka message bus, cleans and flattens their schema, and persists the structured records directly into PostgreSQL for live analytics.

### Real-Time Comment Ingestion & Transformation (`streaming_comments_cleanup.py`)

This PySpark Structured Streaming application runs continuously within the dedicated `spark_streaming_worker` container.

#### 1. Stream Subscription & Kafka Integration
* **Source:** Subscribes to the Kafka topic `comments-streaming-topic` over `kafka:29092` with `startingOffsets=earliest`.
* **Dependencies:** Dynamically loads the `spark-sql-kafka-0-10_2.12:3.3.0` streaming connector and the PostgreSQL JDBC driver (`postgresql:42.6.0`).

#### 2. Schema Definition & Parsing
* Defines an explicit, deeply nested `StructType` schema reflecting the raw YouTube API `/commentThreads` JSON response (`kind`, `etag`, `id`, `snippet.topLevelComment...`).
* Casts the raw Kafka binary `value` payload to a string and parses it using `from_json`.
* Flattens and extracts core comment metadata:
  * `id` -> `comment_id`
  * `snippet.videoId` -> `video_id`
  * `snippet.topLevelComment.snippet.authorDisplayName` -> `author`
  * `snippet.topLevelComment.snippet.textOriginal` -> `text`
  * `snippet.topLevelComment.snippet.likeCount` -> `like_count`
  * `snippet.topLevelComment.snippet.publishedAt` -> `published_at`
* Filters out malformed or unparseable messages where `comment_id IS NOT NULL`.

#### 3. Micro-Batch Sink (`foreachBatch` & PostgreSQL Write)
* Utilizes a `foreachBatch` writer function (`write_to_postgres`) to output streaming micro-batches synchronously into PostgreSQL.
* Writes processed records to the target database table `stream_cleaned_comments` using `append` mode via JDBC.
* Tracks fault tolerance and streaming progress using a persistent local checkpoint location (`/tmp/spark-kafka-checkpoint-v11`).