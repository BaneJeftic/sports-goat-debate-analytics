import os
import glob
from minio import Minio

# Handle compatibility for both MinIO v7 (S3Error) and v6 (ResponseError)
try:
    from minio.error import S3Error
except ImportError:
    from minio.error import ResponseError as S3Error

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# ==========================================
# 1. ENVIRONMENT CONFIGURATION
# ==========================================
raw_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ENDPOINT = raw_endpoint.replace("http://", "").replace("https://", "").rstrip("/")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BUCKET_NAME = os.getenv("BUCKET_NAME") or os.getenv("MINIO_BUCKET", "football-goat-comments-raw")
PREFIX = os.getenv("MINIO_PREFIX", "")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "goat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespassword")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "cleaned_comments")

POSTGRES_URL = os.getenv("POSTGRES_URL", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
LOCAL_TEMP_DIR = os.getenv("LOCAL_TEMP_DIR", "/tmp/spark_processing")

def main():
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    local_file_path = None
    spark = None

    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    try:
        # 2. Check if the specified bucket exists
        if not minio_client.bucket_exists(BUCKET_NAME):
            print(f"[WARNING] Bucket '{BUCKET_NAME}' does not exist on the MinIO server.")
            return

        # 3. Search for objects within the target folder prefix (including raw UUID files without extensions)
        print(f"Scanning bucket '{BUCKET_NAME}' under prefix '{PREFIX}'...")
        objects = list(minio_client.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True))
        
        # Filter out directories and accept all valid data objects
        parquet_objects = [obj for obj in objects if not obj.is_dir]

        if not parquet_objects:
            print(f"No objects found in bucket '{BUCKET_NAME}' with prefix '{PREFIX}'.")
            return

        # Fetch the latest file based on last_modified timestamp
        latest_object = sorted(parquet_objects, key=lambda x: x.last_modified)[-1]
        file_name = os.path.basename(latest_object.object_name)
        local_file_path = os.path.join(LOCAL_TEMP_DIR, file_name)

        print(f"Found latest file for processing: {latest_object.object_name}")
        minio_client.fget_object(BUCKET_NAME, latest_object.object_name, local_file_path)

        # 4. Initialize Spark Session after verifying file download
        print("Initializing Spark session...")
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("MessiVsRonaldo_Batch_Comments")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )

        df = spark.read.parquet(local_file_path)

        # 5. Extract nested YouTube comment fields, format timestamp, and perform deduplication
        clean_df = df.select(
            col("snippet.topLevelComment.id").alias("comment_id"),
            col("snippet.videoId").alias("video_id"),  
            col("snippet.channelId").alias("channel_id"), 
            col("snippet.topLevelComment.snippet.authorDisplayName").alias("author"),
            col("snippet.topLevelComment.snippet.textDisplay").alias("comment_text"),
            col("snippet.topLevelComment.snippet.likeCount").alias("like_count"),
            to_timestamp(col("snippet.topLevelComment.snippet.publishedAt")).alias("published_at")
        ).dropDuplicates(["comment_id"])

        total_inner_unique = clean_df.count()
        print(f"Unique comments in file after internal deduplication: {total_inner_unique}")

        if total_inner_unique == 0:
            print("No comments found in the source file.")
            return

        # 6. Optimized Deduplication (Query only relevant IDs from PostgreSQL)
        print("Checking for existing records in PostgreSQL...")
        
        # Extract comment_ids present in the current batch
        id_list = [row.comment_id for row in clean_df.select("comment_id").collect()]
        formatted_ids = ",".join([f"'{cid}'" for cid in id_list])
        
        # Push down filter query directly to PostgreSQL
        pushed_query = f"(SELECT comment_id FROM {POSTGRES_TABLE} WHERE comment_id IN ({formatted_ids})) AS existing"

        try:
            existing_df = spark.read \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", pushed_query) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            # Filter out existing comment IDs using Left Anti Join
            clean_df = clean_df.join(existing_df, "comment_id", "left_anti")
        except Exception as e:
            print(f"[INFO] Target table is empty or does not exist yet. Skipping join: {e}")

        total_new = clean_df.count()
        print(f"New unique records ready for insertion: {total_new}")

        if total_new > 0:
            # 7. Write clean records into PostgreSQL
            print("Writing new records to PostgreSQL database...")
            (
                clean_df.write
                .format("jdbc")
                .option("url", POSTGRES_URL)
                .option("dbtable", POSTGRES_TABLE)
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )
            print("Successfully processed and saved batch data to PostgreSQL!")
        else:
            print("No new comments to write to the database.")

    except S3Error as err:
        print(f"MinIO/S3 Error encountered: {err}")
    except Exception as e:
        print(f"Error occurred during Spark batch processing: {e}")

    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped.")
        
        if local_file_path and os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Removed temporary local file: {local_file_path}")

if __name__ == "__main__":
    main()