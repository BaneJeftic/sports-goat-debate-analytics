import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType, BooleanType

# 1. Kreiranje Spark sesije
spark = SparkSession.builder \
    .appName("YouTubeCommentsStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Postgres Konekcija
postgres_user = os.environ.get("POSTGRES_USER", "postgres")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
postgres_db = os.environ.get("POSTGRES_DB", "goat_db")
postgres_host = "postgres"
postgres_port = "5432"

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

# 3. TAČNA JSON ŠEMA prema tvojoj poruci
schema = StructType() \
    .add("kind", StringType()) \
    .add("etag", StringType()) \
    .add("id", StringType()) \
    .add("snippet", StructType() \
        .add("channelId", StringType()) \
        .add("videoId", StringType()) \
        .add("topLevelComment", StructType() \
            .add("kind", StringType()) \
            .add("etag", StringType()) \
            .add("id", StringType()) \
            .add("snippet", StructType() \
                .add("channelId", StringType()) \
                .add("videoId", StringType()) \
                .add("textDisplay", StringType()) \
                .add("textOriginal", StringType()) \
                .add("authorDisplayName", StringType()) \
                .add("authorProfileImageUrl", StringType()) \
                .add("authorChannelUrl", StringType()) \
                .add("authorChannelId", StructType().add("value", StringType())) \
                .add("canRate", BooleanType()) \
                .add("viewerRating", StringType()) \
                .add("likeCount", LongType()) \
                .add("publishedAt", StringType()) \
                .add("updatedAt", StringType())
            )
        ) \
        .add("canReply", BooleanType()) \
        .add("totalReplyCount", LongType()) \
        .add("isPublic", BooleanType())
    )

# 4. Čitanje iz Kafke
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "comments-streaming-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 5. Parsiranje sa proverom ne-null vrednosti
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.id").alias("comment_id"),
    col("data.snippet.videoId").alias("video_id"),
    col("data.snippet.topLevelComment.snippet.authorDisplayName").alias("author"),
    col("data.snippet.topLevelComment.snippet.textOriginal").alias("text"),
    col("data.snippet.topLevelComment.snippet.likeCount").alias("like_count"),
    col("data.snippet.topLevelComment.snippet.publishedAt").alias("published_at")
).filter(col("comment_id").isNotNull())  # Odbacuje neuspešno parsirane redove

# 6. ForeachBatch funkcija sa logovanjem i upisom u Postgres
def write_to_postgres(batch_df, batch_id):
    row_count = batch_df.count()
    
    print(f"\n==================== BATCH ID: {batch_id} ====================")
    if row_count == 0:
        print("-> Batch je prazan (nema novih poruka ili poruke ne odgovaraju šemi).")
        return

    print(f"Broj uspešno parsiranih redova: {row_count}")
    batch_df.show(5, truncate=False)
    
    print("Upisujem u Postgres tabelu 'stream_cleaned_comments'...")
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "stream_cleaned_comments") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print("-> UPIS USPEŠNO ZAVRŠEN!")
    except Exception as e:
        print(f"-> GREŠKA PRI UPISU U POSTGRES: {e}")
        traceback.print_exc()

# 7. Pokretanje (novi checkpoint v10 da pročita ponovo sve poruke iz Kafke od početka)
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint-v11") \
    .start()

query.awaitTermination()