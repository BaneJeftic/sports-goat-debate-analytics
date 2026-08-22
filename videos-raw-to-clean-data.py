import json
import os
from minio import Minio
from pyspark.sql import SparkSession

# Čist endpoint bez http:// i donjih crta
endpoint = "minio:9000"

access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

# Inicijalizacija MinIO klijenta
minio_client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False,
)

bucket_raw = "football-goat-videos-raw"

print(f"Čitam fajlove iz bucketa: {bucket_raw}...")

all_items = []

try:
  objects = minio_client.list_objects(bucket_raw, recursive=True)
  for obj in objects:
    if obj.object_name.endswith(".json"):
      response = minio_client.get_object(bucket_raw, obj.object_name)
      try:
        data = json.loads(response.read().decode("utf-8"))
        if "items" in data:
          for item in data["items"]:
            item["ingestion_file"] = obj.object_name
            all_items.append(item)
      finally:
        response.close()
        response.release_conn()
except Exception as e:
  print(f"Greška pri čitanju iz MinIO-a: {e}")

# Inicijalizacija Spark Sesije
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

if not all_items:
  print("Upozorenje: Nema validnih JSON fajlova u raw bucket-u.")
else:
  print(f"Pronađeno {len(all_items)} stavki. Pokrećem PySpark obradu...")

  # Konvertujemo prikupljene JSON objekte u PySpark DataFrame
  df_raw = spark.createDataFrame(all_items)

  # Prečišćavanje podataka
  df_cleaned = (
      df_raw.select(
          df_raw["id"]["videoId"].alias("video_id"),
          df_raw["snippet"]["title"].alias("title"),
          df_raw["snippet"]["channelTitle"].alias("channel_title"),
          df_raw["snippet"]["publishedAt"].alias("published_at"),
          df_raw["ingestion_file"].alias("ingestion_timestamp"),
      )
      .filter("video_id IS NOT NULL")
      .dropDuplicates(["video_id"])
  )

  print("Prikaz prvih 10 prečišćenih zapisa:")
  df_cleaned.show(10, truncate=False)

  # Čuvanje u Parquet format lokalno u kontejneru
  local_parquet_dir = "/tmp/cleaned_parquet"
  
  # .coalesce(1) osigurava da imamo samo jedan fajl umesto više particija
  df_cleaned.coalesce(1).write.mode("overwrite").parquet(local_parquet_dir)

  # Upload Parquet fajla u Silver/Cleaned bucket pod FIKSNIM imenom
  bucket_clean = "football-goat-videos-cleaned"
  if not minio_client.bucket_exists(bucket_clean):
    minio_client.make_bucket(bucket_clean)

  # Tražimo generisani part fajl i dižemo ga pod stalnim imenom
  uploaded = False
  for root, _, files in os.walk(local_parquet_dir):
    for file in files:
      if file.endswith(".parquet") and file.startswith("part-"):
        local_path = os.path.join(root, file)
        target_object_name = "data/latest_cleaned_videos.parquet"
        
        minio_client.fput_object(bucket_clean, target_object_name, local_path)
        uploaded = True
        break
    if uploaded:
      break

  print(
      "\n=== USPEH: Podaci uspešno prečišćeni i prebačeni u"
      " 'football-goat-videos-cleaned/data/latest_cleaned_videos.parquet'! ==="
  )

spark.stop()