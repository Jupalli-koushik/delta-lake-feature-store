import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, current_timestamp
from delta import *
from delta.tables import *

# --- 1. ENVIRONMENT SETUP ---
python_path = r"C:\Users\Saikirankoushik\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# --- 2. SPARK SESSION (With Windows Network Fix) ---
builder = SparkSession.builder \
    .appName("FeatureStore_Day2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Paths
bronze_path = "data/delta_store/transactions_delta"
silver_path = "data/delta_store/user_features"

print("🚀 Starting Feature Engineering Pipeline...")

# --- 3. READ BRONZE LAYER ---
print(f"⏳ Reading Raw Data from: {bronze_path}")
bronze_df = spark.read.format("delta").load(bronze_path)

# --- 4. COMPUTE FEATURES (Aggregations) ---
print("⏳ Computing Derived Features...")
features_df = bronze_df.groupBy("user_id").agg(
    sum("amount").alias("total_spend"),
    count("transaction_id").alias("transaction_count")
).withColumn("updated_at", current_timestamp())

print("\n--- Computed Features Preview ---")
features_df.show()

# --- 5. WRITE TO SILVER LAYER ---
print(f"⏳ Saving to Silver Feature Store at: {silver_path}")
features_df.write.format("delta").mode("overwrite").save(silver_path)

print("✅ Silver Layer Update Complete!")

# --- 6. VERIFY VERSIONING ---
print("\n--- Silver Table History ---")
delta_table = DeltaTable.forPath(spark, silver_path)
delta_table.history().select("version", "timestamp", "operation", "operationParameters.mode").show(truncate=False)