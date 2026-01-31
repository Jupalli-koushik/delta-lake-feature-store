import os
import shutil
import sys
from pyspark.sql import SparkSession
from delta import *

# --- CRITICAL FIX: FORCE PYTHON 3.10 ---
# This path comes from your previous logs. It forces Spark to ignore Python 3.13.
python_path = r"C:\Users\Saikirankoushik\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# --- 1. SETUP SPARK WITH DELTA ---
builder = SparkSession.builder \
    .appName("FeatureStore_Day1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Safety for Windows

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 

print("✅ Spark Session Created Successfully!")

# --- 2. CREATE DUMMY RAW DATA (Bronze Source) ---
raw_csv_path = "data/raw/transactions"
delta_table_path = "data/delta_store/transactions_delta"

# Clean up previous runs
if os.path.exists(raw_csv_path): shutil.rmtree(raw_csv_path)
if os.path.exists(delta_table_path): shutil.rmtree(delta_table_path)

data = [
    (1, "Alice", "2025-01-01", 50.0),
    (2, "Bob",   "2025-01-01", 20.0),
    (3, "Alice", "2025-01-02", 30.0),
    (4, "Charlie","2025-01-03", 100.0)
]
columns = ["transaction_id", "user_id", "date", "amount"]

# Create DataFrame
dummy_df = spark.createDataFrame(data, columns)
dummy_df.write.csv(raw_csv_path, header=True)
print(f"✅ Raw CSV data created at: {raw_csv_path}")

# --- 3. INGESTION (CSV -> DELTA) ---
print("⏳ Reading CSV data...")
raw_df = spark.read.csv(raw_csv_path, header=True, inferSchema=True)

print("⏳ Writing to Delta Table...")
raw_df.write.format("delta").save(delta_table_path)

print(f"✅ Data successfully ingested into Delta Table at: {delta_table_path}")

# --- 4. VERIFICATION ---
print("\n--- Verifying Delta Table Content ---")
delta_df = spark.read.format("delta").load(delta_table_path)
delta_df.show()