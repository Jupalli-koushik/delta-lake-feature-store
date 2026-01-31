import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, current_timestamp
from delta import *
from delta.tables import *

# --- 1. ENVIRONMENT SETUP (Windows Fixes) ---
python_path = r"C:\Users\Saikirankoushik\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# --- 2. SPARK SESSION ---
builder = SparkSession.builder \
    .appName("FeatureStore_Day3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

silver_path = "data/delta_store/user_features"

print("🚀 Starting Day 3: Time Travel Demo...")

# --- 3. SIMULATE NEW DATA (Day 2 Updates) ---
# Alice spends 50 more dollars. Bob spends 10 more.
print("⏳ Simulating new transactions arriving...")
new_data = [
    (1, "Alice", 50.0), # Alice's total was 80, should become 130
    (2, "Bob",   10.0), # Bob's total was 20, should become 30
    (3, "Alice", 30.0), # Alice buys logs of data
    (5, "Dave",  200.0) # New user!
]
# Note: For simplicity in this demo, we are just overwriting with "Updated State"
# In production, you would read raw -> merge -> write. 
# Here we manually construct the 'Next State' df for demonstration.
updated_data = [
    ("Alice", 160.0, 4), # 80 old + 80 new
    ("Bob",   30.0,  2), # 20 old + 10 new
    ("Charlie", 100.0, 1), # No change
    ("Dave",  200.0, 1)  # New User
]
columns = ["user_id", "total_spend", "transaction_count"]

updated_df = spark.createDataFrame(updated_data, columns)\
    .withColumn("updated_at", current_timestamp())

# --- 4. UPDATE THE SILVER TABLE ---
print(f"⏳ Overwriting Silver Table with new data (Creating Version 1)...")
updated_df.write.format("delta").mode("overwrite").save(silver_path)

print("✅ Silver Layer Updated!")

# --- 5. TIME TRAVEL (The Magic) ---
print("\n🔮 TIME TRAVEL: Querying the PAST (Version 0) vs PRESENT (Version 1)")

# Read Version 0 (The old data from Day 2)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
print("\n--- 📜 Version 0 (Old Data) ---")
df_v0.select("user_id", "total_spend").orderBy("user_id").show()

# Read Version 1 (The new data from Day 3)
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(silver_path)
print("\n--- 🆕 Version 1 (Current Data) ---")
df_v1.select("user_id", "total_spend").orderBy("user_id").show()

# --- 6. HISTORY AUDIT ---
print("\n--- 🕵️ Table History Audit ---")
DeltaTable.forPath(spark, silver_path).history().select("version", "timestamp", "operation").show(truncate=False)