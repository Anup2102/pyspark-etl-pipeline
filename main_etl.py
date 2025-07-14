from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, to_date

# 1. Initialize SparkSession
spark = SparkSession.builder \
    .appName("Automated ETL Pipeline") \
    .getOrCreate()
print("Spark session created.")

# 2. Read raw CSV data with header
input_path = "nyc_taxi_sample.csv"
df_raw = spark.read.option("header", True).csv(input_path)
print("Reading input CSV data...")

print("=== RAW DATA SAMPLE ===")
df_raw.show(5)

# 3. Schema validation & type casting
df_clean = df_raw \
    .withColumn("passenger_count", col("passenger_count").cast("int")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("fare_amount", col("fare_amount").cast("double")) \
    .withColumn("pickup_datetime", to_date(col("pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_date(col("dropoff_datetime")))
print("Cleaning and transforming data...")

# 4. Add load_date column with current date
df_clean = df_clean.withColumn("load_date", current_date())

# 5. Filter rows with valid (positive) fare and distance
df_clean = df_clean.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

print("=== CLEANED DATA SAMPLE ===")
df_clean.show(5)

# 6. Write cleaned data as partitioned Parquet files by load_date
output_path = "output_parquet"
df_clean.write.mode("overwrite").partitionBy("load_date").parquet(output_path)
print("Writing partitioned Parquet files...")

# 7. Final message

print(f"ETL Pipeline completed successfully. Cleaned data written to: {output_path}")

spark.stop()
