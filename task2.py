from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# Define the schema for incoming JSON data
schema = StructType([ 
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
aggregated = parsed.groupBy("driver_id").agg(
    spark_sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)
# Compute aggregations: total fare and average distance grouped by driver_id

# Define a function to write each batch to a CSV file

    # Save the batch DataFrame as a CSV file with the batch ID in the filename
def write_batch(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"outputs/task_2/batch_{batch_id}")

query = aggregated.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("complete") \
    .start()

# Use foreachBatch to apply the function to each micro-batch


query.awaitTermination()
