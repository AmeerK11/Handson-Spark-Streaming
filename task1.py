from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed.writeStream \
    .format("csv") \
    .option("path", "outputs/task_1") \
    .option("checkpointLocation", "checkpoints/task_1") \
    .option("header", True) \
    .outputMode("append") \
    .start()

query.awaitTermination()

query.awaitTermination()
