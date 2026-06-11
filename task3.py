from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("RideSharingAnalytics_Task3").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

parsed = parsed.withWatermark("event_time", "1 minute")

windowed = parsed.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(spark_sum("fare_amount").alias("total_fare"))

windowed = windowed \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

def write_batch(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"outputs/task_3/batch_{batch_id}")

query = windowed.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()