# Ride Sharing Analytics Using Spark Streaming and Spark SQL

## Overview
Real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming.

---

## Task 1: Basic Streaming Ingestion and Parsing

### Approach
- Created a Spark session and read streaming data from socket on localhost:9999
- Parsed incoming JSON messages into a DataFrame with columns: trip_id, driver_id, distance_km, fare_amount, timestamp
- Wrote parsed data to CSV files in outputs/task_1

---

## Task 2: Real-Time Aggregations (Driver-Level)

### Approach
- Reused parsed DataFrame from Task 1
- Grouped by driver_id and computed SUM(fare_amount) as total_fare and AVG(distance_km) as avg_distance
- Used foreachBatch to write each micro-batch to outputs/task_2

---

## Task 3: Windowed Time-Based Analytics

### Approach
- Converted timestamp string column to TimestampType as event_time
- Applied 1-minute watermark for late data handling
- Performed 5-minute windowed aggregation sliding by 1 minute on fare_amount
- Used foreachBatch to write windowed results to outputs/task_3

---

## How to Run

```bash
pip install pyspark faker numpy
export JAVA_HOME="/usr/local/sdkman/candidates/java/17.0.11-tem"
export PATH="$JAVA_HOME/bin:$PATH"
```

Terminal 1:
```bash
python data_generator.py
```

Terminal 2:
```bash
python task1.py  # or task2.py or task3.py
```
