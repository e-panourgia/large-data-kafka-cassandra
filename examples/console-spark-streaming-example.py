"""
TestKafkaMovieStream: Basic Real-Time Kafka Stream Reader with Spark

Overview:
---------
This script provides a Spark Structured Streaming pipeline example.
It reads streaming data from a Kafka topic named 'test', parses JSON data,
converts timestamps properly, and prints the parsed results to the console in real-time.

Used for testing and debugging Kafka-to-Spark connectivity and schema parsing.

Tech Stack:
-----------
- Apache Kafka
- Apache Spark
- Python / PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp

# Define schema matching your Kafka messages
ratingSchema = StructType([
    StructField("name", StringType(), False),
    StructField("movie", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("rating", IntegerType(), False)
])

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("TestKafkaMovieStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read stream from Kafka topic 'test'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and convert timestamps
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ratingSchema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Output stream to console for debugging
parsed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
