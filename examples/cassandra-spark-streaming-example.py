from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp, date_format

# Schema for Kafka messages (movie ratings)
ratingSchema = StructType([
    StructField("name", StringType(), False),
    StructField("movie", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("rating", IntegerType(), False)
])

# Schema for Netflix CSV
netflixSchema = StructType([
    StructField("show_id", StringType(), True),
    StructField("title", StringType(), False),
    StructField("director", StringType(), True),
    StructField("country", StringType(), True),
    StructField("release_year", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("duration", StringType(), True)
])

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("MovieRatingStreamer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read Netflix CSV with renamed rating column to avoid conflict with "rating" in Kafka schema
netflix_df = (
    spark.read.schema(netflixSchema)
         .option("header", True)
         .csv("data/netflix.csv")
         .withColumnRenamed("rating", "rating_category")  # Rename avoids conflict
         .cache()
)

# Read streaming data from Kafka
df = (
    spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "localhost:29092")
         .option("subscribe", "test")
         .option("startingOffsets", "latest")
         .load()
)

# Parse JSON from Kafka messages
ratings_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), ratingSchema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp("timestamp"))  # Convert string timestamp to TimestampType
      .withColumn("hour_bucket", date_format("timestamp", "yyyy-MM-dd HH:00"))  # Grouping by hour
)

# Join ratings with static Netflix metadata
enriched_df = (
    ratings_df.join(netflix_df, ratings_df.movie == netflix_df.title, "left")
              .drop("title")
)

# Optional: Print schema for debugging (can be removed later)
enriched_df.printSchema()

# Define Cassandra write logic
def writeToCassandra(writeDF, _):
    (
        writeDF.select(
            "name", "movie", "timestamp", "rating", "hour_bucket",
            "show_id", "director", "country", "release_year", "rating_category", "duration"
        )
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode('append')
        .options(table="movie_ratings", keyspace="netflix_ks")
        .save()
    )

# Write to Cassandra in a streaming loop
result = None
while result is None:
    try:
        result = (
            enriched_df.writeStream
                .foreachBatch(writeToCassandra)
                .outputMode("update")
                .option("checkpointLocation", "/tmp/checkpoints/movie")
                .trigger(processingTime="30 seconds")
                .start()
                .awaitTermination()
        )
    except Exception as e:
        print(f"Streaming error: {e}")