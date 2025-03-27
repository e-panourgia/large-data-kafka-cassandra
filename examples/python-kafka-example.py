"""
Async Kafka Producer: Fake Movie Ratings Generator

Overview:
---------
This script continuously generates **fake movie rating events** and sends them to a Kafka topic ("test").
It simulates users rating random movies with timestamps, mimicking real-time data.

Components:
-----------
1. Faker: Generates fake human names.
2. Pandas: Loads real movie titles from a CSV file.
3. aiokafka: Asynchronous Kafka producer library.
4. Kafka: Acts as the message broker for event streaming.

Use Case:
---------
Run this script to simulate live rating data for a Spark Structured Streaming application to consume and process.
"""
import json
import asyncio
import random
from datetime import datetime

import pandas as pd
from faker import Faker
from aiokafka import AIOKafkaProducer

# Load movie titles
movies_df = pd.read_csv('data/movies.csv', header=None, names=["movie_title"])
movie_titles = movies_df['movie_title'].dropna().tolist()

# Generate names
fake = Faker()
names = [fake.name() for _ in range(10)]
names.append("Evangelia Panourgia") # add my own name

# Kafka settings
KAFKA_TOPIC = "test"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

# JSON serializer
def serializer(data):
    return json.dumps(data).encode()

# Producer logic
async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serializer,
        compression_type="gzip"
    )

    await producer.start()
    try:
        while True:
            for name in names:
                movie = random.choice(movie_titles)
                rating = random.randint(1, 10)
                timestamp = datetime.now().isoformat()

                message = {
                    "name": name,
                    "movie": movie,
                    "timestamp": timestamp,
                    "rating": rating
                }

                print(f"Sending: {message}")
                await producer.send(KAFKA_TOPIC, message)
                await asyncio.sleep(60) # One message per user per minute
    finally:
        await producer.stop()

# Run it (no deprecation warning)
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(produce())
