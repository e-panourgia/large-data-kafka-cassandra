### Kafka-Cassandra-Large-Data
In the scope of this assignment (see `ProgrammingAssignment_2.pdf`), we will use the Apache Spark framework and the Apache Cassandra NoSQL database in order to create a structured streaming Spark process that consumes Kafka messages and uses Cassandra as a sink to persist information.

A detailed report has been written explaining all parts of this assignment (see `report.pdf`).

### Deployment
1. **Open a first terminal for the Kafka Producer** and execute the following commands in sequence:
```bash
vagrant up
vagrant ssh
cd /vagrant

docker-compose -f docker-compose-kafka.yml up -d
docker-compose -f docker-compose-cassandra.yml up -d
docker-compose -f docker-compose-spark.yml up -d

sudo apt update
sudo apt install python3.12-venv -y
python3 -m venv venv
source venv/bin/activate
pip install pandas faker kafka-python aiokafka

python3 examples/python-kafka-example.py
```

2. **Open a second terminal for creating cassandra table and running quiries** :
```bash
vagrant ssh
cd /vagrant

docker exec -it cassandra cqlsh

CREATE KEYSPACE IF NOT EXISTS netflix_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS netflix_ks.movie_ratings (
    name text,
    hour_bucket text,
    timestamp timestamp,
    movie text,
    rating int,
    show_id text,
    director text,
    country text,
    release_year int,         -- changed from text → int
    rating_category text,
    duration int,             -- changed from text → int
    PRIMARY KEY ((name, hour_bucket), timestamp, movie)
);
```

3. **Open a third terminal for running comsole script** helping in the scope of debugging for the first part.:
```bash
vagrant ssh
cd /vagrant

# install java 
sudo apt update
sudo apt install openjdk-17-jdk -y
java -version
readlink -f $(which java)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
echo $JAVA_HOME
java -version

# run console script
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  examples/console-spark-streaming-example.py
```

4. **Open a fourth terminal for running spark script** enrich with metadata and preprocess with spark:
```bash
vagrant ssh
cd /vagrant

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH

SPARK_LOCAL_DIRS=/vagrant/spark-tmp spark-submit \
  --conf spark.local.dir=/vagrant/spark-tmp \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  examples/cassandra-spark-streaming-example.py
```

### CQL Queries 
- Query 1: Retrieve movie durations for average runtime calculation:
```bash
SELECT avg(rating) FROM netflix_ks.movie_ratings
WHERE name = 'Evangelia Panourgia' AND hour_bucket = '2025-03-27 09:00';
```
- __Purpose__: To fetch all movie durations rated by Evangelia Panourgia during the hour 2025-03-27 09:00, which can be used to compute the average runtime manually.

-  Query 2: Retrieve names of the movies rated during the same hour:
```bash
SELECT movie FROM netflix_ks.movie_ratings
WHERE name = 'Evangelia Panourgia' AND hour_bucket = '2025-03-27 09:00';
```
- __Purpose__: To list all movie titles rated by Evangelia Panourgia during the hour 2025-03-27 09:00.