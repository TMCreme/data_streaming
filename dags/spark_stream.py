"""
Spark Stream processing - Consuming from Kafka
Loading to Cassadra
"""
import os
import logging
from pyspark.sql import SparkSession

from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)
daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")


def spark_connect():
    spark = (
        SparkSession.builder.appName("WeatherApp").getOrCreate()
    )
    # spark.conf.set("spark.sql.shuffle.partitions", 1000)
    return spark


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS analytics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def read_stream(spark_session):
    """Define the subscription to kafka topic and read stream"""
    df = spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", daily_topic) \
        .option("startingOffsets", "latest") \
        .load()
    my_df = df.selectExpr("CAST(value as STRING)", "timestamp")
    return my_df


def write_stream_data():
    spark = spark_connect()
    stream_data = read_stream(spark)
    stream_data.writeStream\
        .format("console")\
        .outputMode("append")\
        .start()

    query = stream_data.writeStream\
        .option("checkpointLocation", '/tmp/check_point/')\
        .format("org.apache.spark.sql.cassandra")\
        .option("spark.cassandra.connection.host", "cassandra_db")\
        .option("keyspace", "analytics")\
        .option("table", "hourlydata")\
        .start()
    query.awaitTermination()


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra_db'], port=9042, )

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logger.error(f"Could not create cassandra connection due to {e}")
        return None


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS analytics.hourlydata (
        id TEXT PRIMARY KEY,
        latitude DECIMAL,
        longitude DECIMAL,
        date_time DATE,
        generationtime_ms DECIMAL,
        utc_offset_seconds DECIMAL,
        timezone TEXT,
        timezone_abbreviation TEXT,
        elevation DECIMAL,
        weather_value DECIMAL)
    """)

    print("Table created successfully!")


if __name__ == "__main__":
    cassandra_conn = create_cassandra_connection()
    if cassandra_conn:
        create_keyspace(cassandra_conn)
        create_table(cassandra_conn)
        write_stream_data()
    else:
        logger.error("Cassandra connection failed")
        print("Cassandra connection failed")
