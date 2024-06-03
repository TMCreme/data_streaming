# Data Streaming Pipeline

## Introduction
This is a data processing pipeline which ingests data (weather data) from an endpoint and drops it in Apache Kafka. Then Apache Spark is used to process the data and stored in Apache Cassandra. The entire pipeline is orchestrated using Apache Airflow with the help of Kafka and Spark providers. Due to the fact that data is available in hourly and daily batches, rather than building a streaming pipeline, this is a batch processing pipeline. This solution can also be used for streaming in which case the batch processing in Spark is tweaked to satisfy the streaming solution but the spark triggers have to be handled in a custom fashion. 
The design of tasks in a DAG is that upstream tasks fully execute successfully before a downstream task executes. That dependency nature does not make it suitable to orchestrate an entire streaming pipeline since in a streaming pipeline, all tasks are running continuously.

## Description
* The 

## Running the code locally (Linux or Mac)

## Running the code on AWS EC2


## Tools
* Apache Ariflow
* Cassandra
* Apache Spark
* Apache Kafka
* PostgreSQL
* Confluent Schema Registry

