"""
Daily aggregate
"""
import os
import requests
# import time
import json
# import math
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import task
# from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

from airflow.models import Variable

load_dotenv()

logger = logging.getLogger(__name__)

base_url = os.environ.get("BASE_URL", "https://api.open-meteo.com/v1/dwd-icon")
daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
hourly_topic = os.environ.get("HOURLY_DATA_TOPIC", "hourlymetrics")

# Variables for historic data
latitudes = Variable.get("latitudes", default_var="52.5244,52.3471,53.5507,48.1374,50.1155")
longitudes = Variable.get("longitudes", default_var="13.4105,14.5506,9.993,11.5755,8.6842")
hourly = Variable.get(
    "hourly", "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,surface_pressure,temperature_80m")
daily = Variable.get("daily", default_var="weather_code")
start_date = datetime.today().strftime("%Y-%m-%d")
end_date = start_date

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime.today(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def get_request_paramters() -> dict:
    try:
        logger.info("Variables returned suffessfully")
        return {
            "latitude": latitudes,
            "longitude": longitudes,
            "hourly": hourly,
            "daily": daily,
            "start_date": start_date,
            "end_date": end_date
        }
    except Exception as var_except:
        print(var_except.__str__())
        # logger.error(var_except.__str__())
        return False


def get_hourly_data():
    """Get hourly data"""
    request_parameters = get_request_paramters()
    request_parameters.pop("daily")
    print(request_parameters)
    formatted_params = "&".join([k+'='+str(v) for k, v in request_parameters.items()])
    url = f"{base_url}?{formatted_params}"
    print(url)
    headers = {}

    response = requests.get(url, headers=headers)

    print(response.text)


def get_daily_data():
    request_parameters = get_request_paramters()
    print(request_parameters)
    request_parameters.pop("hourly")
    formatted_params = "&".join([k+'='+str(v) for k, v in request_parameters.items()])
    url = f"{base_url}?{formatted_params}"
    print(url)
    headers = {}

    response = requests.get(url, headers=headers)
    # constant_pair = response.json()
    print(response.json())
    return response.json()


def transform_data(time_values, data_values) -> list:
    """"""
    time_data = time_values
    weather_data = data_values
    result = [{"id": str(uuid4()), "date_time": date, "weather_value": value} for date, value in zip(time_data, weather_data)]

    return result


def daily_main():
    """Process the daily data for loading"""
    response_data = get_daily_data()
    # data_keys = response_data["daily_units"].keys()
    items_to_remove = ("daily_units", "daily")
    i = 0
    for item in response_data:
        data_values = item.pop("daily")
        item.pop("daily_units")

        data_dict = transform_data(data_values["time"], data_values["weather_code"])
 
        for new_item in data_dict:
            i += 1
            new_item.update(item)
            print(json.dumps(new_item))
            yield (json.dumps(i), json.dumps(new_item))


def hourly_main():
    """Process the hourly data coming in. """
    response_data = get_hourly_data()
    lat_long_map = list(zip(latitudes.split(","), longitudes.split(",")))

    i = 0
    for item in response_data:
        hourly_units = item.pop("hourly_units", None)
        data_values = item.pop("hourly", None)
        time_data = data_values.pop("time")
        for hourly_item in data_values:
            data_array = transform_data(time_data, hourly_item)
            for each_data in data_array:
                i += 1
                each_data.update(item)
                json_str = json.dumps(each_data)
                yield (json.dumps(i), json_str)


with DAG(
    "weather_data_dag",
    default_args=default_args,
    description="Weather metrics data streaming",
    schedule_interval="@daily",
) as weather_data_dag:

    @task
    def start_task():
        print("Let's start the task")

    # @task

    produce_daily_metrics = ProduceToTopicOperator(
        task_id="produce_daily_metrics",
        kafka_config_id="kafka_default",
        topic=daily_topic,
        producer_function=daily_main,
        poll_timeout=10,
    )

    produce_hourly_metrics = ProduceToTopicOperator(
        task_id="produce_hourly_metrics",
        kafka_config_id="kafka_default",
        topic=hourly_topic,
        producer_function=hourly_main,
        poll_timeout=10,
    )

    spark_processing_daily = SparkSubmitOperator(
        task_id='spark_processor_daily_task',
        conn_id='spark_default',
        application="/opt/airflow/dags/spark_stream_daily.py",
        total_executor_cores=4,
        executor_memory="12g",
        conf={
            "spark.network.timeout": 1000000,
            "spark.executor.heartbeatInterval": 100000,
            "spark.storage.blockManagerSlaveTimeoutMs": 100000,
            "spark.driver.maxResultSize": "20g"
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15"
    )

    spark_processing_hourly = SparkSubmitOperator(
        task_id='spark_processor_hourly_task',
        conn_id='spark_default',
        application="/opt/airflow/dags/spark_stream_hourly.py",
        total_executor_cores=4,
        executor_memory="12g",
        conf={
            "spark.network.timeout": 1000000,
            "spark.executor.heartbeatInterval": 100000,
            "spark.storage.blockManagerSlaveTimeoutMs": 100000,
            "spark.driver.maxResultSize": "20g"
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15"
    )

    @task
    def done():
        print("Done with the task")

    dummy_start = start_task()
    # main_task = daily_main()
    end_task = done()

    dummy_start >> produce_daily_metrics >> spark_processing_daily >> end_task
    dummy_start >> produce_hourly_metrics >> spark_processing_hourly >> end_task
