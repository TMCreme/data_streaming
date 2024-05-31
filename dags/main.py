"""

"""
import os
import requests
# import time
import json
# import math
import logging
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

from producer import produce_message

from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import task
# from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.models import Variable

load_dotenv()

logger = logging.getLogger(__name__)

base_url = os.environ.get("BASE_URL", "https://api.open-meteo.com/v1/dwd-icon")
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
        latitudes = Variable.get("latitudes", default_var="52.5244,52.3471,53.5507,48.1374,50.1155")
        longitudes = Variable.get("longitudes", default_var="13.4105,14.5506,9.993,11.5755,8.6842")
        hourly = Variable.get(
            "hourly", "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,surface_pressure,temperature_80m")
        daily = Variable.get("daily", default_var="weather_code")
        start_date = Variable.get("start_date", "2024-05-01")
        end_date = Variable.get("end_date", "2024-05-28")
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


def transform_data(data_values) -> list:
    """"""
    time_data = data_values["time"]
    weather_data = data_values["weather_code"]
    result = [{"id": str(uuid4()), "date_time": date, "weather_value": value} for date, value in zip(time_data, weather_data)]

    return result


with DAG(
    "weather_daily_dag",
    default_args=default_args,
    description="dimensions and metrics for google analytics",
    schedule_interval="@daily",
) as ga_dimensions_metrics_dag:

    @task
    def start_task():
        print("Let's start the task")

    @task
    def daily_main():
        """"""
        response_data = get_daily_data()
        # data_keys = response_data["daily_units"].keys()
        data_values = response_data[0]["daily"]

        data_dict = transform_data(data_values)
        items_to_remove = ("daily_units", "daily")
        for d in items_to_remove:
            response_data[0].pop(d)

        for item in data_dict:
            item.update(response_data[0])
        json_str = json.dumps(data_dict)
        print(json_str)
        json_bytes = json_str.encode('utf-8')
        produce_message(message=json_bytes)
        # asyncio.run(consume())
        return data_dict

    spark_processing = SparkSubmitOperator(
        task_id='spark_processor_task',
        conn_id='spark_default',
        application="/opt/airflow/dags/spark_stream.py",
        total_executor_cores=3,
        executor_memory="4g",
        conf={
            "spark.network.timeout": 1000000,
            "spark.executor.heartbeatInterval": 100000,
            "spark.storage.blockManagerSlaveTimeoutMs": 100000,
            "spark.driver.maxResultSize": "10g"
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15"
    )

    @task
    def done():
        print("Done with the task")

    dummy_start = start_task()
    main_task = daily_main()
    end_task = done()

    dummy_start >> main_task >> spark_processing >> end_task


# if __name__ == "__main__":
#     message_data = daily_main()
#     produce_message(key="hourly_data", message=message_data)
