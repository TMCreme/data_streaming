"""

"""
import os
import requests
# import time
import json
import math
# import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.python import PythonOperator

from airflow.models import Variable

load_dotenv()

# logger = logging.getLogger(__name__)

base_url = os.environ.get("BASE_URL")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_request_paramters() -> dict:
    try:
        latitudes = Variable.get("latitudes", default_var="52.5244,52.3471,53.5507,48.1374,50.1155")
        longitudes = Variable.get("longitudes", default_var="13.4105,14.5506,9.993,11.5755,8.6842")
        hourly = Variable.get(
            "hourly", "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,surface_pressure,temperature_80m")
        daily = Variable.get("daily", default_var="weather_code")
        start_date = Variable.get("start_date", "2024-01-01")
        end_date = Variable.get("end_date", "2024-05-01")
        # logger.info("Variables returned suffessfully")
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
    request_parameters.pop("hourly")
    print(request_parameters)
    formatted_params = "&".join([k+'='+str(v) for k, v in request_parameters.items()])
    url = f"{base_url}?{formatted_params}"
    print(url)
    headers = {}

    response = requests.get(url, headers=headers)

    print(response.text)


if __name__ == "__main__":
    get_daily_data()
