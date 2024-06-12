from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 9),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(ti):
        data = ti.xcom_pull(task_ids="extract_weather_data")
        city=data["name"]
        weather_description = data['weather'][0]['description']
        temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure=data['main']['pressure']
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
        
        transformed_data_list = [transformed_data]
        df_data = pd.DataFrame(transformed_data_list)
        
        now = datetime.now()
        dt_string = now.strftime("%d%m%Y%H%M%S")
        
        # Directory where the file will be saved
        directory = '/opt/airflow/'
        os.makedirs(directory, exist_ok=True)
        
        file_name = f'current_weather_data_toronto_{dt_string}.csv'
        local_file_path = os.path.join(directory, file_name)
        df_data.to_csv(local_file_path, index=False)
        print(f'New File saved to {local_file_path}')

with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:
    
        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Toronto&appid=00e17f5fff0f90bfc3fc8702ef4d88fd'
        )

        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Toronto&appid=00e17f5fff0f90bfc3fc8702ef4d88fd',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
