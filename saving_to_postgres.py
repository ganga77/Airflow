from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json

default_args = {
    'owner': 'ganga',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def kelvin_to_fahrenheit(temp_in_kelvin):
    return (temp_in_kelvin - 273.15) * 9/5 + 32

def transform_load_data(ti):
    data = ti.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data['weather'][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data['main']['pressure']
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone']).isoformat()
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone']).isoformat()
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone']).isoformat()


    transformed_data = {
        "city": city,
        "description": weather_description,
        "temperature": temp_fahrenheit,
        "feels_like_f": feels_like_fahrenheit,
        "minimum_temp_f": min_temp_fahrenheit,
        "maximum_temp_f": max_temp_fahrenheit,
        "pressure": pressure,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "time_of_record": time_of_record,
        "sunrise": sunrise_time,
        "sunset": sunset_time
    }

    print('Weather Data is transformed!')

    ti.xcom_push(key='transformed_data', value=transformed_data)

with DAG(
    dag_id='dag_with_postgres_operator_v04',
    default_args=default_args,
    start_date=datetime(2024, 6, 8),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    
    is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Toronto&appid=00e17f5fff0f90bfc3fc8702ef4d88fd'
        )
    
    create_table = PostgresOperator(
        task_id='create_postgres_table_weather',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS weather (
                city VARCHAR(50),
                description VARCHAR(100),
                temperature FLOAT,
                feels_like_f FLOAT,
                minimum_temp_f FLOAT,
                maximum_temp_f FLOAT,
                pressure INTEGER,
                humidity INTEGER,
                wind_speed FLOAT,
                time_of_record TIMESTAMP,
                sunrise TIMESTAMP,
                sunset TIMESTAMP
            );
        """
    )

    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Toronto&appid=00e17f5fff0f90bfc3fc8702ef4d88fd',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

    transform_data = PythonOperator(
        task_id='transform_load_data',
        python_callable=transform_load_data
    )

    insert_data = PostgresOperator(
        task_id='insert_into_database',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO weather (
                city, description, temperature, feels_like_f, minimum_temp_f, maximum_temp_f, 
                pressure, humidity, wind_speed, time_of_record, sunrise, sunset
            ) VALUES (
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["city"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["description"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["temperature"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["feels_like_f"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["minimum_temp_f"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["maximum_temp_f"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["pressure"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["humidity"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["wind_speed"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["time_of_record"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["sunrise"] }}',
                '{{ ti.xcom_pull(task_ids="transform_load_data", key="transformed_data")["sunset"] }}'
            );
        """
    )

    is_weather_api_ready >> create_table >> extract_weather_data >> transform_data >> insert_data
