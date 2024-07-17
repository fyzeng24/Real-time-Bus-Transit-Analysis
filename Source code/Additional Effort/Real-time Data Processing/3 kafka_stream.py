import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 12, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://api.data.gov.sg/v1/environment/rainfall")
    res = res.json()

    return res

def format_data(res):
    # We assume that 'res' is the whole JSON object
    formatted_data = []

    # We link the station info and readings based on station_id
    station_info = {station['id']: station for station in res['metadata']['stations']}
    
    for item in res['items']:
        for reading in item['readings']:
            station_id = reading['station_id']
            station = station_info.get(station_id, {})
            
            data = {}
            data['id'] = station.get('id', '')
            data['device_id'] = station.get('device_id', '')
            data['name'] = station.get('name', '')
            data['location'] = station.get('location', {})
            data['timestamp'] = item.get('timestamp', '')
            data['reading_value'] = reading.get('value', '')
            
            formatted_data.append(data)

    return formatted_data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['localhost:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )