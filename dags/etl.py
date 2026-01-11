import os
import dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import extract_geojson
from scripts.transfrom import transfrom_geojson
from scripts.load import load_geojson

# ui env compose
setting = {
    'owner': 'Rapeepat-dataTeam',
    "email": ["buathongrapeepat@gmail.com"], # ผู้รับ
    "email_on_failure": True, # เปิดใช้งานการแจ้งเตือนเมื่อ Task ล้มเหลว
    "email_on_retry": False, # เปิดใช้งานการแจ้งเตือนเมื่อ Task ถูก Retry
    "retries": 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="etl_earthquakes_api",
    default_args=setting,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl"]
) as dag:

    URL = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    t1 = PythonOperator(
        task_id="extract_geojson",
        python_callable=extract_geojson,
        op_kwargs={'url': URL},
    )

    t2 = PythonOperator(
        task_id="transfrom_geojson",
        python_callable=transfrom_geojson,
    )

    dotenv.load_dotenv(os.path.join(os.path.dirname(__file__),".."))
    DB_USER = os.getenv(DB_USER)
    DB_PASS = os.getenv(DB_PASS)
    DB_HOST = os.getenv(DB_HOST)
    DB_PORT = os.getenv(DB_PORT)
    DB_NAME = os.getenv(DB_NAME)
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    t3 = PythonOperator(
        task_id="load_geojson",
        python_callable=load_geojson,
        op_kwargs={'database_url' : DATABASE_URL},
    )

    t1 >> t2 >> t3
    # ...append