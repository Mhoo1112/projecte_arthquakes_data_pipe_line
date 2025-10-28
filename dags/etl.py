from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_geojson
from scripts.transfrom import transfrom_geojson
from scripts.load import load_geojson


with DAG(
    dag_id="etl_earthquakes_api",
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

    DB_USER = "admin_01"
    DB_PASS = "admin_12345"
    DB_HOST = "postgis_data"
    DB_PORT = "5432"
    DB_NAME = "postgis_db"
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    t3 = PythonOperator(
        task_id="load_geojson",
        python_callable=load_geojson,
        op_kwargs={'database_url' : DATABASE_URL},
    )

    t1 >> t2 >> t3
    # ...append