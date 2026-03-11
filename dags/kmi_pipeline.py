from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/app')
from loader2 import run_pipeline # We importeren jouw functie

default_args = {
    'owner': 'gemini',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kmi_weather_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval='*/10 * * * *', # Elke 10 minuten
    catchup=False
) as dag:

    run_kmi_task = PythonOperator(
        task_id='fetch_kmi_data',
        python_callable=run_pipeline
    )