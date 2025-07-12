from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from airflow.utils.dates import days_ago
from datetime import datetime
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': True,  # Enable email notifications
    'email_on_retry': False,
    'retries': 2,  # Increased retries
    'retry_delay': timedelta(minutes=5)  # Increased retry delay
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify ETL process with proper error handling',
    schedule_interval=timedelta(days=1),
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Prevent concurrent runs
    tags=['spotify', 'etl', 'music']  # Add tags for better organization
)

run_etl = PythonOperator(
    task_id='complete_spotify_etl',
    python_callable=run_spotify_etl,
    dag=dag,
    provide_context=True  # Provide context for better logging
)

run_etl