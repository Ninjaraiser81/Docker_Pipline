from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.fetch_and_store import fetch_and_store_stock_data

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="stock_market_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
) as dag:

    task_fetch_and_store = PythonOperator(
        task_id="fetch_store",
        python_callable=fetch_and_store_stock_data
    )

    task_fetch_and_store
