from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.bronze import run_bronze
from scripts.silver import run_silver
from scripts.gold import run_gold

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_selic_medallion",
    default_args=default_args,
    description="Orchestrates BCB Selic Daily Interest Rates pipeline across Medallion layers",
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze,
        op_kwargs={
            "start_date": "01/01/2020",
            "end_date": "31/12/2024",
            "output_path": "data/bronze/selic_raw.parquet"
        }
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
        op_kwargs={
            "input_path": "data/bronze/selic_raw.parquet",
            "output_path": "data/silver/selic_cleaned.parquet"
        }
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_gold",
        python_callable=run_gold,
        op_kwargs={
            "input_path": "data/silver/selic_cleaned.parquet",
            "output_path": "data/gold/selic_metrics.parquet"
        }
    )

    ingest_bronze >> transform_silver >> aggregate_gold
