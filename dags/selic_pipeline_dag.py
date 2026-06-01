from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments configuration for error handling and retries
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),  # short retry delay for dev/testing
}


def run_bronze():
    # Import inside the callable to prevent Airflow webserver parsing errors
    from bronze.main import run as run_bronze_ingestion
    run_bronze_ingestion(start_date="01/01/2020", end_date="31/12/2024")


def run_silver():
    from silver.main import run as run_silver_transformation
    run_silver_transformation()


def run_gold():
    from gold.main import run as run_gold_aggregation
    run_gold_aggregation()


with DAG(
    "selic_bcb_pipeline",
    default_args=default_args,
    description="Orchestrates daily Selic ingestion, transformation, and aggregation",
    schedule_interval=None,  # Manually triggered for demonstration
    catchup=False,
    tags=["selic", "bcb", "hexagonal"],
) as dag:

    bronze_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="aggregate_gold",
        python_callable=run_gold,
    )

    # Establish sequential dependency
    bronze_task >> silver_task >> gold_task
