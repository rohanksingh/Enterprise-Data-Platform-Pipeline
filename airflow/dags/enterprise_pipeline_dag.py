from datetime import datetime
import subprocess
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"

def run_command(command):
    env = os.environ.copy()
    env["DB_HOST"] = "postgres"
    env["DB_PORT"] = "5432"
    env["DB_NAME"] = "enterprise_data"
    env["DB_USER"] = "admin"
    env["DB_PASSWORD"] = "admin123"

    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        shell=True,
        capture_output=True,
        text=True,
        env=env
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {command}")

def ingest_trades():
    run_command("python -m src.ingestion.ingest_trades_csv")

def bronze_to_silver():
    run_command("python -m src.transformation.run_bronze_to_silver")

def silver_to_gold():
    run_command("python -m src.warehouse.run_silver_to_gold")

with DAG(
    dag_id="enterprise_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "warehouse", "finance"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_trades_to_bronze",
        python_callable=ingest_trades
    )

    bronze_to_silver_task = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=bronze_to_silver
    )

    silver_to_gold_task = PythonOperator(
        task_id="load_silver_to_gold",
        python_callable=silver_to_gold
    )

    ingest_task >> bronze_to_silver_task >> silver_to_gold_task