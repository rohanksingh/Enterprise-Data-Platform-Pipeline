from datetime import datetime
import subprocess
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"
PIPELINE_NAME = "enterprise_data_pipeline"


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
        raise RuntimeError(f"Command failed: {command}\n{result.stderr}")

    return result.stdout


def get_row_count(table_name: str) -> int:
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="admin",
        password="admin123",
        dbname="enterprise_data"
    )
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def log_status(task_name, status, start_time, end_time=None, records_processed=None, error_message=None):
    from src.common.audit_logger import log_pipeline_run

    log_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=task_name,
        status=status,
        start_time=start_time,
        end_time=end_time,
        records_processed=records_processed,
        error_message=error_message,
    )


def ingest_trades():
    start_time = datetime.utcnow()
    task_name = "ingest_trades_to_bronze"
    log_status(task_name, "STARTED", start_time)

    try:
        run_command("python -m src.ingestion.ingest_trades_csv")
        count = get_row_count("bronze.trades_raw")
        log_status(task_name, "SUCCESS", start_time, datetime.utcnow(), count, None)
    except Exception as e:
        log_status(task_name, "FAILED", start_time, datetime.utcnow(), None, str(e))
        raise


def bronze_to_silver():
    start_time = datetime.utcnow()
    task_name = "transform_bronze_to_silver"
    log_status(task_name, "STARTED", start_time)

    try:
        run_command("python -m src.transformation.run_bronze_to_silver")
        count = get_row_count("silver.trades_clean")
        log_status(task_name, "SUCCESS", start_time, datetime.utcnow(), count, None)
    except Exception as e:
        log_status(task_name, "FAILED", start_time, datetime.utcnow(), None, str(e))
        raise


def silver_to_gold():
    start_time = datetime.utcnow()
    task_name = "load_silver_to_gold"
    log_status(task_name, "STARTED", start_time)

    try:
        run_command("python -m src.warehouse.run_silver_to_gold")
        count = get_row_count("gold.fact_trade")
        log_status(task_name, "SUCCESS", start_time, datetime.utcnow(), count, None)
    except Exception as e:
        log_status(task_name, "FAILED", start_time, datetime.utcnow(), None, str(e))
        raise


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