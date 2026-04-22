from sqlalchemy import create_engine, text
import os

def log_pipeline_run(
    pipeline_name,
    task_name,
    status,
    start_time,
    end_time=None,
    records_processed=None,
    error_message=None,
):
    database_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    engine = create_engine(database_url)

    insert_sql = text("""
        INSERT INTO audit.pipeline_runs (
            pipeline_name,
            task_name,
            status,
            start_time,
            end_time,
            records_processed,
            error_message
        )
        VALUES (
            :pipeline_name,
            :task_name,
            :status,
            :start_time,
            :end_time,
            :records_processed,
            :error_message
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "pipeline_name": pipeline_name,
            "task_name": task_name,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "records_processed": records_processed,
            "error_message": error_message,
        })