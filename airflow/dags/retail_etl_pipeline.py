from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    "owner": "retail_team",
    "description": "Run full Nova Retail ETL Pipeline: Bronze → Silver → Gold → PostgreSQL",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["retail", "etl", "spark", "postgresql"],
) as dag:

    run_etl = BashOperator(
        task_id="run_full_etl_pipeline",
        # ✅ FIXED: sets path, ensures Python finds your code
        bash_command="export PYTHONPATH=/opt/retail_project && cd /opt/retail_project && python -m etl.run_pipeline",
    )

    run_etl
