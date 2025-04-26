# 📁 dags/etl_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="etl_dag",
    description="Transforma archivos desde GCS y guarda limpios en el mismo bucket",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

etl_task = BashOperator(
    task_id="transform_from_gcs",
    bash_command="python /opt/airflow/etl/transform_gcs.py",
    dag=dag,
)

etl_task
