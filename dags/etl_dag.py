

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

# ✅ DAG ejecutable manualmente o programado a diario
dag = DAG(
    dag_id="etl_dag",
    description="Ejecuta el proceso ETL: transformación de datos y carga al data lake",
    default_args=default_args,
    schedule_interval="@daily",  # 🟡 Ejecuta automáticamente todos los días
    catchup=False,
    tags=["etl", "gcs", "transform"],
)

# 🔧 BashOperator para ejecutar el script de transformación y carga
etl_task = BashOperator(
    task_id="run_etl_transform",
    bash_command="python /opt/airflow/etl/transform_load.py",
    dag=dag,
)

etl_task
