# 📁 dags/ml_training_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="ml_training_dag",
    description="Entrena el modelo de regresión para predicción de ratings",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
)

train_ml_task = BashOperator(
    task_id="train_rating_model",
    bash_command="python /opt/airflow/ml/train_regression.py",
    dag=dag,
)

train_ml_task
