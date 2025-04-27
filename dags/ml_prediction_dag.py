"""DAG para predicciones"""

# 📁 dags/ml_prediction_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="ml_prediction_dag",
    description="Predice el rating esperado de nuevas reseñas usando el modelo entrenado de XGBoost",
    default_args=default_args,
    schedule_interval="@weekly",  # Ejecución automática semanal
    catchup=False,
)

predict_rating_task = BashOperator(
    task_id="predict_rating",
    bash_command="python /opt/airflow/ml/predict_rating.py",
    dag=dag,
)

predict_rating_task
