"""DAG para predicciones


"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    "ml_prediction_dag",
    default_args=default_args,
    description="Ejecuta la predicción de sentimiento sobre reseñas sin clasificar",
    schedule_interval=None,  # Manual por ahora
    catchup=False,
)

# Tarea: ejecutar el script predict.py
predict_task = BashOperator(
    task_id="predict_sentiment",
    bash_command="python /opt/airflow/ml/predict.py",
    dag=dag,
)

predict_task

