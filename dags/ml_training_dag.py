"""DAG para entrenamiento ML

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
    "ml_training_dag",
    default_args=default_args,
    description="Entrena el modelo de análisis de sentimiento desde DW",
    schedule_interval=None,  # Solo se ejecuta manualmente por ahora
    catchup=False,
)

# Ejecuta el script que entrena el modelo y guarda el .pkl
train_model = BashOperator(
    task_id="train_sentiment_model",
    bash_command="python /opt/airflow/ml/train_sentiment.py",
    dag=dag,
)

train_model

