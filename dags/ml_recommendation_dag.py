"""DAG para recomendaciones


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
    "ml_recommendation_dag",
    default_args=default_args,
    description="Genera recomendaciones de restaurantes a partir de reseñas",
    schedule_interval=None,
    catchup=False,
)

recommendation_task = BashOperator(
    task_id="generate_recommendations",
    bash_command="python /opt/airflow/ml/recommendation.py",
    dag=dag,
)

recommendation_task

