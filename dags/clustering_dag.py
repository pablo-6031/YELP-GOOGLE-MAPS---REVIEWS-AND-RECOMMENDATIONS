"""DAG para clustering
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
    "clustering_dag",
    default_args=default_args,
    description="Clustering de coordenadas geográficas con KMeans",
    schedule_interval=None,
    catchup=False,
)

clustering_task = BashOperator(
    task_id="cluster_locations",
    bash_command="python /opt/airflow/ml/cluster_locations.py",
    dag=dag,
)

clustering_task
