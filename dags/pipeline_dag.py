"""DAG maestro

¿Qué hace?
Preparar el script de carga a GCS como función reutilizable.
Crear el DAG de Airflow (pipeline_dag.py) que:
Ejecuta el ETL
Sube los archivos .parquet generados al bucket del Data Lake (datalake-restaurantes-2025)
(más adelante) ejecutará la carga al DW y modelos de ML"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    "pipeline_dag",
    default_args=default_args,
    description="DAG maestro que orquesta ETL → DW → ML Training",
    schedule_interval=None,
    catchup=False,
)

trigger_etl = TriggerDagRunOperator(
    task_id="trigger_etl_dag",
    trigger_dag_id="etl_dag",
    dag=dag,
)

trigger_dw = TriggerDagRunOperator(
    task_id="trigger_dw_dag",
    trigger_dag_id="dw_dag",
    dag=dag,
)

trigger_ml_training = TriggerDagRunOperator(
    task_id="trigger_ml_training_dag",
    trigger_dag_id="ml_training_dag",
    dag=dag,
)

# Flujo de ejecución
trigger_etl >> trigger_dw >> trigger_ml_training

