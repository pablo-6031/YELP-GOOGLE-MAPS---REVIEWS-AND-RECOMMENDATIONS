"""DAG para carga a DW

Objetivo del DAG: dw_dag.py
Leer archivos .parquet del bucket datalake-restaurantes-2025
Cargar esos archivos a BigQuery (ya sea como carga inicial o incremental)
Usar PythonOperator para ejecutar la lógica definida en tu script load_dw.py
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ruta al script del DW (carga de datos a BigQuery)
sys.path.append("/opt/airflow/dw/scripts")
from load_dw import main as load_dw_main

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    "dw_dag",
    default_args=default_args,
    description="Carga los archivos limpios desde el Data Lake a BigQuery",
    schedule_interval=None,  # Ejecutable manualmente
    catchup=False,
)

# Tarea: ejecutar el script que carga los .parquet a BigQuery
load_dw_task = PythonOperator(
    task_id="load_dw_from_datalake",
    python_callable=load_dw_main,
    dag=dag,
)

load_dw_task

