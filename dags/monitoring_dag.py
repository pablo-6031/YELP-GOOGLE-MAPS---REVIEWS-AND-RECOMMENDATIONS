"""Este DAG  permitirá:

Verificar si hay archivos nuevos en GCS
Chequear si hay datos nuevos en BigQuery
(Opcionalmente) enviar logs o alertas si se detectan inconsistencias"""


# 📁 dags/monitoring_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage, bigquery
from google.oauth2 import service_account

# Config
CREDENTIALS_PATH = "/opt/airflow/keys/keyfile.json"
BUCKET_NAME = "datalake-restaurantes-2025"
BQ_TABLE = "shining-rampart-455602-a7.restaurantes_dw.google_reviews"

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 0,
}

dag = DAG(
    "monitoring_dag",
    default_args=default_args,
    description="Monitorea GCS y BigQuery para verificar integridad del pipeline",
    schedule_interval="@daily",
    catchup=False,
)

def check_gcs_files():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())
    print(f"📦 Archivos en GCS: {len(blobs)}")
    for blob in blobs[:5]:
        print(f"- {blob.name}")
    if not blobs:
        raise Exception("❌ No hay archivos en el bucket del data lake.")

def check_bigquery_rows():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials)
    query = f"SELECT COUNT(*) as total FROM `{BQ_TABLE}`"
    result = client.query(query).to_dataframe()
    count = result["total"][0]
    print(f"📊 Registros en BigQuery: {count}")
    if count == 0:
        raise Exception("❌ No hay datos en la tabla de BigQuery.")

check_gcs_task = PythonOperator(
    task_id="check_gcs_files",
    python_callable=check_gcs_files,
    dag=dag,
)

check_bq_task = PythonOperator(
    task_id="check_bigquery_rows",
    python_callable=check_bigquery_rows,
    dag=dag,
)

check_gcs_task >> check_bq_task
