from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from google.cloud import storage

# Configuración
LOCAL_PARQUET_FOLDER = "/opt/airflow/data_lake/"
GCP_BUCKET_NAME = "datalake-restaurantes-2025"
GCP_CREDENTIALS = "/opt/airflow/keys/keyfile.json"

default_args = {
    "owner": "harry",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
}

dag = DAG(
    "upload_to_gcs_dag",
    default_args=default_args,
    description="Sube archivos parquet transformados al Data Lake en GCS",
    schedule_interval="@daily",
    catchup=False,
)

def upload_parquet_files_to_gcs():
    print(f"📁 Buscando archivos en: {LOCAL_PARQUET_FOLDER}")
    files = [f for f in os.listdir(LOCAL_PARQUET_FOLDER) if f.endswith(".parquet")]
    if not files:
        print("⚠️ No se encontraron archivos .parquet para subir.")
        return

    client = storage.Client.from_service_account_json(GCP_CREDENTIALS)
    bucket = client.bucket(GCP_BUCKET_NAME)

    for file_name in files:
        local_path = os.path.join(LOCAL_PARQUET_FOLDER, file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(local_path)
        print(f"✅ Archivo subido a GCS: {file_name}")

upload_task = PythonOperator(
    task_id="upload_parquet_files_to_gcs",
    python_callable=upload_parquet_files_to_gcs,
    dag=dag,
)
