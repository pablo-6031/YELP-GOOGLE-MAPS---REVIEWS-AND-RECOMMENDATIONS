"""Carga al Data Warehouse desde GCS
Objetivo:
Cargar los archivos .parquet ya transformados desde el data lake (local o GCS) hacia el Data Warehouse (BigQuery)

Supuestos:
Tienes una cuenta de servicio con permisos y su archivo .json (clave de autenticación)
Ya tienes un bucket datalake-restaurantes-2025 (GCS)
Tienes un proyecto en GCP llamado shining-rampart-455602-a7
Y una tabla destino en BigQuery (o quieres crearla desde el script)"""

import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Configura estos valores según tu proyecto
PROJECT_ID = "shining-rampart-455602-a7"
DATASET_ID = "restaurantes_dw"
TABLE_ID = "google_reviews"
GCP_CREDENTIALS_PATH = "path/to/your/keyfile.json"  # <-- Ajustar

# Carpeta local con los .parquet ya limpios (del data lake)
PARQUET_FOLDER = "data_lake/"

def upload_parquet_to_bigquery(file_path, table_id, client):
    print(f"⬆️ Cargando archivo a BigQuery: {file_path}")

    # Leer archivo parquet
    df = pd.read_parquet(file_path)

    # Subir como append a BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Cambia a "WRITE_TRUNCATE" para reemplazar
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()  # Esperar a que termine
    print(f"✅ Archivo cargado en {table_id}")

def main():
    # Autenticación
    credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # Tabla destino completa
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Listar archivos .parquet en la carpeta
    files = [f for f in os.listdir(PARQUET_FOLDER) if f.endswith(".parquet")]

    if not files:
        print("⚠️ No hay archivos .parquet para cargar.")
        return

    print("📁 Archivos encontrados:")
    for f in files:
        print(f"  - {f}")

    confirm = input("\n¿Cargar estos archivos a BigQuery? (S/N): ").strip().lower()
    if confirm != "s":
        print("🚫 Proceso cancelado.")
        return

    for f in files:
        upload_parquet_to_bigquery(os.path.join(PARQUET_FOLDER, f), table_ref, client)

    print("✅ Carga completada.")

if __name__ == "__main__":
    main()


