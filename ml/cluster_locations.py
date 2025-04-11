"""Este script:

Carga datos desde BigQuery con coordenadas

Aplica clustering (ej. KMeans)

Guarda el resultado con un identificador de cluster"""

# 📁 ml/cluster_locations.py

import pandas as pd
from sklearn.cluster import KMeans
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Configuración
PROJECT_ID = "shining-rampart-455602-a7"
DATASET = "restaurantes_dw"
TABLE = "google_reviews"
CREDENTIALS_PATH = "/opt/airflow/keys/keyfile.json"
OUTPUT_PATH = "/opt/airflow/clusters/clustered_locations.parquet"

# 1. Leer coordenadas desde BigQuery
def load_coordinates():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
        SELECT review_id, latitude, longitude
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return client.query(query).to_dataframe()

# 2. Aplicar KMeans clustering
def apply_clustering(df, n_clusters=5):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    coords = df[["latitude", "longitude"]]
    df["cluster"] = kmeans.fit_predict(coords)
    return df

# 3. Guardar resultado
def save_results(df):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Clusters guardados en: {OUTPUT_PATH}")

def main():
    print("📍 Cargando coordenadas desde BigQuery...")
    df = load_coordinates()

    if df.empty:
        print("⚠️ No se encontraron datos geográficos.")
        return

    print(f"📊 {len(df)} ubicaciones cargadas.")
    df_clustered = apply_clustering(df)
    save_results(df_clustered)

if __name__ == "__main__":
    main()
