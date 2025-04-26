# 📁 etl/transform_gcs.py

import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import io
import uuid

# Configuración
BUCKET_NAME = "datalake-restaurantes-2025"
RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
CREDENTIALS_PATH = "/opt/airflow/keys/keyfile.json"

# Autenticación
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = storage.Client(credentials=credentials)

# Limpieza
def transform_dataframe(df):
    df["text"] = df["text"].fillna("").astype("string")
    df["review_length"] = df["text"].apply(lambda x: len(str(x).split()))
    df["review_date"] = pd.to_datetime(df["time"], unit="ms", errors="coerce")

    for col in ["user_id", "gmap_id", "name", "state", "city"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for geo_col in ["latitude", "longitude"]:
        if geo_col in df.columns:
            df[geo_col] = pd.to_numeric(df[geo_col], errors="coerce")

    aspectos = {
        "comida": ["comida", "sabor", "plato", "menu", "cocina"],
        "servicio": ["servicio", "mesero", "atención", "amable"],
        "precio": ["precio", "costo", "caro", "barato"],
        "ambiente": ["ambiente", "lugar", "decoración", "ruido"]
    }
    positivas = ["bueno", "excelente", "genial", "rico", "agradable"]
    negativas = ["malo", "horrible", "pésimo", "caro", "sucio"]

    for aspecto, palabras in aspectos.items():
        df[f"mentions_{aspecto}"] = df["text"].apply(lambda x: int(any(p in x.lower() for p in palabras)))
        def detectar_sentimiento(x):
            x = x.lower()
            if any(p in x for p in positivas) and any(k in x for k in palabras):
                return "positivo"
            elif any(p in x for p in negativas) and any(k in x for k in palabras):
                return "negativo"
            else:
                return "neutral"
        df[f"aspect_sentiment_{aspecto}"] = df["text"].apply(detectar_sentimiento)

    df["review_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df.drop_duplicates(inplace=True)
    return df

# Proceso principal
def process_json_files():
    bucket = client.bucket(BUCKET_NAME)
    blobs = client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX)

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        print(f"📥 Procesando: {blob.name}")
        content = blob.download_as_bytes()
        df = pd.read_json(io.BytesIO(content), lines=True)

        df_transformed = transform_dataframe(df)
        now = datetime.now().strftime("%Y%m%d")
        output_blob_name = f"{PROCESSED_PREFIX}{os.path.basename(blob.name).replace('.json', f'_clean_{now}.parquet')}"
        buffer = io.BytesIO()
        df_transformed.to_parquet(buffer, index=False)
        buffer.seek(0)

        processed_blob = bucket.blob(output_blob_name)
        processed_blob.upload_from_file(buffer, content_type="application/octet-stream")
        print(f"✅ Guardado en: {output_blob_name}")

if __name__ == "__main__":
    process_json_files()
