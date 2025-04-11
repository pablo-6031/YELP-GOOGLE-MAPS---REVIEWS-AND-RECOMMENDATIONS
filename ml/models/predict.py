
"""predict.py 
Lea datos nuevos desde BigQuery
Cargue el modelo y vectorizador entrenados (.pkl)
Genere predicciones de sentimiento
Guarde los resultados con la predicción en un archivo .parquet o .csv
"""

import pandas as pd
import joblib
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Configuración
PROJECT_ID = "shining-rampart-455602-a7"
DATASET = "restaurantes_dw"
TABLE = "google_reviews"
CREDENTIALS_PATH = "/opt/airflow/keys/keyfile.json"  # Ruta dentro del contenedor Airflow

MODEL_PATH = "/opt/airflow/models/sentiment_model.pkl"
VECTORIZER_PATH = "/opt/airflow/models/tfidf_vectorizer.pkl"
OUTPUT_FILE = "/opt/airflow/predictions/predicted_reviews.parquet"

# 1. Leer datos desde BigQuery
def load_data():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
        SELECT review_id, review_text
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE sentiment IS NULL
    """

    df = client.query(query).to_dataframe()
    return df

# 2. Cargar modelo y vectorizador
def load_model_and_vectorizer():
    model = joblib.load(MODEL_PATH)
    vectorizer = joblib.load(VECTORIZER_PATH)
    return model, vectorizer

# 3. Realizar predicciones
def predict_sentiment(df, model, vectorizer):
    X = vectorizer.transform(df["review_text"].astype(str))
    df["predicted_sentiment"] = model.predict(X)
    return df

# 4. Guardar resultados
def save_predictions(df):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Predicciones guardadas en: {OUTPUT_FILE}")

def main():
    print("🚀 Cargando datos desde BigQuery...")
    df = load_data()

    if df.empty:
        print("⚠️ No hay registros nuevos sin sentimiento.")
        return

    print(f"📊 {len(df)} registros cargados para predecir.")
    model, vectorizer = load_model_and_vectorizer()

    print("🔮 Realizando predicciones...")
    df_pred = predict_sentiment(df, model, vectorizer)

    save_predictions(df_pred)

if __name__ == "__main__":
    main()
