"""Entrenamiento de modelos ML
Objetivo:
Entrenar el modelo de sentimiento usando los datos del Data Warehouse (DW), preferiblemente desde BigQuery
"""


# 📁 ml/train_model.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import joblib
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
PROJECT_ID = "shining-rampart-455602-a7"
DATASET = "restaurantes_dw"
TABLE = "google_reviews"
CREDENTIALS_PATH = "path/to/your/keyfile.json"  # Actualiza con tu ruta real
MODEL_OUTPUT_PATH = "models/sentiment_model.pkl"
VECTORIZER_PATH = "models/tfidf_vectorizer.pkl"

# 1. Leer datos desde BigQuery
def load_data_from_bigquery():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    query = f"""
        SELECT review_text, sentiment
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE sentiment IS NOT NULL
    """

    df = client.query(query).to_dataframe()
    return df

# 2. Entrenar modelo
def train_model(df):
    X_text = df["review_text"]
    y = df["sentiment"]

    vectorizer = TfidfVectorizer(max_features=5000)
    X = vectorizer.fit_transform(X_text)

    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=42)

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    accuracy = model.score(X_test, y_test)

    # Guardar modelo y vectorizador
    joblib.dump(model, MODEL_OUTPUT_PATH)
    joblib.dump(vectorizer, VECTORIZER_PATH)

    print(f"✅ Modelo entrenado con accuracy: {accuracy:.2f}")
    print(f"💾 Guardado en: {MODEL_OUTPUT_PATH}")

def main():
    print("🚀 Cargando datos desde BigQuery...")
    df = load_data_from_bigquery()

    if df.empty:
        print("⚠️ No se encontraron datos para entrenar.")
        return

    print(f"📊 {len(df)} registros cargados.")
    train_model(df)

if __name__ == "__main__":
    main()

