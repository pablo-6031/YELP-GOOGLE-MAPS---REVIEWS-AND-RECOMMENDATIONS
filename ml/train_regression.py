# 📁 ml/train_regression.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from xgboost import XGBRegressor
import joblib

# Cargar datos
df = pd.read_parquet("gs://datalake-restaurantes-2025/processed/fact_review.parquet")

# Variables
X_text = df["review_text"]
y = df["stars"]

# Vectorización de texto (TF-IDF)
vectorizer = TfidfVectorizer(max_features=5000)
X = vectorizer.fit_transform(X_text)

# División en entrenamiento/prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

# Modelo de regresión
model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
model.fit(X_train, y_train)

# Evaluación
score = model.score(X_test, y_test)
print(f"⭐ Precisión R2 del modelo: {score:.2f}")

# Guardar modelo y vectorizador
joblib.dump(model, "ml/models/model_rating_xgboost.pkl")
joblib.dump(vectorizer, "ml/models/tfidf_vectorizer_rating.pkl")
