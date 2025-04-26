# 📁 ml/predict_rating.py

import joblib

# Cargar modelo y vectorizador
model = joblib.load("ml/models/model_rating_xgboost.pkl")
vectorizer = joblib.load("ml/models/tfidf_vectorizer_rating.pkl")

# Simulación de nueva reseña
new_reviews = ["El servicio fue excelente y la comida deliciosa."]
X_new = vectorizer.transform(new_reviews)

# Predicción
predicted_rating = model.predict(X_new)
print(f"⭐ Calificación esperada: {predicted_rating[0]:.2f}")
