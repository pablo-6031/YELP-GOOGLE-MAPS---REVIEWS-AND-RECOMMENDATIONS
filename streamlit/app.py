import os
import requests
import joblib
import streamlit as st

# Función para descargar el archivo
def download_file(url, dest_path):
    response = requests.get(url)
    with open(dest_path, 'wb') as f:
        f.write(response.content)

# Crear el directorio 'ml/models' si no existe
os.makedirs('ml/models', exist_ok=True)

# URLs de los archivos en tu repositorio
url_modelo = "https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/raw/main/ml/models/modelo_sentimiento.joblib"
url_vectorizador = "https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/raw/main/ml/models/vectorizador_tfidf.joblib"

# Descargar los archivos en el directorio local de Streamlit
download_file(url_modelo, "ml/models/modelo_sentimiento.joblib")
download_file(url_vectorizador, "ml/models/vectorizador_tfidf.joblib")

# Cargar los archivos descargados
modelo_sentimiento = joblib.load("ml/models/modelo_sentimiento.joblib")
vectorizador_tfidf = joblib.load("ml/models/vectorizador_tfidf.joblib")

# Ejemplo de uso con Streamlit
st.title("Modelo de Sentimiento")
text_input = st.text_area("Introduce un texto para análisis de sentimiento")

if text_input:
    # Preprocesamiento y predicción
    texto_transformado = vectorizador_tfidf.transform([text_input])
    sentimiento_predicho = modelo_sentimiento.predict(texto_transformado)
    
    st.write(f"Sentimiento Predicho: {'Positivo' if sentimiento_predicho[0] == 1 else 'Negativo'}")
