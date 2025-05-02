import requests

def download_file(url, dest_path):
    response = requests.get(url)
    with open(dest_path, 'wb') as f:
        f.write(response.content)

# URLs de los archivos en tu repositorio
url_modelo = "https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/raw/main/ml/models/modelo_sentimiento.joblib"
url_vectorizador = "https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/raw/main/ml/models/vectorizador_tfidf.joblib"

# Descargar los archivos en el directorio local de Streamlit
download_file(url_modelo, "ml/models/modelo_sentimiento.joblib")
download_file(url_vectorizador, "ml/models/vectorizador_tfidf.joblib")

# Cargar los archivos descargados
import joblib
modelo_sentimiento = joblib.load("ml/models/modelo_sentimiento.joblib")
vectorizador_tfidf = joblib.load("ml/models/vectorizador_tfidf.joblib")
