
import datetime
import streamlit as st
from PIL import Image
import base64
import requests
from io import BytesIO
from google.cloud import bigquery
from google.oauth2 import service_account
from streamlit_option_menu import option_menu
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import WordCloud
import pydeck as pdk
import pandas_gbq
import joblib
import urllib.request

# === CONFIGURACIÓN GENERAL ===



# Estilo global y fondo
def set_background(image):
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue()).decode()
    st.markdown(f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{img_str}");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
        }}
        html, body, [class*="css"] {{
            color: #FFFFFF !important;
            background-color: #121212;
        }}
        h1, h2, h3, h4 {{ color: #FFFFFF !important; }}
        p {{ color: #FFFFFF; }}
        .subtitle {{ color: #FFFFFF; }}
        .css-1r6slb0, .css-1d391kg {{
            background-color: #1E1E1E !important;
            color: #FFFFFF !important;
        }}
        .element-container .stMetric {{
            background-color: #1F1F1F;
            border-radius: 8px;
            padding: 10px;
        }}
        .stTextInput>div>div>input, .stTextArea>div>textarea, .stSelectbox>div>div>div>div {{
            background-color: #1E1E1E;
            color: white;
            border: 1px solid #444;
        }}
        .stSelectbox>div>div>div>div {{
            background-color: #2C2C2C !important;
        }}
        .logo-hype {{
            position: fixed;
            top: 10px;
            left: 10px;
            width: 120px;
        }}
        </style>
    """, unsafe_allow_html=True)

set_background(fondo)
st.image(logo_restaurante, width=200)

# === CONFIGURACIÓN BIGQUERY ===
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
    return pd.DataFrame([dict(row) for row in client.query(query).result()])

# === CARGAR MODELOS LOCALES ===
# Ruta local para los modelos
modelo_sentimiento_path = "path/to/your/local/modelo_sentimiento.joblib"
vectorizador_path = "path/to/your/local/vectorizador_tfidf.joblib"

# Cargar los modelos locales
modelo_sentimiento = joblib.load(modelo_sentimiento_path)
vectorizador = joblib.load(vectorizador_path)

def predecir_sentimiento(texto):
    X_vector = vectorizador.transform([texto])
    pred_sentimiento = modelo_sentimiento.predict(X_vector)[0]
    return pred_sentimiento

def predecir_rating(texto):
    X_vector = vectorizador.transform([texto])
    pred_rating = modelo_sentimiento.predict(X_vector)[0]
    return pred_rating

def generar_nube_palabras(texto):
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(texto)
    return wordcloud

# === NAVEGACIÓN ===
with st.sidebar:
    opcion = option_menu(
        "Navegación", 
        ["Inicio", "Explorar Reseñas", "Análisis Integral de Competencia", "Análisis de Sentimiento"],
        icons=["house", "bar-chart", "graph-up-arrow", "emoji-smile"],
        menu_icon="cast",
        default_index=0
    )

# === PÁGINAS ===

if opcion == "Análisis de Sentimiento":
    st.title("Análisis de Sentimiento de Reseñas")
    texto = st.text_area("Ingresa una reseña:")

    if texto:
        sentimiento = predecir_sentimiento(texto)
        rating = predecir_rating(texto)

        st.subheader("Resultado del análisis:")

        if sentimiento == 1:
            st.success("**Sentimiento:** Positivo")
        else:
            st.error("**Sentimiento:** Negativo")

        st.write(f"**Rating estimado:** {round(rating, 2)} ⭐")

        wordcloud = generar_nube_palabras(texto)
        fig_wc, ax_wc = plt.subplots(figsize=(10, 5))
        ax_wc.imshow(wordcloud, interpolation='bilinear')
        ax_wc.axis("off")
        st.pyplot(fig_wc)

    else:
        st.info("Por favor, ingresa una reseña para analizarla.")

# --- INICIO ---

if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Camino Real")
    col1, col2 = st.columns([1, 2])

    with col1:
        st.image(logo_restaurante, width=200)

    with col2:
        st.write("""
        Bienvenido al análisis de reseñas de **El Camino Real**. En esta aplicación, podrás explorar las reseñas de los clientes, analizar el sentimiento de las opiniones y explorar visualizaciones de las principales métricas de desempeño.
        """)

if opcion == "Explorar Reseñas":
    st.title("Explorar Reseñas - El Camino Real")

    st.write("""
    En esta sección, podrás explorar las reseñas más recientes de **El Camino Real** y consultar su flujo de opiniones a lo largo del tiempo, mediante un **filtro de fechas personalizado**. Esto te permitirá analizar periodos específicos de interés.

    El análisis incluye métricas como la **calificación promedio** y el **volumen de reseñas**, con visualizaciones disponibles de forma **mensual o anual**, lo que facilita la detección de tendencias y patrones.

    Las reseñas pueden ser filtradas también por **sentimiento** (positivo, neutro o negativo), devolviendo sugerencias según el contenido de las opiniones analizadas.

    Además, podrás ver una **nube de palabras** generada a partir del contenido de las reseñas más relevantes, lo que permite identificar visualmente los temas y aspectos más mencionados por los clientes.
    """)

if opcion == "Análisis Integral de Competencia":
    st.markdown("### Selección de categoría de comida")
    st.write(
        """
        En esta sección podés elegir una categoría de lugares gastronómicos.
        El menú muestra las **10 categorías más populares** en base a la cantidad de reseñas registradas 
        en nuestra base de datos. Esto permite enfocar los análisis en los rubros con mayor actividad.
        """
    )
