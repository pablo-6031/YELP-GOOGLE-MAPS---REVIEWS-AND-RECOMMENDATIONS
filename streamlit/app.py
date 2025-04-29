import streamlit as st
from PIL import Image
import base64
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_option_menu import option_menu
import pandas as pd

# URLs raw de GitHub
url_logo_torito = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/logo%20Torito.png"
url_logo_hype = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/logo%20hype.png"
url_fondo = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/fondoTorito.png"

# Cargar imágenes desde las URLs
logo_torito = Image.open(BytesIO(requests.get(url_logo_torito).content))
logo_hype = Image.open(BytesIO(requests.get(url_logo_hype).content))
fondo = Image.open(BytesIO(requests.get(url_fondo).content))

# Función para establecer fondo personalizado
def set_background(image_file):
    with open(image_file, "rb") as f:
        data_url = base64.b64encode(f.read()).decode()
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{data_url}");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Aplicar fondo
set_background(fondo)

# Estilos de texto
st.markdown(
    """
    <style>
    .centered {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        margin-top: 100px;
    }
    .title {
        font-size: 48px;
        font-weight: bold;
        color: white;
        text-shadow: 2px 2px 4px #000;
    }
    .subtitle {
        font-size: 24px;
        color: white;
        text-shadow: 1px 1px 3px #000;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --------- CONEXIÓN A BIGQUERY ---------
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
    return [dict(row) for row in client.query(query).result()]

# --------- SIDEBAR ---------
with st.sidebar:
    opcion = option_menu(
        "Navegación", 
        ["Inicio", "KPIs", "Mapas", "Recomendador", "Análisis de Sentimiento", "Predicciones", "Distribución de Reseñas", "Competencia", "Explorar Reseñas"],
        icons=['house', 'bar-chart', 'map', 'robot', 'chat', 'graph-up', 'folder', 'flag', 'search'],
        menu_icon="cast", default_index=0
    )

# ID del restaurante principal
business_id = "7yr4oqcapzbkckrlb3isig"

# --------- PÁGINAS ---------

# INICIO
if opcion == "Inicio":
    st.markdown('<div class="centered">', unsafe_allow_html=True)
    st.image(logo_torito, width=250)
    st.markdown('<div class="title">Bienvenido a Torito Comida Mexicana</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Análisis de datos · Recomendaciones · Opiniones de clientes</div>', unsafe_allow_html=True)
    st.image(logo_hype, width=100)
    st.markdown('</div>', unsafe_allow_html=True)

    st.title("Análisis de Reseñas: El Torito")
    st.markdown("""
    ## ¿Quiénes somos?
    Somos **HYPE Analytics**, especialistas en análisis de datos aplicados al sector gastronómico.
    
    ## Objetivo del Proyecto
    Analizar reseñas de clientes para ofrecer recomendaciones, insights y métricas relevantes a El Torito.

    ## Equipo
    - **Harry Guevara**: Functional Analyst
    - **Yanina Spina**: Data Scientist
    - **Elvis Bernuy**: Data Analyst
    - **Pablo Carrizo** y **Pablo Mizzau**: Data Engineers

    Utilizamos BigQuery + Streamlit para ofrecerte una app interactiva.
    """)

# KPIs
elif opcion == "KPIs":
    st.title("KPIs de El Torito")
    query = f"""
    SELECT 
        AVG(stars) AS avg_rating,
        COUNT(review_text) AS review_count
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    """
    resultados = run_query(query)
    st.metric("Promedio de Rating", round(resultados[0]['avg_rating'], 2))
    st.metric("Número de Reseñas", resultados[0]['review_count'])

# MAPAS
elif opcion == "Mapas":
    st.title("Mapa de Ubicaciones de El Torito")

    # Lista de ubicaciones (evita duplicados)
    locations_data = [
        {"latitude": 33.8366, "longitude": -117.9145, "name": "Anaheim, CA"},
        {"latitude": 33.8753, "longitude": -117.5664, "name": "Corona, CA"},
        {"latitude": 33.8134, "longitude": -118.0201, "name": "Cypress, CA"},
        {"latitude": 33.9164, "longitude": -118.3526, "name": "Hawthorne, CA"},
        {"latitude": 33.6695, "longitude": -117.8231, "name": "Irvine, CA"},
        {"latitude": 32.7795, "longitude": -117.0340, "name": "La Mesa, CA"},
        {"latitude": 33.8530, "longitude": -118.1326, "name": "Lakewood, CA"},
        {"latitude": 33.9765, "longitude": -118.4682, "name": "Marina del Rey, CA"},
        {"latitude": 37.4284, "longitude": -122.0296, "name": "Milpitas, CA"},
        {"latitude": 36.6002, "longitude": -121.8947, "name": "Monterey, CA"},
        {"latitude": 34.1897, "longitude": -118.5376, "name": "Northridge, CA"},
        {"latitude": 34.0633, "longitude": -117.6130, "name": "Ontario, CA"},
        {"latitude": 34.1457, "longitude": -118.2205, "name": "Palmdale, CA"},
        {"latitude": 34.1478, "longitude": -118.1349, "name": "Pasadena, CA"},
        {"latitude": 33.9533, "longitude": -117.3962, "name": "Riverside, CA"},
        {"latitude": 33.7483, "longitude": -116.4194, "name": "San Bernardino, CA"},
        {"latitude": 37.7749, "longitude": -122.4194, "name": "San Leandro, CA"},
        {"latitude": 34.1496, "longitude": -118.4515, "name": "Sherman Oaks, CA"},
        {"latitude": 33.8358, "longitude": -118.3406, "name": "Torrance, CA"},
        {"latitude": 33.7457, "longitude": -117.9389, "name": "Tustin, CA"},
        {"latitude": 34.0686, "longitude": -118.1018, "name": "West Covina, CA"},
        {"latitude": 34.1698, "longitude": -118.1079, "name": "Westminster, CA"},
    ]

    df_map = pd.DataFrame(locations_data)
    st.map(df_map)


# Página de Recomendador
if opcion == "Recomendador":
    st.title("Recomendador de Restaurantes")
    # Ejemplo de recomendación basada en categoría
    query = f"""
    SELECT business_name, categories, AVG(stars) as avg_rating
    FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
    JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    ON dim_business.business_id = fact_review.business_id
    WHERE categories LIKE '%Mexicano%' AND business_id != '{business_id}'
    GROUP BY business_name, categories
    ORDER BY avg_rating DESC
    LIMIT 5
    """
    recommendations = run_query(query)
    st.write("Recomendaciones basadas en categoría 'Mexicano':")
    st.dataframe(recommendations)

# Página de Análisis de Sentimiento
if opcion == "Análisis de Sentimiento":
    st.title("Análisis de Sentimiento de las Reseñas")
    # Aquí puedes integrar un modelo de ML para analizar el sentimiento
    st.write("Este análisis de sentimiento puede realizarse usando un modelo entrenado para clasificar reseñas como positivas, negativas o neutrales.")

# Página de Predicciones
if opcion == "Predicciones":
    st.title("Predicción de Rating para El Torito")
    # Aquí puedes agregar un modelo ML para predecir el rating futuro basado en el histórico
    st.write("Predicción de rating usando modelos de Machine Learning.")

# Página de Distribución de Reseñas
if opcion == "Distribución de Reseñas por Año y Sucursal":
    st.title("Distribución de Reseñas de El Torito por Año y Sucursal")

# Página de Competencia
if opcion == "Competencia":
    st.title("Análisis de Competencia para El Torito")
    
    # Agregar un try-except para manejar errores en la consulta
    try:
        # Consulta SQL para analizar la competencia
        query = f"""
        SELECT business_name, AVG(stars) AS avg_rating
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review`
        ON dim_business.business_id = fact_review.business_id
        WHERE dim_business.categories LIKE '%Mexicano%' AND dim_business.business_id != '{business_id}'
        GROUP BY business_name
        ORDER BY avg_rating DESC
        LIMIT 5
        """
        
        # Ejecutar la consulta usando la función run_query
        competition = run_query(query)
        
        # Verificar si hay datos y mostrarlos
        if competition:
            st.write("Competencia más cercana:")
            st.dataframe(competition)
        else:
            st.warning("No se encontraron resultados para la competencia.")
    
    except Exception as e:
        st.error(f"Error ejecutando la consulta de competencia: {str(e)}")

# Página de Explorar Reseñas
if opcion == "Explorar Reseñas":
    st.title("Explorar Reseñas de El Torito")

    # Diccionario de sucursales de El Torito con su business_id
    sucursales = {
        "El Torito Sucursal 1": "0x80844a01be660f09:0x661fee46237228d7",
        "El Torito Sucursal 2": "0x808fc9e896f1d559:0x8c0b57a8edd4fd5d",
        "El Torito Sucursal 3": "0x808fccb4507dc323:0x297d7fd58fc8ff91",
        "El Torito Sucursal 4": "0x809ade1814a05da3:0xad096a803d166a4c",
        "El Torito Sucursal 5": "0x80c280a9a282d2e9:0xf3a894f129f38b2f",
        "El Torito Sucursal 6": "0x80c29794c7e2d44d:0xda1266db4b03e83c",
        "El Torito Sucursal 7": "0x80c297ce3cd0f54b:0xececf01e9eeee6f7",
        "El Torito Sucursal 8": "0x80c2b4d2ca3e19c9:0xcf83f70eaba7a203",
        "El Torito Sucursal 9": "0x80c2bfcf8cc535fd:0xea7ffe91727d1946",
        "El Torito Sucursal 10": "0x80dbf8ec8ade5d45:0x952d1e263dadc54e",
        "El Torito Sucursal 11": "0x80dcd43a352d3ae3:0xae921b0c9e9cbdb7",
        "El Torito Sucursal 12": "0x80dd2ddc6a24e4af:0xcadb76671ddbc94d",
        "El Torito Sucursal 13": "0x80dd32f142b8252b:0x1af197c9399f5231",
        "El Torito Sucursal 14": "0x80e9138c2f68bd4f:0x64f25be6f8d56d95",
        "El Torito Sucursal 15": "0x80ea4fe71c447a1b:0x17232153c8e87293",
        "El Torito Sucursal 16": "7yr4oqcapzbkckrlb3isig",
    }

    # Selector de sucursal
    sucursal_seleccionada = st.selectbox("Selecciona una sucursal de El Torito:", list(sucursales.keys()))

    # Obtener el business_id de la sucursal seleccionada
    business_id = sucursales[sucursal_seleccionada]

    # Consulta a la base de datos
    query = f"""
    SELECT review_text, stars, review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    ORDER BY review_date DESC
    LIMIT 10
    """
    reviews = run_query(query)

    st.write(f"Últimas 10 reseñas de {sucursal_seleccionada}:")
    st.dataframe(reviews)
