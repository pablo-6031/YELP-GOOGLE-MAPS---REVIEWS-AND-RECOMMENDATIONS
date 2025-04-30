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

# === CONFIGURACIÓN GENERAL ===

# URLs de imágenes desde GitHub
url_logo_torito = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/logo%20Torito.png"
url_logo_hype = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/logo%20hype.png"
url_fondo = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/fondoTorito.png"

# Cargar imágenes
logo_torito = Image.open(BytesIO(requests.get(url_logo_torito).content))
logo_hype = Image.open(BytesIO(requests.get(url_logo_hype).content))
fondo = Image.open(BytesIO(requests.get(url_fondo).content))

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
            color: #FFFFFF;
            background-color: #121212;
        }}
        h1, h2, h3, h4 {{ color: #FFFFFF; }}
        p {{ color: #E0E0E0; }}
        .subtitle {{ color: #BBBBBB; }}
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

# Mostrar logos
st.markdown(f'<img class="logo-hype" src="{url_logo_hype}">', unsafe_allow_html=True)
st.image(logo_torito, width=200)

# === CONFIGURACIÓN BIGQUERY ===
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def run_query(query):
    return pd.DataFrame([dict(row) for row in client.query(query).result()])

# ID fijo del negocio principal
BUSINESS_ID_EL_TORITO = "7yr4oqcapzbkckrlb3isig"

# === FUNCIÓN DE COMPETENCIA ===

def show_competencia():
    st.title("Competidores y Sucursales de El Torito (Categoría: Mexican)")

    # --- CONSULTAS ---
    queries = {
        "df_comp": """
            SELECT b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews
            FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
            JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.categories LIKE '%Mexican%'
            GROUP BY b.business_name
            ORDER BY RAND() LIMIT 10
        """,
        "df_torito": """
            SELECT b.business_id, b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews
            FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
            JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.business_name LIKE '%Torito%'
            GROUP BY b.business_id, b.business_name
            ORDER BY avg_rating DESC
        """,
        "df_torito_pie": """
            SELECT stars, COUNT(*) AS cantidad
            FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
            WHERE business_id IN (
                SELECT business_id FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
                WHERE business_name LIKE '%Torito%'
            )
            GROUP BY stars ORDER BY stars
        """,
        "df_dist": """
            SELECT r.stars AS star, COUNT(*) AS count
            FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
            JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.categories LIKE '%Mexican%'
            GROUP BY r.stars ORDER BY r.stars
        """
    }

    df_comp = run_query(queries["df_comp"])
    df_torito = run_query(queries["df_torito"])
    df_torito_pie = run_query(queries["df_torito_pie"])
    df_dist = run_query(queries["df_dist"])

    # --- VISUALIZACIONES ---
    st.subheader("10 Competidores Aleatorios (Mexican)")
    st.dataframe(df_comp)

    st.subheader("Sucursales de El Torito")
    st.dataframe(df_torito)

    st.subheader("Dispersión: Número de Reseñas vs Calificación Promedio")
    if not df_comp.empty:
        fig, ax = plt.subplots()
        ax.scatter(df_comp['num_reviews'], df_comp['avg_rating'], alpha=0.7)
        for i, row in df_comp.iterrows():
            ax.annotate(row['business_name'], (row['num_reviews'], row['avg_rating']),
                        textcoords="offset points", xytext=(5,5), ha="left", fontsize=8)
        ax.set_xlabel("Número de Reseñas")
        ax.set_ylabel("Calificación Promedio")
        ax.set_title("Competidores Mexican – Dispersión")
        st.pyplot(fig)
    else:
        st.warning("No hay datos de competidores para mostrar.")

    st.subheader("Distribución de Estrellas – El Torito")
    if not df_torito_pie.empty:
        fig = px.pie(df_torito_pie, names="stars", values="cantidad",
                     title="Distribución de Calificaciones en El Torito",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig)
    else:
        st.info("No hay datos de calificaciones para El Torito.")

    st.subheader("Distribución de Estrellas – Categoría Mexican")
    if not df_dist.empty:
        fig, ax = plt.subplots()
        ax.pie(df_dist['count'], labels=df_dist['star'].astype(str), autopct='%1.1f%%', startangle=90)
        ax.axis('equal')
        ax.set_title("Porcentaje de Reseñas por Estrellas")
        st.pyplot(fig)
    else:
        st.info("No hay datos de distribución de estrellas.")


# --- SIDEBAR ---
with st.sidebar:
    opcion = option_menu("Navegación", 
        ["Inicio", "KPIs", "Mapas", "Recomendador", "Análisis de Sentimiento", "Predicciones", "Distribución de Reseñas", "Competencia", "Explorar Reseñas"],
        icons=['house', 'bar-chart', 'map', 'robot', 'chat', 'graph-up', 'folder', 'flag', 'search'],
        menu_icon="cast", default_index=0, orientation="vertical"
    )

# --- INICIO ---
if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Torito")
    st.markdown(""" 
    ## ¿Quiénes somos?
    Somos **HYPE Analytics**, especialistas en proporcionar información relevante para mejorar el rendimiento de nuestros clientes.

    ## Objetivo del Proyecto
    Analizar las reseñas de clientes del restaurante **El Torito**, extrayendo KPIs, sentimientos y comparativas que permitan optimizar la estrategia del negocio.

    ## Nuestro Equipo de Trabajo
    - **Harry Guevara** – Functional Analyst
    - **Yanina Spina** – Data Scientist
    - **Elvis Bernuy** – Data Analyst
    - **Pablo Carrizo** – Data Engineer
    - **Pablo Mizzau** – Data Engineer

    ## ¿Qué hacemos?
    Consultamos datos de Yelp y Google Maps desde **Google BigQuery**, y desarrollamos esta app interactiva con **Streamlit**.

    ### Funcionalidades:
    - KPIs clave (promedio de rating, volumen de reseñas)
    - Análisis de Sentimiento
    - Sistema Recomendador
    - Distribución de reseñas
    - Comparativas con la competencia
    ---
    """)

# --- KPIs ---
elif opcion == "KPIs":
    st.title("KPIs de El Torito")
    query = """
    SELECT 
        AVG(stars) AS avg_rating,
        COUNT(review_text) AS review_count
    FROM shining-rampart-455602-a7.dw_restaurantes.fact_review
    WHERE business_id = 'your_business_id'
    """
    resultados = run_query(query)
    st.metric("Promedio de Rating", round(resultados[0]['avg_rating'], 2))
    st.metric("Número de Reseñas", resultados[0]['review_count'])

# --- MAPAS ---
elif opcion == "Mapas":
    st.title("Mapa de Ubicaciones de El Torito")
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
    st.map(df_map[['latitude', 'longitude']])

# --- RECOMENDADOR ---
elif opcion == "Recomendador":
    st.title("Recomendador de Restaurantes")
    business_id = 'your_business_id'
    query = f"""
    SELECT business_name, categories, AVG(stars) AS avg_rating
    FROM shining-rampart-455602-a7.dw_restaurantes.dim_business
    JOIN shining-rampart-455602-a7.dw_restaurantes.fact_review
    ON dim_business.business_id = fact_review.business_id
    WHERE categories LIKE '%Mexicano%' AND dim_business.business_id != '{business_id}'
    GROUP BY business_name, categories
    ORDER BY avg_rating DESC
    LIMIT 5
    """
    recommendations = run_query(query)
    st.write("Recomendaciones basadas en categoría 'Mexicano':")
    st.dataframe(recommendations)

# --- ANÁLISIS DE SENTIMIENTO ---
elif opcion == "Análisis de Sentimiento":
    st.title("Análisis de Sentimiento de las Reseñas")
    st.write("Este análisis puede usar modelos entrenados para clasificar reseñas como positivas, negativas o neutras.")

# --- PREDICCIONES ---
elif opcion == "Predicciones":
    st.title("Predicción de Rating para El Torito")
    st.write("Predicción de rating usando modelos de Machine Learning.")

# --- DISTRIBUCIÓN DE RESEÑAS ---
elif opcion == "Distribución de Reseñas":
    st.title("Distribución de Reseñas de El Torito por Año y Sentimiento")

    q_general = """
    SELECT 
        EXTRACT(YEAR FROM r.review_date) AS anio,
        CASE 
            WHEN r.stars <= 2.5 THEN 'Negativo'
            WHEN r.stars > 2.5 AND r.stars <= 3.5 THEN 'Neutro'
            ELSE 'Positivo'
        END AS sentimiento,
        COUNT(*) AS cantidad
    FROM shining-rampart-455602-a7.dw_restaurantes.fact_review r
    JOIN shining-rampart-455602-a7.dw_restaurantes.dim_business b
      ON r.business_id = b.business_id
    WHERE LOWER(b.business_name) LIKE '%torito%'
    GROUP BY anio, sentimiento
    ORDER BY anio;
    """
    df_general = run_query(q_general)
    if not df_general.empty:
        pivot_df = df_general.pivot(index="anio", columns="sentimiento", values="cantidad").fillna(0)
        pivot_df = pivot_df[["Negativo", "Neutro", "Positivo"]]
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(pivot_df.index, pivot_df["Negativo"], label="Negativo", color="red")
        ax.bar(pivot_df.index, pivot_df["Neutro"], bottom=pivot_df["Negativo"], label="Neutro", color="gray")
        ax.bar(pivot_df.index, pivot_df["Positivo"], bottom=pivot_df["Negativo"] + pivot_df["Neutro"], label="Positivo", color="green")
        ax.set_xlabel("Año")
        ax.set_ylabel("Cantidad de Reseñas")
        ax.set_title("Distribución de Reseñas por Año y Sentimiento")
        ax.legend()
        st.pyplot(fig)

# --- COMPETENCIA ---
elif opcion == "Competencia":
    st.title("Competidores & Sucursales de El Torito")
    show_competencia()
elif opcion == "Distribución de Reseñas":
    st.subheader("Distribución General de Reseñas (todas las sucursales)")

    # Consulta general por sentimiento y año
    q_general = """
    SELECT 
        EXTRACT(YEAR FROM r.review_date) AS anio,
        CASE 
            WHEN r.stars <= 2.5 THEN 'Negativo'
            WHEN r.stars > 2.5 AND r.stars <= 3.5 THEN 'Neutro'
            ELSE 'Positivo'
        END AS sentimiento,
        COUNT(*) AS cantidad
    FROM shining-rampart-455602-a7.dw_restaurantes.fact_review r
    JOIN shining-rampart-455602-a7.dw_restaurantes.dim_business b
      ON r.business_id = b.business_id
    WHERE LOWER(b.business_name) LIKE '%torito%'
    GROUP BY anio, sentimiento
    ORDER BY anio;
    """
    df_general = run_query(q_general)

    if not df_general.empty:
        pivot_df = df_general.pivot(index="anio", columns="sentimiento", values="cantidad").fillna(0)
        pivot_df = pivot_df[["Negativo", "Neutro", "Positivo"]]

        fig1, ax1 = plt.subplots(figsize=(10, 6))
        ax1.bar(pivot_df.index, pivot_df["Negativo"], label="Negativo", color="red")
        ax1.bar(pivot_df.index, pivot_df["Neutro"], bottom=pivot_df["Negativo"], label="Neutro", color="gray")
        ax1.bar(pivot_df.index, pivot_df["Positivo"], bottom=pivot_df["Negativo"] + pivot_df["Neutro"], label="Positivo", color="green")

        ax1.set_title("Distribución General de Sentimientos por Año")
        ax1.set_xlabel("Año")
        ax1.set_ylabel("Cantidad de Reseñas")
        ax1.legend(title="Sentimiento")
        st.pyplot(fig1)
    else:
        st.warning("No hay datos para El Torito.")

    # --- Selección de sucursal ---
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

    sucursal_seleccionada = st.selectbox("Selecciona una sucursal de El Torito:", list(sucursales.keys()))
    business_id = sucursales[sucursal_seleccionada]

    # --- Distribución por sucursal ---
    q_reseñas = f"""
    SELECT 
        EXTRACT(YEAR FROM r.review_date) AS anio,
        CASE 
            WHEN r.stars <= 2.5 THEN 'Negativo'
            WHEN r.stars > 2.5 AND r.stars <= 3.5 THEN 'Neutro'
            ELSE 'Positivo'
        END AS sentimiento,
        COUNT(*) AS cantidad
    FROM shining-rampart-455602-a7.dw_restaurantes.fact_review r
    WHERE r.business_id = '{business_id}'
    GROUP BY anio, sentimiento
    ORDER BY anio;
    """
    df_reseñas = run_query(q_reseñas)

    if not df_reseñas.empty:
        st.write(f"Distribución de Reseñas de {sucursal_seleccionada} por Año y Sentimiento")

        df_pivot = df_reseñas.pivot(index='anio', columns='sentimiento', values='cantidad').fillna(0)
        df_pivot = df_pivot[["Negativo", "Neutro", "Positivo"]]

        fig2, ax2 = plt.subplots(figsize=(10, 6))
        df_pivot.plot(kind='bar', stacked=True, ax=ax2)

        ax2.set_title(f"Distribución de Sentimientos de las Reseñas de {sucursal_seleccionada}")
        ax2.set_xlabel("Año")
        ax2.set_ylabel("Número de Reseñas")
        ax2.legend(title="Sentimiento")

        st.pyplot(fig2)
    else:
        st.warning("No hay datos de reseñas para la sucursal seleccionada.")

    # --- Últimas reseñas por sucursal ---
    st.subheader(f"Últimas 10 Reseñas de {sucursal_seleccionada}")
    query_reviews = f"""
    SELECT review_text, stars, review_date
    FROM shining-rampart-455602-a7.dw_restaurantes.fact_review
    WHERE business_id = '{business_id}'
    ORDER BY review_date DESC
    LIMIT 10;
    """
    df_reviews = run_query(query_reviews)
    st.dataframe(df_reviews)

