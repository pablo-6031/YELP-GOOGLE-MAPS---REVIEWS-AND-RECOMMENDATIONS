# app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_option_menu import option_menu
import pandas as pd

# --- CONFIGURACIÓN DE CONEXIÓN A BIGQUERY ---
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# --- FUNCIÓN PARA CONSULTAS A BIGQUERY ---
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# --- ID PRINCIPAL DEL RESTAURANTE ---
main_business_id = "7yr4oqcapzbkckrlb3isig"

# --- SIDEBAR DE NAVEGACIÓN ---
with st.sidebar:
    opcion = option_menu("Navegación", 
        ["Inicio", "KPIs", "Mapas", "Recomendador", "Análisis de Sentimiento"],
        icons=['house', 'bar-chart', 'map', 'robot', 'chat'],
        menu_icon="cast", default_index=0)

# --- CONTENIDO SEGÚN OPCIÓN SELECCIONADA ---
if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Torito")
    st.markdown("""
    ## ¿Quiénes somos?
    Somos **HYPE Analytics**, especialistas en proporcionar **información relevante** para ayudar a nuestros clientes a mejorar su rendimiento en el mercado.

    ## Objetivo
    Realizar un análisis exhaustivo de las **reseñas de clientes** del restaurante **El Torito** usando KPIs, análisis de sentimiento y comparaciones con la competencia.

    ## Equipo
    - **Harry Guevara** – Functional Analyst
    - **Yanina Spina** – Data Scientist
    - **Elvis Bernuy** – Data Analyst
    - **Pablo Carrizo** – Data Engineer
    - **Pablo Mizzau** – Data Engineer

    ## Tecnologías
    - **Google BigQuery** para bases de datos.
    - **Streamlit** para visualización interactiva.

    ¡Esperamos que esta información te ayude a tomar decisiones basadas en datos!
    """)

elif opcion == "KPIs":
    st.title("KPIs de El Torito")
    # Consulta para KPIs
    query = f"""
    SELECT 
        AVG(stars) AS avg_rating,
        COUNT(review_text) AS review_count
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{main_business_id}'
    """
    kpis = run_query(query)
    avg_rating = round(kpis[0]['avg_rating'], 2)
    review_count = kpis[0]['review_count']

    st.metric("⭐ Promedio de Rating", avg_rating)
    st.metric("📝 Número de Reseñas", review_count)

elif opcion == "Mapas":
    st.title("Ubicaciones de las Sucursales de El Torito")
    
    # Datos de las ubicaciones
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
        {"latitude": 33.8484, "longitude": -118.1326, "name": "Redondo Beach, CA"},
        {"latitude": 33.9533, "longitude": -117.3962, "name": "Riverside, CA"},
        {"latitude": 33.7483, "longitude": -116.4194, "name": "San Bernardino, CA"},
        {"latitude": 37.7749, "longitude": -122.4194, "name": "San Leandro, CA"},
        {"latitude": 34.1496, "longitude": -118.4515, "name": "Sherman Oaks, CA"},
        {"latitude": 33.8358, "longitude": -118.3406, "name": "Torrance, CA"},
        {"latitude": 33.7457, "longitude": -117.9389, "name": "Tustin, CA"},
        {"latitude": 34.0686, "longitude": -118.1018, "name": "West Covina, CA"},
        {"latitude": 34.1698, "longitude": -118.1079, "name": "Westminster, CA"},
    ]

    locations_df = pd.DataFrame(locations_data)
    st.map(locations_df)

elif opcion == "Recomendador":
    st.title("Recomendador de Restaurantes")

    query = f"""
    SELECT 
        dim_business.business_name, 
        dim_business.categories, 
        ROUND(AVG(fact_review.stars),2) AS avg_rating
    FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` AS dim_business
    JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` AS fact_review
    ON dim_business.business_id = fact_review.business_id
    WHERE dim_business.categories LIKE '%Mexicano%'
    AND dim_business.business_id != '{main_business_id}'
    GROUP BY dim_business.business_name, dim_business.categories
    ORDER BY avg_rating DESC
    LIMIT 5
    """
    recommendations = run_query(query)
    st.write("Top 5 restaurantes mexicanos recomendados:")
    st.dataframe(recommendations)

elif opcion == "Análisis de Sentimiento":
    st.title("Análisis de Sentimiento de las Reseñas")
    st.write("Aquí puedes integrar un modelo de análisis de sentimientos, como uno basado en Transformers o Scikit-learn.")
    st.info("💡 Ejemplo: Positivo / Negativo basado en las reseñas de Yelp y Google Maps.")

