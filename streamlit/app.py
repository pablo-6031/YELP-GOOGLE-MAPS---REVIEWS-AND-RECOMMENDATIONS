import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_option_menu import option_menu
import pandas as pd
import matplotlib.pyplot as plt

# Configuración de la conexión a BigQuery
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Función para realizar consultas a BigQuery
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# Navegación en el sidebar
with st.sidebar:
    opcion = option_menu("Navegación", 
        ["Inicio", "KPIs", "Mapas", "Recomendador", "Análisis de Sentimiento", "Predicciones", "Distribución de Reseñas", "Competencia", "Explorar Reseñas"],
        icons=['house', 'bar-chart', 'map', 'robot', 'chat', 'graph-up', 'folder', 'flag', 'search'],
        menu_icon="cast", default_index=0, orientation="vertical")

# ID del restaurante "El Torito"
business_id = "7yr4oqcapzbkckrlb3isig"

# Página de Inicio
if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Torito")
    st.markdown("""
    ## ¿Quiénes somos?
    Somos **HYPE Analytics**, especialistas en proporcionar **información relevante** para ayudar a nuestros clientes a mejorar su rendimiento en el mercado. Nuestro enfoque es **analizar reseñas de clientes** para obtener insights valiosos sobre la satisfacción, competencia y oportunidades de mejora.

    ## Objetivo del Proyecto
    El objetivo de este proyecto es realizar un análisis exhaustivo de las **reseñas de clientes** del restaurante **El Torito**. A través de diferentes KPIs, análisis de sentimiento y comparaciones con la competencia, buscamos proporcionar una visión clara y precisa del desempeño del restaurante, con el fin de **mejorar su estrategia de negocio**.

    ## Nuestro Equipo de Trabajo
    Somos un equipo multidisciplinario compuesto por:
    - **Harry Guevara**: Líder del equipo y Functional Analyst, responsable de analizar los requerimientos funcionales y la gestión del proyecto.
    - **Yanina Spina**: Data Scientist, encargada del análisis de datos.
    - **Elvis Bernuy**: Data Analyst, encargado de los análisis exploratorios y creación de visualizaciones.
    - **Pablo Carrizo**: Data Engineer, responsable de la integración de datos y mantenimiento de la infraestructura de datos.
    - **Pablo Mizzau**: Data Engineer, encargado de la optimización y automatización de los procesos de datos.

    ## ¿Qué Hacemos?
    Utilizamos **Google BigQuery** para realizar consultas a las bases de datos de Yelp y Google Maps y obtener información precisa sobre el restaurante. Luego, desarrollamos un **dashboard interactivo** con **Streamlit** para que puedas explorar los datos de manera dinámica.

    A lo largo de esta aplicación, podrás explorar los siguientes análisis:
    - **KPIs clave** como el promedio de ratings y el número de reseñas.
    - **Análisis de sentimiento** de las reseñas de los clientes.
    - **Recomendaciones** basadas en otras reseñas de la misma categoría de restaurante.
    - **Distribución de reseñas** y cómo los clientes califican al restaurante.

    ¡Esperamos que esta información te sea útil y te ayude a tomar decisiones basadas en datos!

    ---
    """)

# Página de KPIs
if opcion == "KPIs":
    st.title("KPIs de El Torito")
    # Query para obtener KPIs del restaurante
    query = f"""
    SELECT 
        AVG(stars) AS avg_rating,
        COUNT(review_text) AS review_count
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    """
    kpis = run_query(query)
    avg_rating = kpis[0]['avg_rating']
    review_count = kpis[0]['review_count']
    
    st.metric("Promedio de Rating", avg_rating)
    st.metric("Número de Reseñas", review_count)
# Lista de los business_id de las sucursales de El Torito
business_ids = [
    "0x80c2bfcf8cc535fd:0xea7ffe91727d1946", 
    "0x80c29794c7e2d44d:0xda1266db4b03e83c",
    "0x80dbf8ec8ade5d45:0x952d1e263dadc54e",
    "0x80dd2ddc6a24e4af:0xcadb76671ddbc94d",
    "0x80dd32f142b8252b:0x1af197c9399f5231",
    "0x80c297ce3cd0f54b:0xececf01e9eeee6f7",
    "0x808fccb4507dc323:0x297d7fd58fc8ff91",
    "0x80ea4fe71c447a1b:0x17232153c8e87293"
]

# Página de Mapas
st.title(f"Mapa de Ubicaciones de El Torito")


# Datos de las sucursales de El Torito
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
    {"latitude": 34.1478, "longitude": -118.2205, "name": "West Covina, CA"},
    {"latitude": 33.7483, "longitude": -116.4194, "name": "San Bernardino, CA"},
    {"latitude": 37.7749, "longitude": -122.4194, "name": "San Leandro, CA"},
    {"latitude": 34.1496, "longitude": -118.4515, "name": "Sherman Oaks, CA"},
    {"latitude": 33.8358, "longitude": -118.3406, "name": "Torrance, CA"},
    {"latitude": 33.7457, "longitude": -117.9389, "name": "Tustin, CA"},
    {"latitude": 34.0686, "longitude": -118.1018, "name": "West Covina, CA"},
    {"latitude": 34.1698, "longitude": -118.1079, "name": "Westminster, CA"},
]

# Convertir a DataFrame
locations_df = pd.DataFrame(locations_data)

# Mostrar el mapa con todas las ubicaciones
st.title("Ubicaciones de las sucursales de El Torito")
st.map(locations_df)

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
