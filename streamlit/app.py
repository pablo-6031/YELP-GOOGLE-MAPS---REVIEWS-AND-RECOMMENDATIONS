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

# Página de Mapas
if opcion == "Mapas":
    st.title("Mapa de Ubicación de El Torito")
    query = f"""
    SELECT latitude, longitude, business_name
    FROM `shining-rampart-455602-a7.dw_restaurantes.dim_locations`
    WHERE business_id = '{business_id}'
    """
    location = run_query(query)
    lat, lon, name = location[0]['latitude'], location[0]['longitude'], location[0]['business_name']
    
    st.map(pd.DataFrame({'latitude': [lat], 'longitude': [lon]}))  # Mapa con la ubicación de El Torito

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
if opcion == "Distribución de Reseñas":
    st.title("Distribución de Reseñas de El Torito")
    query = f"""
    SELECT stars, COUNT(*) as count
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    GROUP BY stars
    ORDER BY stars
    """
    review_distribution = run_query(query)
    stars = [row['stars'] for row in review_distribution]
    count = [row['count'] for row in review_distribution]
    
    st.bar_chart(pd.DataFrame({'Estrellas': stars, 'Cantidad': count}))

# Página de Competencia
if opcion == "Competencia":
    st.title("Análisis de Competencia para El Torito")
    
    # Agregar un try-except para manejar errores en la consulta
    try:
        # Consulta SQL para analizar la competencia
        query = f"""
        SELECT business_name, AVG(stars) AS avg_rating, COUNT(*) AS review_count
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review`
        ON dim_business.business_id = fact_review.business_id
        WHERE categories LIKE '%Mexicano%' 
        AND business_id != '{business_id}'  -- Excluyendo El Torito
        GROUP BY business_name
        ORDER BY avg_rating DESC
        LIMIT 10
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
    query = f"""
    SELECT review_text, stars, review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    LIMIT 10
    """
    reviews = run_query(query)
    st.write("Últimas 10 reseñas de El Torito:")
    st.dataframe(reviews)
