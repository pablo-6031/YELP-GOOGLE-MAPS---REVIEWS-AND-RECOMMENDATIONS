import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import os
import glob
import pyarrow.parquet as pq
from google.cloud import bigquery

# Configuración general
st.set_page_config(page_title="Yelp & Google Reviews - Torito Comida Mexicana", layout="wide")

# Estilo personalizado
st.markdown("""
    <style>
    .stApp {
        background-color: #FFFFFF;  /* Blanco */
        color: #000000;  /* Negro */
        font-family: 'Segoe UI', sans-serif;
    }

    h1, h2, h3 {
        color: #860A35;  /* Rojo Oscuro */
    }

    .css-1d391kg, .css-1d391kg::before {
        background-color: #860A35;  /* Rojo Oscuro */
    }

    .css-1d391kg a, .css-1d391kg span {
        color: #E4C590;  /* Beige Claro */
    }

    .stMetric {
        background-color: #E4C590;  /* Beige Claro */
        border-radius: 10px;
        padding: 10px;
        color: #000000;  /* Negro */
    }

    a {
        color: #860A35 !important;  /* Rojo Oscuro */
        text-decoration: none;
    }

    footer {
        visibility: hidden;
    }

    .css-1v0mbdj {
        padding-bottom: 2rem;
    }

    .sidebar .sidebar-content {
        background-color: #860A35;  /* Rojo Oscuro */
    }

    .css-1lcb4p0 {
        background-color: #860A35 !important;  /* Rojo Oscuro */
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar con logo
with st.sidebar:
    st.image("https://kids.kiddle.co/images/9/90/El_Torito_Logo.jpg", width=180)

# Título
st.title("Test de conexión a BigQuery")

# Conectar a BigQuery
client = bigquery.Client()
query = "SELECT CURRENT_TIMESTAMP() as current_time;"
query_job = client.query(query)
result = query_job.result()

# Mostrar resultados de BigQuery
for row in result:
    st.write(f"Conexión exitosa, hora actual: {row.current_time}")

# Navegación a secciones
st.sidebar.title("Navegación")
opcion = st.sidebar.radio("Ir a:", [
    "Inicio", 
    "KPIs", 
    "Mapas", 
    "Recomendador", 
    "Análisis de Sentimiento", 
    "Predicciones", 
    "Distribución de Reseñas", 
    "Competencia", 
    "Explorar Reseñas"
])

# Página de Inicio
if opcion == "Inicio":
    st.subheader("🔍 Proyecto de Ciencia de Datos orientado a negocio")
    st.markdown(""" 
    Bienvenidos a la demo de análisis de reseñas para **Torito Comida Mexicana**.
    
    Este dashboard presenta un análisis profundo de las reseñas obtenidas de **Yelp** y **Google Maps**, 
    con el objetivo de generar insights accionables para mejorar la experiencia del cliente 
    y apoyar el crecimiento estratégico del negocio.
    """)

    st.header("🎯 Objetivos del Proyecto")
    st.markdown(""" 
    1. **Identificar factores clave** que influyen en las calificaciones otorgadas por los clientes.
    2. **Evaluar el impacto** de esos factores en el comportamiento del consumidor.
    3. **Proponer estrategias basadas en datos** para mejorar la visibilidad y satisfacción del cliente.
    4. **Desarrollar un modelo predictivo** para recomendar ubicaciones óptimas para abrir nuevos restaurantes.
    """)

    st.header("📌 KPIs Iniciales")
    col1, col2, col3 = st.columns(3)
    total_reviews = np.random.randint(10000, 20000)
    avg_rating = round(np.random.uniform(3.5, 5.0), 1)
    locations_analyzed = np.random.randint(40, 60)
    
    col1.metric("Cantidad total de reseñas", f"{total_reviews:,}", "📈")
    col2.metric("Calificación promedio", f"{avg_rating} ★", f"-{round(np.random.uniform(0, 0.3), 1)} desde el mes anterior")
    col3.metric("Ubicaciones analizadas", f"{locations_analyzed} ciudades")

    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/Taco_icon.svg/1024px-Taco_icon.svg.png",
             caption="Proyecto desarrollado para Torito Comida Mexicana", width=200)

# Mapa de ubicaciones
elif opcion == "Mapas":
    st.header("🗺️ Visualización Geográfica de las Ubicaciones de El Torito")
    data = {
        'Nombre': [
            'Anaheim', 'Corona', 'Cypress', 'Hawthorne', 'Irvine', 'La Mesa', 'Lakewood',
            'Marina del Rey', 'Milpitas', 'Monterey', 'Northridge', 'Ontario', 'Palmdale',
            'Pasadena', 'Redondo Beach', 'Riverside', 'San Bernardino', 'San Leandro',
            'Sherman Oaks', 'Torrance', 'Tustin', 'West Covina', 'Woodland Hills', 'Yorba Linda'
        ],
        'Dirección': [
            'Anaheim, CA', 'Corona, CA', 'Cypress, CA', 'Hawthorne, CA', 'Irvine, CA',
            'La Mesa, CA', 'Lakewood, CA', 'Marina del Rey, CA', 'Milpitas, CA', 'Monterey, CA',
            'Northridge, CA', 'Ontario, CA', 'Palmdale, CA', 'Pasadena, CA', 'Redondo Beach, CA',
            'Riverside, CA', 'San Bernardino, CA', 'San Leandro, CA', 'Sherman Oaks, CA',
            'Torrance, CA', 'Tustin, CA', 'West Covina, CA', 'Woodland Hills, CA', 'Yorba Linda, CA'
        ],
        'Latitud': [
            33.8366, 33.8753, 33.8162, 33.9164, 33.6696, 32.8326, 33.8492, 33.9826, 37.4284, 36.6002,
            34.2283, 34.0633, 34.5792, 34.1478, 33.8492, 33.9533, 33.9867, 34.0706, 37.7249, 34.1496,
            33.8358, 34.0686, 34.1706, 34.1812
        ],
        'Longitud': [
            -117.9145, -117.5664, -118.0229, -118.3526, -117.8231, -116.9865, -118.1336, -118.4695,
            -121.8996, -122.0226, -118.5395, -117.6073, -118.1503, -118.1445, -118.3882, -117.3755,
            -117.2898, -122.1561, -118.4452, -118.3402, -118.0297, -118.0351, -118.4441, -118.3431
        ]
    }

    if len(set(len(v) for v in data.values())) != 1:
        st.error("❌ Las listas del diccionario 'data' no tienen la misma longitud.")
    else:
        df = pd.DataFrame(data)
        fig = px.scatter_geo(df,
                             lat='Latitud',
                             lon='Longitud',
                             hover_name='Nombre',
                             hover_data=['Dirección'],
                             title="Restaurantes El Torito en California",
                             template="plotly",
                             projection="albers usa")
        fig.update_geos(showland=True, landcolor="white", showcoastlines=True)
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig)

# Recomendador
elif opcion == "Recomendador":
    st.header("🤖 Sistema de Recomendación")
    st.info("Recomendaciones próximamente...")

# Análisis de Sentimiento
elif opcion == "Análisis de Sentimiento":
    st.header("💬 Análisis de Sentimiento de Reseñas de Clientes")
    data_sentimiento = pd.DataFrame({
        'Sentimiento': ['Positivo', 'Neutral', 'Negativo'],
        'Cantidad': [6200, 1800, 2000]
    })
    fig = px.bar(
        data_sentimiento,
        x='Sentimiento',
        y='Cantidad',
        color='Sentimiento',
        color_discrete_map={'Positivo': 'green', 'Neutral': 'gray', 'Negativo': 'red'},
        title="Distribución de Sentimientos en las Reseñas"
    )
    st.plotly_chart(fig)

# Predicciones
elif opcion == "Predicciones":
    st.header("🔮 Predicciones sobre el Comportamiento de los Clientes")
    st.info("Modelo predictivo en desarrollo...")

# Distribución de Reseñas
elif opcion == "Distribución de Reseñas":
    st.header("📊 Análisis de la Distribución de Reseñas")
    st.info("Distribución próximamente...")

# Competencia
elif opcion == "Competencia":
    st.header("📍 Competencia de El Torito")
    st.info("Análisis de competencia próximamente...")

# Explorar Reseñas
elif opcion == "Explorar Reseñas":
    st.header("📝 Reseñas de Clientes")
    st.info("Exploración de reseñas próximamente...")
# --- Footer del Dashboard ---
st.markdown("---")
st.markdown("### 📚 Documentación")
st.markdown("[Ver README del Proyecto en GitHub](https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS)", unsafe_allow_html=True)

st.markdown("### 🌐 Enlace al Dashboard Online")
st.markdown("[Próximamente en Streamlit Cloud](https://nombreapp.streamlit.app)", unsafe_allow_html=True)

st.markdown("### 👥 Equipo del Proyecto")
st.markdown("""
- **Yanina Spina – Data Scientist** – [GitHub](https://github.com/yaninaspina) | [LinkedIn](https://www.linkedin.com/in/yaninaspina)
- **Harry Guevara – Functional Analyst** – [GitHub](https://github.com/harryguevara) | [LinkedIn](https://www.linkedin.com/in/harryguevara)
- **Elvis Bernuy – Data Analyst** – [GitHub](https://github.com/elvisbernuy) | [LinkedIn](https://www.linkedin.com/in/elvisbernuy)
- **Pablo Mizzau – Data Engineer** – [GitHub](https://github.com/pablomizzau) | [LinkedIn](https://www.linkedin.com/in/pablomizzau)
- **Pablo Carrizo – Data Engineer** – [GitHub](https://github.com/pablocarrizo) | [LinkedIn](https://www.linkedin.com/in/pablocarrizo)
""")

# Estilo extra para ocultar footer de Streamlit
st.markdown("""
<style>
    footer {visibility: hidden;}
    .css-1v0mbdj {padding-bottom: 2rem;}
</style>
""", unsafe_allow_html=True)

# Estilo extra para ocultar footer de Streamlit
st.markdown("""
<style>
    footer {visibility: hidden;}
    .css-1v0mbdj {padding-bottom: 2rem;}
</style>
""", unsafe_allow_html=True)
