import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import os
import glob
import pyarrow.parquet as pq
import pandas as pd
import streamlit as st
import glob
# Configuración de la página
st.set_page_config(page_title="Yelp & Google Reviews - Torito Comida Mexicana", layout="wide")

# Título principal
st.title("📊 Yelp & Google Maps - Reviews y Recomendaciones")

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
        title="Distribución de Sentimientos en Reseñas",
        labels={'Cantidad': 'Número de Reseñas'},
        template="plotly_dark"
    )
    st.plotly_chart(fig)

# Predicciones
elif opcion == "Predicciones":
    st.header("📈 Predicción de Tendencias")
    data_prediccion = pd.DataFrame({
        'Fecha': pd.date_range(start="2023-01-01", periods=12, freq='M'),
        'Reseñas': np.random.randint(1000, 2000, 12)
    })
    data_prediccion['Tendencia'] = np.poly1d(np.polyfit(range(len(data_prediccion)), data_prediccion['Reseñas'], 1))(range(len(data_prediccion)))
    fig = px.line(data_prediccion, x='Fecha', y=['Reseñas', 'Tendencia'], title="Predicción de Crecimiento de Reseñas")
    st.plotly_chart(fig)




# Ruta a la carpeta con los archivos Parquet
ruta_reviews = r"C:\Users\yanin\OneDrive\Desktop\reseñas\*.parquet"
archivos = glob.glob(ruta_reviews)

# Intentar cargar todos los archivos Parquet en fragmentos con PyArrow
try:
    dfs = []
    for archivo in archivos:
        table = pq.read_table(archivo)
        df = table.to_pandas()  # Convertir de Arrow Table a Pandas DataFrame
        dfs.append(df)

    df_reviews = pd.concat(dfs, ignore_index=True)
    st.success(f"Se cargaron correctamente {len(df_reviews)} reseñas.")
except Exception as e:
    st.error(f"❌ Error leyendo los archivos Parquet con PyArrow: {e}")

if 'df_reviews' in locals() and not df_reviews.empty:
    df_reviews["review_date"] = pd.to_datetime(df_reviews["review_date"])

    # Filtrar los datos
    plataformas = st.multiselect("Filtrar por plataforma:", df_reviews["plataforma"].unique(), default=df_reviews["plataforma"].unique())
    ciudades = st.multiselect("Filtrar por ciudad:", df_reviews["ciudad"].unique(), default=df_reviews["ciudad"].unique())
    sentimientos = st.multiselect("Filtrar por sentimiento:", df_reviews["sentimiento"].unique(), default=df_reviews["sentimiento"].unique())
    fecha_inicio = st.date_input("Desde:", df_reviews["review_date"].min().date())
    fecha_fin = st.date_input("Hasta:", df_reviews["review_date"].max().date())

    mask = (
        df_reviews["plataforma"].isin(plataformas) &
        df_reviews["ciudad"].isin(ciudades) &
        df_reviews["sentimiento"].isin(sentimientos) &
        (df_reviews["review_date"] >= pd.to_datetime(fecha_inicio)) &
        (df_reviews["review_date"] <= pd.to_datetime(fecha_fin))
    )

    df_filtrado = df_reviews[mask]
    st.subheader(f"🔍 Se encontraron {len(df_filtrado)} reseñas:")
    st.dataframe(df_filtrado[["review_date", "plataforma", "ciudad", "sentimiento", "calificacion", "texto"]])

    # Gráfico de sentimiento filtrado
    data_sentimiento = df_filtrado["sentimiento"].value_counts().reset_index()
    data_sentimiento.columns = ['Sentimiento', 'Cantidad']
    fig = px.bar(data_sentimiento, x='Sentimiento', y='Cantidad', color='Sentimiento',
                 title="Distribución de Sentimientos (Filtrados)", template="plotly_dark")
    st.plotly_chart(fig)


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
