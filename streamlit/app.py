
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
BUSINESS_ID_EL_CAMINO_REAL= "julsvvavzvghwffkkm0nlg";






# === FUNCIÓN DE COMPETENCIA ===


def show_competencia():
  def show_competencia():
    st.title("Competidores y Desempeño de El Camino Real (Categoría: Mexican)")

    # --- CONSULTAS ACTUALIZADAS ---
    camino_real_id = 'julsvvavzvghwffkkm0nlg'  # ID único de El Camino Real

    queries = {
        # 10 negocios mexicanos aleatorios para comparar, incluyendo reseñas
        "df_comp": f"""
            SELECT b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews, r.review_text
            FROM `TU_PROYECTO.dw_restaurantes.dim_business` b
            JOIN `TU_PROYECTO.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.categories LIKE '%Mexican%' AND b.business_id != '{camino_real_id}'
            GROUP BY b.business_name, r.review_text
            ORDER BY RAND() LIMIT 10
        """,
        # Datos de El Camino Real
        "df_camino_real": f"""
            SELECT b.business_id, b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews
            FROM `TU_PROYECTO.dw_restaurantes.dim_business` b
            JOIN `TU_PROYECTO.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.business_id = '{camino_real_id}'
            GROUP BY b.business_id, b.business_name
        """,
        # Pie chart de estrellas solo para El Camino Real
        "df_camino_pie": f"""
            SELECT stars, COUNT(*) AS cantidad
            FROM `TU_PROYECTO.dw_restaurantes.fact_review`
            WHERE business_id = '{camino_real_id}'
            GROUP BY stars ORDER BY stars
        """,
        # Distribución general de categoría "Mexican"
        "df_dist": """
            SELECT r.stars AS star, COUNT(*) AS count
            FROM `TU_PROYECTO.dw_restaurantes.dim_business` b
            JOIN `TU_PROYECTO.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
            WHERE b.categories LIKE '%Mexican%'
            GROUP BY r.stars ORDER BY r.stars
        """
    }

    # Ejecutar consultas
    df_comp = run_query(queries["df_comp"])
    df_camino = run_query(queries["df_camino_real"])
    df_camino_pie = run_query(queries["df_camino_pie"])
    df_dist = run_query(queries["df_dist"])

    # --- VISUALIZACIONES ---
    st.subheader("10 Competidores Aleatorios (Categoría: Mexican)")
    st.dataframe(df_comp)

    st.subheader("Desempeño de El Camino Real")
    st.dataframe(df_camino)

    st.subheader("Reseñas de Competidores Aleatorios")
    if not df_comp.empty:
        st.write("Las reseñas de los competidores aleatorios son las siguientes:")
        for index, row in df_comp.iterrows():
            st.write(f"**{row['business_name']}**:")
            st.write(f"Reseñas: {row['review_text']}")
            st.write(f"Calificación Promedio: {row['avg_rating']}")
            st.write(f"Número de Reseñas: {row['num_reviews']}")
            st.write("---")
    else:
        st.warning("No hay datos de competidores para mostrar.")

    st.subheader("Dispersión: Número de Reseñas vs Calificación Promedio")
    if not df_comp.empty:
        fig, ax = plt.subplots()
        ax.scatter(df_comp['num_reviews'], df_comp['avg_rating'], alpha=0.7, label="Competidores")
        # Agregar El Camino Real al gráfico
        if not df_camino.empty:
            ax.scatter(df_camino['num_reviews'], df_camino['avg_rating'], color='red', label="El Camino Real", s=100)
            ax.annotate("El Camino Real", 
                        (df_camino['num_reviews'][0], df_camino['avg_rating'][0]),
                        textcoords="offset points", xytext=(5,5), ha="left", fontsize=10, color='red')
        ax.set_xlabel("Número de Reseñas")
        ax.set_ylabel("Calificación Promedio")
        ax.set_title("Competidores vs El Camino Real")
        ax.legend()
        st.pyplot(fig)
    else:
        st.warning("No hay datos de competidores para mostrar.")

    st.subheader("Distribución de Calificaciones – El Camino Real")
    if not df_camino_pie.empty:
        fig = px.pie(df_camino_pie, names="stars", values="cantidad",
                     title="Distribución de Calificaciones en El Camino Real",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig)
    else:
        st.info("No hay datos de calificaciones para El Camino Real.")

    st.subheader("Distribución de Estrellas – Categoría Mexican")
    if not df_dist.empty:
        fig, ax = plt.subplots()
        ax.pie(df_dist['count'], labels=df_dist['star'].astype(str), autopct='%1.1f%%', startangle=90)
        ax.axis('equal')
        ax.set_title("Porcentaje de Reseñas por Estrellas en la Categoría Mexican")
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
    st.title("Análisis de Reseñas: El Camino Real")
    st.markdown(""" 
    ## ¿Quiénes somos?
    Somos **HYPE Analytics**, especialistas en proporcionar información relevante para mejorar el rendimiento de nuestros clientes.

    ## Objetivo del Proyecto
    Analizar las reseñas de clientes del restaurante **El Camino Real**, extrayendo KPIs, sentimientos y comparativas que permitan optimizar la estrategia del negocio.

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

    🔗 [Ver Dashboard Interactivo en Looker Studio](https://lookerstudio.google.com/u/0/reporting/df20fc98-f8fa-42bf-8734-92d4ff90e6f5/page/7xbIF)

    📄 [Leer README del Proyecto en GitHub](https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/blob/main/README.md)
    """)
if opcion == "KPIs":
    st.title("KPIs de El Torito - El Camino Real")

    # Business ID fijo para El Torito El Camino Real
    business_id = "julsvvavzvghwffkkm0nlg"

    # Selección de frecuencia de análisis (mensual o anual)
    frecuencia = st.radio("Selecciona la frecuencia de análisis:", ('Mensual', 'Anual'))

    # Selección de rango de fechas
    fecha_desde = st.date_input("Desde:", value=pd.to_datetime("2020-01-01"))
    fecha_hasta = st.date_input("Hasta:", value=pd.to_datetime("2023-12-31"))

    # Ajuste del formato de la fecha dependiendo de la frecuencia seleccionada
    if frecuencia == 'Mensual':
        formato_periodo = "FORMAT_TIMESTAMP('%Y-%m', review_date) AS periodo"
    else:
        formato_periodo = "FORMAT_TIMESTAMP('%Y', review_date) AS periodo"

    # Query para obtener los KPIs
    query_kpi = f"""
    SELECT 
        {formato_periodo},
        COUNT(*) AS volumen_resenas,
        ROUND(AVG(stars), 2) AS calificacion_promedio
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    AND review_date BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
    GROUP BY periodo
    ORDER BY periodo
    """

    # Ejecutar la consulta
    kpis = run_query(query_kpi)

    if not kpis.empty:
        st.write(f"KPIs de El Torito - El Camino Real:")
        st.dataframe(kpis)
    else:
        st.warning("No se encontraron datos para los KPIs en el rango seleccionado.")


if opcion == "Mapas":
    st.title(f"Ubicación de {BUSINESS_NAME_EL_CAMINO_REAL}")

    # Reemplaza estos valores con las coordenadas reales obtenidas de la consulta
    latitude = 34.0522
    longitude = -118.2437

    df_map = pd.DataFrame([{"latitude": latitude, "longitude": longitude}])
    st.map(df_map)


# --- RECOMENDADOR ---
if opcion == "Recomendador":
    st.title("💡 Recomendador para El Camino Real")
    st.markdown("""
    Este módulo analiza las reseñas **positivas** de la competencia directa de *El Camino Real* para detectar las frases más frecuentes
    que los clientes valoran. A partir de eso, generamos recomendaciones accionables para mejorar la propuesta del local.
    """)

    st.divider()
    st.subheader("📦 Cargando reseñas positivas de competidores...")

    BUSINESS_ID_EL_CAMINO_REAL = "julsvvavzvghwffkkm0nlg"

    @st.cache_data
    def cargar_datos():
        query = f"""
        SELECT review_text
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
          ON r.business_id = b.business_id
        WHERE LOWER(b.categories) LIKE '%mexican%'
          AND b.business_id != '{BUSINESS_ID_EL_CAMINO_REAL}'
          AND r.stars >= 4
          AND r.review_text IS NOT NULL
        """
        return client.query(query).to_dataframe()

    df = cargar_datos()

    # --- Procesamiento ---
    df['review_text'] = df['review_text'].str.lower().str.replace(r'[^\w\s]', '', regex=True)

    vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
    X = vectorizer.fit_transform(df['review_text'])
    sum_words = X.sum(axis=0)

    phrases_freq = [(phrase, int(sum_words[0, idx])) for phrase, idx in vectorizer.vocabulary_.items()]
    phrases_freq = sorted(phrases_freq, key=lambda x: x[1], reverse=True)

    # --- Visualizaciones ---
    st.divider()
    st.subheader("🔍 Frases más frecuentes en reseñas positivas")

    top_n = st.slider("Seleccioná cuántas frases mostrar", 5, 50, 20)
    st.dataframe(pd.DataFrame(phrases_freq[:top_n], columns=["Frase", "Frecuencia"]))

    # --- Opcional: nube de palabras ---
    if st.checkbox("Mostrar nube de palabras"):
        wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(phrases_freq[:top_n]))

        # Mostrar la nube de palabras en Streamlit
        st.subheader("📝 Nube de palabras de las frases más mencionadas")
        fig, ax = plt.subplots(figsize=(10, 5))  # Crear un gráfico para la nube de palabras
        ax.imshow(wordcloud, interpolation="bilinear")
        ax.axis("off")  # Quitar los ejes
        st.pyplot(fig)

    # --- Recomendaciones ---
    st.divider()
    st.subheader("💡 Recomendaciones basadas en la voz del cliente")

    recomendaciones = []

    # Mejorar la calidad de la comida
    if any(phrase in [f[0] for f in phrases_freq[:top_n]] for phrase in ["good food", "mexican food", "great food", "delicious food"]):
        recomendaciones.append("🍽️ Mejorar la calidad de los platillos, enfocándose en sabores auténticos y frescura de los ingredientes.")

    # Mejorar el servicio
    if any(phrase in [f[0] for f in phrases_freq[:top_n]] for phrase in ["good service", "great service", "customer service", "service great"]):
        recomendaciones.append("👨‍🍳 Mejorar la atención al cliente y ofrecer un servicio más rápido y personalizado.")

    # Resaltar la autenticidad de los platillos
    if any(phrase in [f[0] for f in phrases_freq[:top_n]] for phrase in ["authentic mexican", "mexican food", "carne asada"]):
        recomendaciones.append("🌮 Resaltar la autenticidad de la comida mexicana en el menú, destacando platillos tradicionales como la carne asada.")

    # Mejorar la visibilidad online
    if any(phrase in [f[0] for f in phrases_freq[:top_n]] for phrase in ["google good", "translated google"]):
        recomendaciones.append("🌐 Mejorar la visibilidad en plataformas como Google Reviews, asegurándose de tener reseñas positivas y respuestas a las mismas.")

    # Crear un ambiente agradable
    if any(phrase in [f[0] for f in phrases_freq[:top_n]] for phrase in ["great place", "love place", "great food"]):
        recomendaciones.append("🏡 Mejorar el ambiente del restaurante, creando un espacio acogedor y cómodo para los comensales.")

    # Mostrar las recomendaciones dinámicas
    for recomendacion in recomendaciones:
        st.markdown(f"- {recomendacion}")

    st.caption("Análisis basado en reseñas positivas de negocios mexicanos con alta calificación.")

# --- ANÁLISIS DE SENTIMIENTO ---
if opcion == "Análisis de Sentimiento":
    st.title("Análisis de Sentimiento de las Reseñas")

    st.markdown("""
    Analizamos las opiniones de los clientes para entender su percepción sobre el restaurante **El Torito**.

    Usamos modelos de **Procesamiento de Lenguaje Natural (NLP)** entrenados para identificar si una reseña es **positiva**, **negativa** o **neutral**.

    ### ¿Por qué es importante?
    - Detectar puntos fuertes (como el servicio o la comida)
    - Identificar áreas de mejora (como tiempos de espera o precios)
    - Tomar decisiones estratégicas basadas en la voz del cliente
    """)

    # Ejemplo visual
    st.subheader("Ejemplo de clasificación de sentimiento")
    example_review = st.text_area("Escribe una reseña para analizar:", "La comida fue excelente pero el servicio muy lento.")
    
    if st.button("Analizar Sentimiento"):
        # Aquí va el modelo real, por ahora es una simulación
        st.success("Resultado: Neutro")



# --- PREDICCIONES ---
if opcion == "Predicciones":
    st.title("Predicción de Rating para El camino real")

    st.markdown("""
    Utilizamos modelos de **Machine Learning** para predecir cuántas estrellas podría recibir una nueva reseña, basándonos en su contenido textual.

    Esto puede ayudar a:
    - Anticipar el impacto de nuevos comentarios
    - Detectar automáticamente reseñas problemáticas
    - Medir la calidad del servicio en tiempo real
    """)

    st.subheader("Ingresá una reseña para predecir su calificación")
    user_input = st.text_area("Reseña del cliente:", "El ambiente es agradable y el personal muy atento.")

    if st.button("Predecir Rating"):
        # Aquí iría el modelo real de ML, por ahora simulamos
        st.success("Predicción: ⭐⭐⭐⭐ (4.0 estrellas)")

# --- COMPETENCIA ---
if opcion == "Competencia":
    st.title("Competidores & Sucursales de El camino real")
    show_competencia()
# --- Página de Distribución de Reseñas ---
# --- Página de Distribución de Reseñas ---
if opcion == "Distribución de Reseñas":
    # --- Distribución General ---
    st.subheader("Distribución General de Reseñas - El Camino Real")

    BUSINESS_ID_EL_CAMINO_REAL = "julsvvavzvghwffkkm0nlg"

    q_general = f"""
    SELECT 
        EXTRACT(YEAR FROM r.review_date) AS anio,
        CASE 
            WHEN r.stars <= 2.5 THEN 'Negativo'
            WHEN r.stars > 2.5 AND r.stars <= 3.5 THEN 'Neutro'
            ELSE 'Positivo'
        END AS sentimiento,
        COUNT(*) AS cantidad
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
      ON r.business_id = b.business_id
    WHERE b.business_id = '{BUSINESS_ID_EL_CAMINO_REAL}'
    GROUP BY anio, sentimiento
    ORDER BY anio;
    """

    df_general = run_query(q_general)

    if not df_general.empty:
        pivot_df = df_general.pivot(index="anio", columns="sentimiento", values="cantidad").fillna(0)
        pivot_df = pivot_df[["Negativo", "Neutro", "Positivo"]]  # Orden deseado

        fig1, ax1 = plt.subplots(figsize=(10, 6))
        ax1.bar(pivot_df.index, pivot_df["Negativo"], label="Negativo", color="red")
        ax1.bar(pivot_df.index, pivot_df["Neutro"], bottom=pivot_df["Negativo"], label="Neutro", color="gray")
        ax1.bar(pivot_df.index, pivot_df["Positivo"], bottom=pivot_df["Negativo"] + pivot_df["Neutro"], label="Positivo", color="green")

        ax1.set_title("Distribución de Sentimientos por Año - El Camino Real")
        ax1.set_xlabel("Año")
        ax1.set_ylabel("Cantidad de Reseñas")
        ax1.legend(title="Sentimiento")
        st.pyplot(fig1)
    else:
        st.warning("No hay datos para El Camino Real.")

if opcion == "Explorar Reseñas":
    st.subheader("Últimas reseñas de  - El Camino Real")

    # Business ID fijo para El Torito El Camino Real
    business_id = "0x808fc9e896f1d559:0x8c0b57a8edd4fd5d"

    # Consulta a BigQuery
    query = f"""
    SELECT review_text, stars, review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    ORDER BY review_date DESC
    LIMIT 10
    """
    reviews = run_query(query)

    if not reviews.empty:
        st.dataframe(reviews)
    else:
        st.warning("No hay reseñas recientes para  - El Camino Real.")
