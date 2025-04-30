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
import openai

# === CONFIGURACIÓN GENERAL ===
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
        .chatbox {{
            position: fixed;
            bottom: 0;
            right: 20px;
            width: 300px;
            height: 400px;
            background-color: #2c2c2c;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.2);
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}
        .chatbox-header {{
            font-size: 18px;
            font-weight: bold;
            color: #ffffff;
        }}
        .chatbox-messages {{
            flex: 1;
            overflow-y: auto;
            margin-bottom: 10px;
            color: #ffffff;
            font-size: 14px;
        }}
        .chatbox-input {{
            width: 100%;
            padding: 10px;
            border-radius: 5px;
            background-color: #3c3c3c;
            color: white;
            border: none;
            font-size: 14px;
        }}
        .chatbox-button {{
            padding: 10px 15px;
            background-color: #4CAF50;
            border: none;
            color: white;
            cursor: pointer;
            border-radius: 5px;
        }}
        .chatbox-button:hover {{
            background-color: #45a049;
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

# Configuración de la página
st.set_page_config(page_title="Asistente Torito", page_icon="🤖")
st.title("🗨️ Asistente Virtual de Torito Comida Mexicana")

# Configurar clave API de OpenAI
openai.api_key = st.secrets["OPENAI_API_KEY"]

# Inicializar historial del chat si no existe
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Diccionario de respuestas fijas
respuestas_fijas = {
    "dashboard": "📊 Podés ver el dashboard haciendo clic en el siguiente enlace:\n\n👉 [Ir al Dashboard](https://tudashboard.streamlit.app)",
    "kpi": "📉 Gracias por informarlo. Avisaremos al área correspondiente para que lo revise.",
    "objetivo": "🎯 El objetivo principal de esta app es ayudarte a analizar el rendimiento de tu restaurante.",
    "falla": "⚠️ Gracias por avisar. El equipo técnico será notificado de inmediato.",
    "error": "⚠️ Gracias por avisar. El equipo técnico será notificado de inmediato.",
    "no funciona": "⚠️ Estamos revisando el sistema. Agradecemos tu paciencia."
}

# Entrada del usuario
user_input = st.chat_input("Escribí tu consulta o comentario aquí...")

if user_input:
    st.session_state.chat_history.append(("usuario", user_input))
    user_input_lower = user_input.lower()

    # Buscar si hay respuesta fija
    respuesta = next((respuesta for keyword, respuesta in respuestas_fijas.items() if keyword in user_input_lower), None)

    if respuesta:
        st.session_state.chat_history.append(("bot", respuesta))
    else:
        with st.spinner("Pensando... 🤔"):
            gpt_response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[ 
                    {"role": "system", "content": "Sos un asistente virtual 24/7 para un restaurante mexicano llamado Torito. Respondé en tono amable y claro."},
                    {"role": "user", "content": user_input}
                ]
            ).choices[0].message["content"]

            st.session_state.chat_history.append(("bot", gpt_response))

# Mostrar historial del chat
st.markdown("""
    <div class="chatbox">
        <div class="chatbox-header">Chatbot</div>
        <div class="chatbox-messages">
            <!-- Mostrar mensajes aquí -->
            """)

# Mostrar mensajes del chat
for speaker, mensaje in st.session_state.chat_history:
    st.markdown(f"<p><strong>{speaker.capitalize()}:</strong> {mensaje}</p>", unsafe_allow_html=True)

# Entrada para nueva consulta
st.markdown("""
        </div>
        <input class="chatbox-input" type="text" placeholder="Escribe tu mensaje..." />
    </div>
""", unsafe_allow_html=True)

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
if opcion == "KPIs":
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
if opcion == "Mapas":
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
if opcion == "Recomendador":
    st.title("💡 Recomendador para Torito Comida Mexicana")
    st.markdown("""
    Este módulo analiza las reseñas **positivas** de la competencia directa de *El Torito* para detectar las frases más frecuentes
    que los clientes valoran. A partir de eso, generamos recomendaciones accionables para mejorar la propuesta del local.
    """)

    st.divider()
    st.subheader("📦 Cargando reseñas positivas de competidores...")

    @st.cache_data
    def cargar_datos():
        query = """
        SELECT review_text
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
          ON r.business_id = b.business_id
        WHERE LOWER(b.categories) LIKE '%mexican%'
          AND LOWER(b.business_name) NOT LIKE '%torito%'
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
    st.write("Este análisis puede usar modelos entrenados para clasificar reseñas como positivas, negativas o neutras.")

# --- PREDICCIONES ---
if opcion == "Predicciones":
    st.title("Predicción de Rating para El Torito")
    st.write("Predicción de rating usando modelos de Machine Learning.")



# --- COMPETENCIA ---
if opcion == "Competencia":
    st.title("Competidores & Sucursales de El Torito")
    show_competencia()
# --- Página de Distribución de Reseñas ---

if opcion == "Distribución de Reseñas":
    # --- Distribución General de todas las sucursales ---
    st.subheader("Distribución General de Reseñas (todas las sucursales)")

    q_general = """
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
    WHERE LOWER(b.business_name) LIKE '%torito%'
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

        ax1.set_title("Distribución General de Sentimientos por Año")
        ax1.set_xlabel("Año")
        ax1.set_ylabel("Cantidad de Reseñas")
        ax1.legend(title="Sentimiento")
        st.pyplot(fig1)
    else:
        st.warning("No hay datos para El Torito.")

    # --- Distribución por sucursal específica ---
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

    sucursal_seleccionada = st.selectbox("Selecciona una sucursal de El Torito:", list(sucursales.keys()))
    business_id = sucursales[sucursal_seleccionada]

    q_reseñas = f"""
    SELECT r.business_id,
           b.business_name,
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
    WHERE b.business_id = '{business_id}'
    GROUP BY r.business_id, b.business_name, anio, sentimiento
    ORDER BY anio
    """

    df_reseñas = run_query(q_reseñas)

    if not df_reseñas.empty:
        st.write(f"Distribución de Reseñas de {sucursal_seleccionada} por Año y Sentimiento")
        
        df_pivot = df_reseñas.pivot_table(index='anio', columns='sentimiento', values='cantidad', aggfunc='sum', fill_value=0)

        fig, ax = plt.subplots(figsize=(10, 6))
        df_pivot.plot(kind='bar', stacked=True, ax=ax)

        ax.set_title(f"Distribución de Sentimientos de las Reseñas de {sucursal_seleccionada}")
        ax.set_xlabel("Año")
        ax.set_ylabel("Número de Reseñas")
        ax.legend(title="Sentimiento")

        st.pyplot(fig)
    else:
        st.warning("No hay datos de reseñas para la sucursal seleccionada.")

    # --- Últimas reseñas por sucursal ---
if opcion == "Explorar Reseñas":
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

