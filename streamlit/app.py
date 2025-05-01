
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
BUSINESS_ID_EL_CAMINO_REAL = "julsvvavzvghwffkkm0nlg"

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
    st.title("Análisis de Reseñas: El Camino Real")
    
    st.markdown(""" 
    ## ¿Qué hace esta aplicación?
    **El Camino Real** es una plataforma que analiza las reseñas de clientes sobre el restaurante. Utilizando datos de **Yelp** y **Google Maps**, extraemos **KPIs clave**, analizamos **sentimientos**, generamos **recomendaciones personalizadas**, y comparamos el desempeño con la **competencia** para mejorar la estrategia del negocio.

    ## Producto del Proyecto
    Este producto es una aplicación interactiva desarrollada con **Streamlit**, que permite al restaurante **El Camino Real** obtener información valiosa a partir de las reseñas de clientes. A través de la visualización de KPIs y análisis de sentimiento, ayudamos a los dueños y gerentes a tomar decisiones informadas para mejorar la experiencia de los comensales y optimizar la operación del restaurante.

    ### Funcionalidades:
    - KPIs clave (promedio de rating, volumen de reseñas)
    - Análisis de Sentimiento
    - Sistema Recomendador
    - Distribución de reseñas
    - Comparativas con la competencia

    ---
    ### Recursos adicionales:
    


    🔗 [![Looker Studio](https://img.icons8.com/ios/452/google-looker-studio.png)](https://lookerstudio.google.com/u/0/reporting/df20fc98-f8fa-42bf-8734-92d4ff90e6f5/page/7xbIF) Ver Dashboard Interactivo en Looker Studio

    📄 [![GitHub](https://img.icons8.com/ios/452/github.png)](https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/blob/main/README.md) Leer README del Proyecto en GitHub
    """)
if opcion == "Recomendador":
    st.title("💡 Recomendador para El Camino Real")
    st.markdown("""
    Este módulo analiza las reseñas **positivas** de la competencia directa de *El Camino Real* para detectar las frases más frecuentes
    que los clientes valoran. A partir de eso, generamos recomendaciones accionables para mejorar la propuesta del local.
    """)

    st.divider()
    st.subheader("📦 Cargando reseñas positivas de competidores...")

    # Obtener los negocios de competencia directamente de la base de datos
    @st.cache_data
    def cargar_negocios():
        query = """
        SELECT DISTINCT business_id, business_name
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        WHERE LOWER(categories) LIKE '%mexican%' AND business_id != 'julsvvavzvghwffkkm0nlg'
        """
        return client.query(query).to_dataframe()

    # Cargar la lista de negocios para el selector
    df_negocios = cargar_negocios()
    negocios_opciones = df_negocios['business_name'].tolist()
    negocio_seleccionado = st.selectbox("Selecciona un negocio de la competencia", negocios_opciones)

    # Obtener el business_id del negocio seleccionado
    business_id_seleccionado = df_negocios[df_negocios['business_name'] == negocio_seleccionado]['business_id'].values[0]

    # Cargar reseñas de ese negocio
    @st.cache_data
    def cargar_datos(business_id):
        query = f"""
        SELECT review_text
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
          ON r.business_id = b.business_id
        WHERE b.business_id = '{business_id}'
          AND r.stars >= 4
          AND r.review_text IS NOT NULL
        """
        return client.query(query).to_dataframe()

    df = cargar_datos(business_id_seleccionado)

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

    top_n = st.slider("Selecciona cuántas frases mostrar", 5, 50, 20)
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

