import datetime 
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
import pydeck as pdk
import pandas_gbq
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



 # --- SIDEBAR ---
with st.sidebar:
    opcion = option_menu(
        "Navegación", 
        ["Inicio", "Explorar Reseñas y KPIs", "Análisis Integral de Competencia"],
        icons=["house", "bar-chart", "graph-up-arrow"],
        menu_icon="cast",
        default_index=0
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

  

if opcion == "Explorar Reseñas y KPIs":
    st.title("Explorar Reseñas y KPIs de El Camino Real")
    
    # Breve explicación introductoria
    st.write("""
    En esta sección, podrás explorar las reseñas más recientes de **El Camino Real** y revisar los KPIs de desempeño.
    Las reseñas se pueden filtrar por sentimiento (positivo, neutro, negativo) y por fecha, mientras que los KPIs permiten ver el comportamiento general de las reseñas, incluyendo la calificación promedio y el volumen de reseñas por periodo.
    """)

    # --- Filtro por fecha ---
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", datetime.date(2020, 1, 1))
    with col2:
        fecha_fin = st.date_input("Hasta", datetime.date.today())

    # --- EXPLORAR RESEÑAS Y KPIs ---
    st.subheader("📝 Reseñas y KPIs de El Camino Real")

    # Business ID fijo
    business_id = "julsvvavzvghwffkkm0nlg"  # ID del negocio para El Camino Real

    # Filtro por sentimiento
    sentimiento = st.selectbox("Filtrar por sentimiento", ["Todos", "Positivo", "Neutro", "Negativo"])
    filtro_sentimiento = ""
    if sentimiento == "Positivo":
        filtro_sentimiento = "AND stars >= 4"
    elif sentimiento == "Negativo":
        filtro_sentimiento = "AND stars <= 2"
    elif sentimiento == "Neutro":
        filtro_sentimiento = "AND stars = 3"

    # Consulta SQL para obtener las reseñas filtradas
    query_reseñas = f"""
    SELECT review_text, stars, review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    AND review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    {filtro_sentimiento}
    ORDER BY review_date DESC
    LIMIT 100
    """

    reviews = run_query(query_reseñas)

    if not reviews.empty:
        # Mostrar las reseñas
        reviews["calificación"] = reviews["stars"].apply(lambda x: "⭐" * int(round(x)))
        st.dataframe(reviews[["review_date", "calificación", "review_text"]])

        # Botón para ver nube de palabras
        if st.button("🔍 Ver palabras más frecuentes"):
            from wordcloud import WordCloud

            texto = " ".join(reviews["review_text"].dropna().tolist())
            wc = WordCloud(width=800, height=400, background_color="white").generate(texto)

            st.subheader("☁️ Nube de palabras más frecuentes")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wc, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig)

        # --- RECOMENDACIONES ---  
        st.subheader("💡 Recomendaciones basadas en la voz del cliente")

        recomendaciones = []

        # Analizar las palabras más frecuentes en las reseñas
        from collections import Counter

        # Extraemos las palabras más frecuentes
        palabras = " ".join(reviews["review_text"].dropna().tolist()).lower().split()
        palabras_comunes = Counter(palabras).most_common(10)

        # Crear recomendaciones basadas en palabras clave
        top_n = 10  # Número de palabras clave más comunes
        for word, count in palabras_comunes[:top_n]:
            if word in ["comida", "sabrosa", "deliciosa", "plato", "sabor", "fresca"]:  # Términos relacionados con la comida
                recomendaciones.append("🍽️ Mejorar la calidad de los platillos, enfocándose en sabores auténticos y frescura de los ingredientes.")
            elif word in ["servicio", "atención", "rápido", "amable"]:  # Términos relacionados con el servicio
                recomendaciones.append("👨‍🍳 Mejorar la atención al cliente y ofrecer un servicio más rápido y personalizado.")
            elif word in ["ambiente", "lugar", "acogedor", "cómodo"]:  # Términos relacionados con el ambiente
                recomendaciones.append("🏡 Mejorar el ambiente del restaurante, creando un espacio acogedor y cómodo para los comensales.")
            elif word in ["precio", "bueno", "valor"]:  # Términos relacionados con el precio
                recomendaciones.append("💸 Ofrecer precios competitivos que resalten el valor de la calidad de los platillos.")
            elif word in ["google", "reseñas", "opciones"]:  # Términos relacionados con visibilidad online
                recomendaciones.append("🌐 Mejorar la visibilidad en plataformas como Google Reviews, asegurándose de tener reseñas positivas y respuestas a las mismas.")

        # Mostrar las recomendaciones dinámicas
        if recomendaciones:
            for recomendacion in recomendaciones:
                st.markdown(f"- {recomendacion}")
        else:
            st.warning("No se encontraron recomendaciones basadas en las palabras clave.")

        st.caption("Análisis basado en reseñas filtradas de El Camino Real.")
        
    else:
        st.warning("No hay reseñas disponibles para el período seleccionado.")
        
    st.divider()


    # ---------------------- 💡 Recomendador -----------------------
    st.subheader("💡 Recomendador basado en reseñas")

    @st.cache_data
    def cargar_negocios():
        query = """
        SELECT DISTINCT business_id, business_name
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        WHERE LOWER(categories) LIKE '%mexican%' AND business_id != 'julsvvavzvghwffkkm0nlg'
        """
        return client.query(query).to_dataframe()

    df_negocios = cargar_negocios()
    negocio_seleccionado = st.selectbox("Selecciona un negocio (Recomendador)", df_negocios['business_name'].tolist())
    business_id_seleccionado = df_negocios[df_negocios['business_name'] == negocio_seleccionado]['business_id'].values[0]

    tipo_reseña = st.selectbox("Tipo de reseña", ("Positiva", "Negativa", "Neutra"))
    stars_filter = {"Positiva": "r.stars >= 4", "Negativa": "r.stars <= 2", "Neutra": "r.stars = 3"}[tipo_reseña]

    @st.cache_data
    def cargar_datos(business_id, stars_filter):
        query = f"""
        SELECT review_text
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
        ON r.business_id = b.business_id
        WHERE b.business_id = '{business_id}' AND {stars_filter} AND r.review_text IS NOT NULL
        """
        return client.query(query).to_dataframe()

    df = cargar_datos(business_id_seleccionado, stars_filter)
    if df.empty:
        st.warning("No se encontraron reseñas.")
    else:
        df['review_text'] = df['review_text'].fillna('').str.lower().str.replace(r'[^\w\s]', '', regex=True)
        vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
        X = vectorizer.fit_transform(df['review_text'])
        sum_words = X.sum(axis=0)
        phrases_freq = [(phrase, int(sum_words[0, idx])) for phrase, idx in vectorizer.vocabulary_.items()]
        phrases_freq = sorted(phrases_freq, key=lambda x: x[1], reverse=True)
        top_n = st.slider("Frases más frecuentes", 5, 50, 20)
        st.dataframe(pd.DataFrame(phrases_freq[:top_n], columns=["Frase", "Frecuencia"]))

        if st.checkbox("Mostrar nube de palabras"):
            wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(phrases_freq[:top_n]))
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig)

    st.divider()

    # ---------------------- 📈 Distribución de Reseñas -----------------------
    st.subheader("📈 Distribución de Sentimientos por Año")

    @st.cache_data
    def cargar_negocios_disponibles():
        query = """
        SELECT DISTINCT b.business_id, b.business_name
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
        ON r.business_id = b.business_id
        WHERE r.review_text IS NOT NULL
        """
        return run_query(query)

    negocios_df = cargar_negocios_disponibles()
    negocio_elegido = st.selectbox("Negocio (Distribución)", negocios_df['business_name'].tolist())
    business_id = negocios_df[negocios_df['business_name'] == negocio_elegido]['business_id'].values[0]

    query_sentimiento = f"""
    SELECT 
        EXTRACT(YEAR FROM r.review_date) AS anio,
        CASE 
            WHEN r.stars <= 2.5 THEN 'Negativo'
            WHEN r.stars > 2.5 AND r.stars <= 3.5 THEN 'Neutro'
            ELSE 'Positivo'
        END AS sentimiento,
        COUNT(*) AS cantidad
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    WHERE r.business_id = '{business_id}'
    GROUP BY anio, sentimiento
    ORDER BY anio
    """
    df_general = run_query(query_sentimiento)

    if not df_general.empty:
        pivot_df = df_general.pivot(index="anio", columns="sentimiento", values="cantidad").fillna(0)
        pivot_df = pivot_df[["Negativo", "Neutro", "Positivo"]]
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(pivot_df.index, pivot_df["Negativo"], label="Negativo", color="red")
        ax.bar(pivot_df.index, pivot_df["Neutro"], bottom=pivot_df["Negativo"], label="Neutro", color="gray")
        ax.bar(pivot_df.index, pivot_df["Positivo"], bottom=pivot_df["Negativo"] + pivot_df["Neutro"], label="Positivo", color="green")
        ax.set_title(f"Distribución de Sentimientos - {negocio_elegido}")
        ax.set_xlabel("Año")
        ax.set_ylabel("Cantidad de Reseñas")
        ax.legend(title="Sentimiento")
        st.pyplot(fig)
    else:
        st.warning("No se encontraron reseñas.")

    st.divider()

    # ---------------------- 🔍 Análisis de Competencia -----------------------
    st.subheader("🔍 Análisis de Competencia por Categoría")

    categoria = st.text_input("Ingresá una categoría", value="Mexican")
    n_competidores = st.slider("Cantidad de competidores a mostrar", 5, 50, 10)

    if categoria:
        query_comp = f"""
        SELECT b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
        JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
        WHERE LOWER(b.categories) LIKE '%{categoria.lower()}%'
        GROUP BY b.business_name
        ORDER BY RAND()
        LIMIT {n_competidores}
        """

        query_dist = f"""
        SELECT r.stars AS star, COUNT(*) AS count
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
        JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
        WHERE LOWER(b.categories) LIKE '%{categoria.lower()}%'
        GROUP BY r.stars
        ORDER BY r.stars
        """

        df_comp = run_query(query_comp)
        df_dist = run_query(query_dist)

        st.dataframe(df_comp)

        if not df_comp.empty:
            fig1, ax1 = plt.subplots()
            ax1.scatter(df_comp["num_reviews"], df_comp["avg_rating"], alpha=0.7)
            for _, row in df_comp.iterrows():
                ax1.annotate(row["business_name"], (row["num_reviews"], row["avg_rating"]), fontsize=7, xytext=(3,3), textcoords='offset points')
            ax1.set_xlabel("Número de Reseñas")
            ax1.set_ylabel("Rating Promedio")
            ax1.set_title(f"Competencia – Categoría: {categoria.title()}")
            st.pyplot(fig1)

        if not df_dist.empty:
            fig2, ax2 = plt.subplots()
            ax2.pie(df_dist['count'], labels=df_dist['star'], autopct='%1.1f%%', startangle=90)
            ax2.axis('equal')
            st.pyplot(fig2)
    else:
        st.info("Por favor, ingresá una categoría válida.")

