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
    
    # Título de la página
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
            texto = " ".join(reviews["review_text"].dropna().tolist())
            wc = WordCloud(width=800, height=400, background_color="white").generate(texto)

            st.subheader("☁️ Nube de palabras más frecuentes")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wc, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig)

        # --- Recomendaciones basadas en palabras clave ---
        st.subheader("💡 Recomendaciones basadas en palabras clave")

        # Definir las palabras clave y la función para generar recomendaciones personalizadas
        palabras_clave = ["food", "service", "price", "taste", "ambience", "attention", "speed", "music"]
        recomendaciones = []

        # Recorrer las reseñas y palabras clave
        for index, row in reviews.iterrows():
            review_text = row["review_text"].lower()  # Convertir texto de reseña a minúsculas
            estrellas = row["stars"]

            for palabra in palabras_clave:
                if palabra in review_text:
                    if estrellas >= 4:  # Si la calificación es positiva (4 o 5 estrellas)
                        if palabra == "food":
                            recomendaciones.append("¡Los clientes elogian la comida! Tal vez podrías seguir innovando en la variedad y la presentación de los platillos.")
                        elif palabra == "service":
                            recomendaciones.append("El servicio ha recibido buenos comentarios. ¿Has considerado reforzar la capacitación para mantener y mejorar esta experiencia?")
                        elif palabra == "price":
                            recomendaciones.append("El precio está bien recibido por los clientes. Podrías explorar nuevas opciones de menú sin alterar mucho los precios.")
                        elif palabra == "taste":
                            recomendaciones.append("¡El sabor es un punto fuerte en tus reseñas! Mantén esa calidad y tal vez podrías probar con nuevos sabores o combinaciones.")
                        elif palabra == "ambience":
                            recomendaciones.append("El ambiente ha sido bien valorado. Considera mantenerlo y tal vez ajustar la decoración o música para seguir creando una experiencia única.")
                        elif palabra == "attention":
                            recomendaciones.append("La atención al cliente ha sido muy positiva. Sigue enfocados en la calidad del servicio para mantener esa excelente experiencia.")
                        elif palabra == "speed":
                            recomendaciones.append("La rapidez en el servicio ha sido destacada. Tal vez podrías explorar mejoras para seguir optimizando los tiempos sin perder calidad.")
                        elif palabra == "music":
                            recomendaciones.append("La música ha sido bien recibida. Tal vez podrías experimentar con nuevas playlists o ajustar el volumen para seguir mejorando el ambiente.")
                    elif estrellas <= 2:  # Si la calificación es negativa (1 o 2 estrellas)
                        if palabra == "food":
                            recomendaciones.append("Parece que los clientes no están tan satisfechos con la comida. ¿Has considerado revisar la calidad de los ingredientes o probar nuevas recetas?")
                        elif palabra == "service":
                            recomendaciones.append("El servicio es una área de mejora. Tal vez podrías enfocarte en capacitar mejor al equipo o aumentar el personal para mejorar la atención.")
                        elif palabra == "price":
                            recomendaciones.append("El precio parece ser una preocupación para algunos clientes. ¿Has considerado ofrecer promociones o ajustar los precios para que sean más atractivos?")
                        elif palabra == "taste":
                            recomendaciones.append("El sabor no ha sido bien recibido. Tal vez sería útil revisar las recetas o los métodos de preparación para asegurarte de que se cumpla con las expectativas de los clientes.")
                        elif palabra == "ambience":
                            recomendaciones.append("El ambiente podría necesitar mejoras. Podrías considerar una renovación en la decoración o ajustar la música y el ambiente general.")
                        elif palabra == "attention":
                            recomendaciones.append("Parece que la atención al cliente necesita mejorar. Tal vez podrías enfocarte en un servicio más personalizado o mejorar la velocidad de respuesta del personal.")
                        elif palabra == "speed":
                            recomendaciones.append("La rapidez en el servicio es una de las áreas críticas. ¿Has considerado hacer ajustes en los procesos para reducir los tiempos de espera?")
                        elif palabra == "music":
                            recomendaciones.append("Algunos clientes mencionan la música de manera negativa. ¿Has considerado cambiar el estilo o ajustar el volumen para que sea más agradable?")
                    else:  # Si la calificación es neutra (3 estrellas)
                        if palabra == "food":
                            recomendaciones.append("La comida ha sido mencionada, pero podría mejorar. ¿Tal vez algunas nuevas opciones o mejoras en la preparación?")
                        elif palabra == "service":
                            recomendaciones.append("El servicio es un tema mencionado. Podrías mejorar la experiencia general haciendo pequeños ajustes, como tiempos de espera más cortos.")
                        elif palabra == "price":
                            recomendaciones.append("El precio es un tema recurrente. Podrías explorar opciones de menús con precios diferentes para atraer a más clientes.")
                        elif palabra == "taste":
                            recomendaciones.append("El sabor tiene comentarios mixtos. Tal vez podrías probar con ingredientes frescos o diferentes combinaciones de sabores.")
                        elif palabra == "ambience":
                            recomendaciones.append("El ambiente es mencionado, aunque podría mejorarse. Tal vez una renovación en la decoración o un ajuste en la iluminación y música podría ayudar.")
                        elif palabra == "attention":
                            recomendaciones.append("La atención parece ser un tema de discusión. ¿Tal vez alguna capacitación extra para el equipo o revisar cómo interactúan con los clientes?")
                        elif palabra == "speed":
                            recomendaciones.append("La rapidez en el servicio podría mejorar. Considera una revisión de los tiempos de espera y cómo hacer más eficiente el proceso.")
                        elif palabra == "music":
                            recomendaciones.append("La música tiene menciones, tal vez podrías probar un estilo diferente o ajustar el volumen para que sea más agradable.")

        # Mostrar las recomendaciones
        if recomendaciones:
            for recomendacion in recomendaciones:
                st.write(recomendacion)
        else:
            st.write("No se encontraron menciones suficientes para generar recomendaciones.")

        st.divider()

        # --- KPIs basados en las fechas de las reseñas ---
        st.subheader("📊 KPIs de El Camino Real")

        # Construcción de la query SQL para los KPIs
        filtro = f"WHERE business_id = '{business_id}'"
        query_kpi = f"""
        SELECT 
            FORMAT_TIMESTAMP('%Y-%m', review_date) AS periodo,
            COUNT(*) AS volumen_resenas,
            ROUND(AVG(stars), 2) AS calificacion_promedio
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
        {filtro}
        AND review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        GROUP BY periodo
        ORDER BY periodo
        """

        # Ejecutar la consulta
        df_kpi = run_query(query_kpi)

        # Visualizar resultados
        if not df_kpi.empty:
            st.subheader(f"KPIs por Periodo - El Camino Real")

            # Gráfico 1: Calificación promedio
            fig1, ax1 = plt.subplots(figsize=(10, 4))
            ax1.plot(df_kpi["periodo"], df_kpi["calificacion_promedio"], marker='o', color='green')
            ax1.set_title("Calificación Promedio por Periodo")
            ax1.set_xlabel("Periodo")
            ax1.set_ylabel("Calificación Promedio")
            ax1.tick_params(axis='x', rotation=45)
            st.pyplot(fig1)

            # Gráfico 2: Volumen de reseñas
            fig2, ax2 = plt.subplots(figsize=(10, 4))
            ax2.bar(df_kpi["periodo"], df_kpi["volumen_resenas"], color='skyblue')
            ax2.set_title("Volumen de Reseñas por Periodo")
            ax2.set_xlabel("Periodo")
            ax2.set_ylabel("Cantidad de Reseñas")
            ax2.tick_params(axis='x', rotation=45)
            st.pyplot(fig2)

        else:
            st.warning("No hay datos disponibles para El Camino Real en el periodo seleccionado.")
    else:
        st.warning("No hay reseñas disponibles para el período seleccionado.")

    # ---------------------- 🔍 Análisis de Competencia -----------------------
if opcion == "Análisis Integral de Competencia":
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

