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
url_logo_restaurante = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/logoCaminoReal.png"
url_fondo = "https://raw.githubusercontent.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/main/streamlit/fondoTorito.png"

# Cargar imágenes
logo_restaurante = Image.open(BytesIO(requests.get(url_logo_restaurante).content))
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
            color: #FFFFFF !important;
            background-color: #121212;
        }}
        h1, h2, h3, h4 {{ color: #FFFFFF !importan; }}
        p {{ color: #FFFFFF; }}
        .subtitle {{ color: #FFFFFF; }}
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

st.image(logo_restaurante, width=200)

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
        ["Inicio", "Explorar Reseñas ", "Análisis Integral de Competencia"],
        icons=["house", "bar-chart", "graph-up-arrow"],
        menu_icon="cast",
        default_index=0
    )


# --- INICIO ---

if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Camino Real")
    
    st.markdown(""" 
  
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
    
    🔗 [Looker Studio](https://lookerstudio.google.com/u/0/reporting/df20fc98-f8fa-42bf-8734-92d4ff90e6f5/page/7xbIF) Ver Dashboard Interactivo en Looker Studio

    📄 [GitHub](https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/blob/main/README.md) Leer README del Proyecto en GitHub
    """)


if opcion == "Explorar Reseñas":
 
    st.title("Explorar Reseñas  El Camino Real")

    # --------------------------------------
    # 🔍 DESCRIPCIÓN DEL FUNCIONAMIENTO:
    # Este código permite al usuario:
    # 1. Seleccionar un rango de fechas para analizar reseñas de un restaurante específico.
    # 2. Ver KPIs como calificación promedio y volumen de reseñas por mes o año.
    # 3. Filtrar reseñas por sentimiento (positivo, neutro o negativo).
    # 4. Visualizar una nube de palabras con los términos más usados en las reseñas.
    # 5. Obtener recomendaciones automáticas basadas en palabras clave dentro de las reseñas.
    # --------------------------------------

  st.write("""
  En esta sección, podrás explorar las reseñas más recientes de **El Camino Real** y consultar su flujo de opiniones a lo largo del tiempo.

  Las reseñas pueden ser filtradas por sentimiento (positivo, neutro o negativo) y mediante un **filtro de fechas personalizado**, lo que te permite analizar periodos específicos de interés. Este análisis incluye métricas como la **calificación promedio** y el **volumen de reseñas**, con visualizaciones disponibles de forma **mensual o anual**, facilitando la detección de tendencias y patrones.

  Además, podrás ver una **nube de palabras** generada a partir del contenido de las reseñas más relevantes, lo que permite identificar de forma visual los temas y aspectos más mencionados por los clientes.
  """)


    # --- Filtros por fecha ---
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", datetime.date(2020, 1, 1))
    with col2:
        fecha_fin = st.date_input("Hasta", datetime.date.today())

    # Business ID fijo
    business_id = "julsvvavzvghwffkkm0nlg"

    # --- Flujo de reseñas---
    st.subheader("📊 KPIs de El Camino Real")

    tipo_periodo = st.selectbox("Seleccionar periodo de tiempo", ["Mensual", "Anual"])
    formato_periodo = "%Y-%m" if tipo_periodo == "Mensual" else "%Y"

    query_kpi = f"""
    SELECT 
        FORMAT_TIMESTAMP('{formato_periodo}', review_date) AS periodo,
        COUNT(*) AS volumen_resenas,
        ROUND(AVG(stars), 2) AS calificacion_promedio
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    AND review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    GROUP BY periodo
    ORDER BY periodo
    """

    df_kpi = run_query(query_kpi)

    if not df_kpi.empty:
        st.subheader(f"KPIs por Periodo - El Camino Real")

        fig1, ax1 = plt.subplots(figsize=(10, 4))
        ax1.plot(df_kpi["periodo"], df_kpi["calificacion_promedio"], marker='o', color='green')
        ax1.set_title("Calificación Promedio por Periodo")
        ax1.set_xlabel("Periodo")
        ax1.set_ylabel("Calificación Promedio")
        ax1.tick_params(axis='x', rotation=45)
        st.pyplot(fig1)

        fig2, ax2 = plt.subplots(figsize=(10, 4))
        ax2.bar(df_kpi["periodo"], df_kpi["volumen_resenas"], color='skyblue')
        ax2.set_title("Volumen de Reseñas por Periodo")
        ax2.set_xlabel("Periodo")
        ax2.set_ylabel("Cantidad de Reseñas")
        ax2.tick_params(axis='x', rotation=45)
        st.pyplot(fig2)
    else:
        st.warning("No hay datos disponibles para El Camino Real en el periodo seleccionado.")

    st.divider()

    # --- Reseñas y filtros ---
    st.subheader("📝 Reseñas de El Camino Real")

    sentimiento = st.selectbox("Filtrar por sentimiento", ["Todos", "Positivo", "Neutro", "Negativo"])
    filtro_sentimiento = ""
    if sentimiento == "Positivo":
        filtro_sentimiento = "AND stars >= 4"
    elif sentimiento == "Negativo":
        filtro_sentimiento = "AND stars <= 2"
    elif sentimiento == "Neutro":
        filtro_sentimiento = "AND stars = 3"

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
        reviews["calificación"] = reviews["stars"].apply(lambda x: "⭐" * int(round(x)))
        st.dataframe(reviews[["review_date", "calificación", "review_text"]])

        if st.button("🔍 Ver palabras más frecuentes"):
            texto = " ".join(reviews["review_text"].dropna().tolist())
            wc = WordCloud(width=800, height=400, background_color="white").generate(texto)

            st.subheader("☁️ Nube de palabras más frecuentes")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wc, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig)

        st.divider()

        # --- Recomendaciones ---
        st.subheader("💡 Recomendaciones basadas en palabras clave")

        palabras_clave = ["food", "service", "price", "taste", "ambience", "attention", "speed", "music"]

        recomendaciones_dict = {
            "Positivo": {
                "food": "Los clientes disfrutan de la comida...",
                "service": "El servicio ha sido bien valorado...",
                "price": "Los precios son bien recibidos...",
                "taste": "El sabor es un punto fuerte...",
                "ambience": "El ambiente agrada a los clientes...",
                "attention": "La atención al cliente ha sido destacada...",
                "speed": "La rapidez del servicio fue positiva...",
                "music": "La música contribuye a una buena experiencia..."
            },
            "Negativo": {
                "food": "Algunos clientes mencionan insatisfacción...",
                "service": "El servicio podría mejorarse...",
                "price": "El precio genera preocupación...",
                "taste": "El sabor parece no cumplir con expectativas...",
                "ambience": "El ambiente no fue del agrado de algunos...",
                "attention": "Hay observaciones sobre la atención...",
                "speed": "La espera fue mencionada negativamente...",
                "music": "Algunos comentarios sobre la música fueron negativos..."
            },
            "Neutro": {
                "food": "La comida fue mencionada sin entusiasmo...",
                "service": "El servicio fue regular...",
                "price": "Los precios no destacaron...",
                "taste": "El sabor podría mejorarse...",
                "ambience": "El ambiente es neutro...",
                "attention": "La atención necesita refinarse...",
                "speed": "El servicio no fue rápido ni lento...",
                "music": "La música fue mencionada pero sin impacto claro..."
            }
        }

        recomendaciones_generadas = {}
        for _, row in reviews.iterrows():
            texto = row["review_text"].lower()
            estrellas = row["stars"]
            sentimiento = "Positivo" if estrellas >= 4 else "Negativo" if estrellas <= 2 else "Neutro"

            for palabra in palabras_clave:
                if palabra in texto and palabra not in recomendaciones_generadas:
                    recomendaciones_generadas[palabra] = recomendaciones_dict[sentimiento][palabra]

        if recomendaciones_generadas:
            st.write("Basado en las reseñas analizadas, te sugerimos lo siguiente:")
            for rec in list(recomendaciones_generadas.values())[:5]:
                st.write("- " + rec)
        else:
            st.write("No se encontraron menciones suficientes para generar recomendaciones.")

if opcion == "Análisis Integral de Competencia":
   # ---------------------- 🔍 Análisis de Competencia -----------------------
    st.subheader("🔍 Análisis de Competencia por Categoría")
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
