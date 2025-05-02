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
        ["Inicio", "Explorar Reseñas", "Análisis Integral de Competencia"],
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
 
    st.title("Explorar Reseñas - El Camino Real")

    st.write("""
    En esta sección, podrás explorar las reseñas más recientes de **El Camino Real** y consultar su flujo de opiniones a lo largo del tiempo, mediante un **filtro de fechas personalizado**. Esto te permitirá analizar periodos específicos de interés.

    El análisis incluye métricas como la **calificación promedio** y el **volumen de reseñas**, con visualizaciones disponibles de forma **mensual o anual**, lo que facilita la detección de tendencias y patrones.

    Las reseñas pueden ser filtradas también por **sentimiento** (positivo, neutro o negativo), devolviendo sugerencias según el contenido de las opiniones analizadas.

    Además, podrás ver una **nube de palabras** generada a partir del contenido de las reseñas más relevantes, lo que permite identificar visualmente los temas y aspectos más mencionados por los clientes.
    """)

    # --- Filtros por fecha ---
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", datetime.date(2020, 1, 1))
    with col2:
        fecha_fin = st.date_input("Hasta", datetime.date.today())

    business_id = "julsvvavzvghwffkkm0nlg"

    # --- Flujo de reseñas ---
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
    st.subheader("🔍 Análisis de Competencia por Categoría")

    # Cargar categorías de negocios
    @st.cache_data
    def cargar_categorias():
        query = """
        SELECT DISTINCT LOWER(categories) AS categoria
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        WHERE categories IS NOT NULL
        """
        categorias_raw = run_query(query)
        categorias = set()
        for cat in categorias_raw["categoria"]:
            for c in cat.split(","):
                categorias.add(c.strip())
        return sorted(categorias)

    categorias = cargar_categorias()
    categoria_seleccionada = st.selectbox(
        "Elegí una categoría",
        categorias,
        index=categorias.index("mexican") if "mexican" in categorias else 0
    )

    # Cargar negocios
    @st.cache_data
    def cargar_negocios():
        query = """
        SELECT DISTINCT business_id, business_name
        FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
        WHERE LOWER(categories) LIKE '%mexican%' AND business_id != 'julsvvavzvghwffkkm0nlg'
        """
        return run_query(query)

    # Cargar reseñas de un negocio según el tipo de reseña seleccionado
    @st.cache_data
    def cargar_datos(business_id, stars_filter):
        try:
            query = f"""
            SELECT review_text
            FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
            JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
              ON r.business_id = b.business_id
            WHERE b.business_id = '{business_id}'
              AND {stars_filter}
              AND r.review_text IS NOT NULL
            """
            return run_query(query)
        except Exception as e:
            st.error(f"Error al cargar las reseñas: {e}")
            return pd.DataFrame()  # Retornar un DataFrame vacío en caso de error

    # 1. Cargar lista de negocios para seleccionar
    df_negocios = cargar_negocios()
    negocios_opciones = df_negocios['business_name'].tolist()
    negocio_seleccionado = st.selectbox("Selecciona un negocio de la competencia", negocios_opciones)

    # Obtener el business_id del negocio seleccionado
    business_id_seleccionado = df_negocios[df_negocios['business_name'] == negocio_seleccionado]['business_id'].values[0]

    # 2. Seleccionar el tipo de reseña (Positiva, Negativa, Neutra)
    tipo_reseña = st.selectbox("Selecciona el tipo de reseña", ("Positiva", "Negativa", "Neutra"))

    # Obtener el filtro de estrellas según la selección del usuario
    if tipo_reseña == "Positiva":
        stars_filter = "r.stars >= 4"
    elif tipo_reseña == "Negativa":
        stars_filter = "r.stars <= 2"
    else:  # Neutra (3 estrellas)
        stars_filter = "r.stars = 3"

    # 3. Cargar reseñas según el tipo de reseña seleccionado
    df = cargar_datos(business_id_seleccionado, stars_filter)

    if df.empty:
        st.warning("No se encontraron reseñas para este negocio con el tipo seleccionado.")
    else:
        # Procesamiento de reseñas
        df['review_text'] = df['review_text'].fillna('').str.lower().str.replace(r'[^\w\s]', '', regex=True)

        vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
        X = vectorizer.fit_transform(df['review_text'])
        sum_words = X.sum(axis=0)

        phrases_freq = [(phrase, int(sum_words[0, idx])) for phrase, idx in vectorizer.vocabulary_.items()]
        phrases_freq = sorted(phrases_freq, key=lambda x: x[1], reverse=True)

        # Mostrar resultados de frases frecuentes
        st.subheader("🔍 Frases más frecuentes en reseñas")
        top_n = st.slider("Selecciona cuántas frases mostrar", 5, 50, 20)
        st.dataframe(pd.DataFrame(phrases_freq[:top_n], columns=["Frase", "Frecuencia"]))

        # Nube de palabras opcional
        if st.checkbox("Mostrar nube de palabras"):
            wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(phrases_freq[:top_n]))
            st.subheader("📝 Nube de palabras de las frases más mencionadas")
            fig, ax = plt.subplots(figsize=(10, 5))  # Crear un gráfico para la nube de palabras
            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis("off")  # Quitar los ejes
            st.pyplot(fig)

    # 4. Consultar Distribución de Sentimientos por Año
    st.subheader("📈 Distribución de Sentimientos por Año")

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
    WHERE r.business_id = '{business_id_seleccionado}'
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
        ax.set_title(f"Distribución de Sentimientos - {negocio_seleccionado}")
        ax.set_xlabel("Año")
        ax.set_ylabel("Cantidad de Reseñas")
        ax.legend(title="Sentimiento")
        st.pyplot(fig)
    else:
        st.warning("No se encontraron reseñas.")

    st.divider()

    # 5. Mostrar Competencia y Reseñas
    n_competidores = st.slider("Cantidad de competidores a mostrar", 5, 50, 10)

    # Consultar competidores
    query_comp = f"""
    SELECT b.business_name, AVG(r.stars) AS avg_rating, COUNT(r.review_text) AS num_reviews
    FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
    JOIN `shining-rampart-455602-a7.dw_restaurantes.fact_review` r ON b.business_id = r.business_id
    WHERE LOWER(b.categories) LIKE '%mexican%'
    GROUP BY b.business_name
    ORDER BY num_reviews DESC
    LIMIT {n_competidores}
    """
    df_comp = run_query(query_comp)

    # Consultar distribución de estrellas
    query_estrellas = f"""
    SELECT r.stars, COUNT(*) AS cantidad
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    WHERE r.business_id = '{business_id_seleccionado}'
    GROUP BY r.stars
    ORDER BY r.stars DESC
    """
    df_estrellas = run_query(query_estrellas)

    # Mostrar gráficos
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Rating vs. Reseñas")
        if not df_comp.empty:
            fig1, ax1 = plt.subplots()
            ax1.scatter(df_comp["num_reviews"], df_comp["avg_rating"], alpha=0.7)
            for _, row in df_comp.iterrows():
                ax1.annotate(row["business_name"], (row["num_reviews"], row["avg_rating"]),
                             fontsize=7, xytext=(3,3), textcoords='offset points',
                             bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="gray", alpha=0.5))
            ax1.set_xlabel("Número de Reseñas")
            ax1.set_ylabel("Rating Promedio")
            ax1.set_title("Negocios Mexicanos")
            st.pyplot(fig1)
        else:
            st.info("No hay datos suficientes para mostrar la competencia.")

    with col2:
        st.subheader("📊 Distribución de Estrellas")
        if not df_estrellas.empty:
            fig_pie, ax_pie = plt.subplots(figsize=(6, 6))
            ax_pie.pie(df_estrellas['cantidad'], labels=df_estrellas['stars'], autopct='%1.1f%%', colors=['#ff9999','#66b3ff','#99ff99','#ffcc99','#c2c2f0'])
            ax_pie.set_title(f"Distribución de Estrellas - {negocio_seleccionado}")
            st.pyplot(fig_pie)
        else:
            st.info("No hay suficientes datos para mostrar la distribución de estrellas.")

    # 6. Mostrar reseñas recientes del negocio
    st.subheader("🗣️ Reseñas de Usuarios")

    query_resenas = f"""
    SELECT r.review_text, r.stars, r.review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    WHERE r.business_id = '{business_id_seleccionado}'
    AND r.review_text IS NOT NULL
    ORDER BY r.review_date DESC
    LIMIT 10
    """
    df_resenas = run_query(query_resenas)

    if not df_resenas.empty:
        for i, row in df_resenas.iterrows():
            st.markdown(f"**{row['review_date'].date()} – ⭐ {row['stars']}**")
            st.write(f"> {row['review_text']}")
            st.markdown("---")
    else:
        st.info("No se encontraron reseñas disponibles.")

