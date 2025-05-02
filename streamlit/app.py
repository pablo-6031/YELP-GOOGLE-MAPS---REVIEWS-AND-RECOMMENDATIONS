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
import tempfile
import joblib
import urllib

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

        /* Fondo general */
        html, body, [class*="css"] {{
            background-color: #121212;
        }}

        /* Texto blanco en markdown, títulos, listas */
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown p, .stMarkdown li,
        h1, h2, h3, h4, h5, h6, p, .subtitle {{
            color: #FFFFFF !important;
        }}

        /* Métricas */
        .element-container .stMetric {{
            background-color: #1F1F1F;
            border-radius: 8px;
            padding: 10px;
        }}

        /* Inputs de texto y selectbox con texto negro y fondo claro */
        .stTextInput input, .stTextArea textarea {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 1px solid #444;
        }}

        .stSelectbox div[data-baseweb="select"] > div {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
        }}

        /* Botones personalizados */
        button[kind="primary"] {{
            background-color: #1E90FF !important;  /* Azul llamativo */
            color: #FFFFFF !important;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 8px;
        }}

        button[kind="primary"]:hover {{
            background-color: #1C86EE !important;
            color: #FFFFFF !important;
        }}

        button[kind="primary"] {{
        background-color: #1E90FF !important;  /* fondo azul */
        color: white !important;              /* texto blanco */
        border-radius: 8px !important;
        padding: 0.5rem 1rem !important;
        font-weight: bold !important;
        }}

        button[kind="primary"]:hover {{
        background-color: #1C86EE !important;  /* fondo más oscuro al pasar el mouse */
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
with st.sidebar:
    opcion = option_menu(
        "Navegación", 
        ["Inicio", "Explorar Reseñas", "Análisis de Competencia", "Análisis de Sentimiento"],
        icons=["house", "bar-chart", "graph-up-arrow", "emoji-smile"],
        menu_icon="cast",
        default_index=0
    )

if opcion == "Análisis de Sentimiento":
    # Página de la app
    st.title("Análisis de Reseñas de Restaurante")

    # Paso 1: Ingreso de reseña
    texto = st.text_area("Ingresa una reseña:")

# --- INICIO ---

if opcion == "Inicio":
    st.title("Análisis de Reseñas: El Camino Real")
    
    st.markdown(""" 
  
    ## Producto del Proyecto
    Este producto es una aplicación interactiva desarrollada con **Streamlit**, que permite al restaurante **El Camino Real** obtener información valiosa a partir de las reseñas de clientes. A través de la visualización de los datos, ayudamos a los dueños y gerentes a tomar decisiones informadas para mejorar la experiencia de los comensales y optimizar la operación del restaurante.

    ### Funcionalidades:
    - Explorar reseñas
    - Análisis de Sentimiento
    - Comparativas con la competencia

    ### Recursos adicionales:
    
    🔗 [Looker Studio](https://lookerstudio.google.com/u/0/reporting/df20fc98-f8fa-42bf-8734-92d4ff90e6f5/page/7xbIF) Ver Dashboard Interactivo en Looker Studio

    📄 [GitHub](https://github.com/yaninaspina1/YELP-GOOGLE-MAPS---REVIEWS-AND-RECOMMENDATIONS/blob/main/README.md) Leer README del Proyecto en GitHub
    """)


if opcion == "Explorar Reseñas":
 
    st.title("Explorar Reseñas - El Camino Real")

    st.write("""
    En esta sección, podrás explorar las reseñas más recientes de **El Camino Real** y consultar su flujo de opiniones a lo largo del tiempo, mediante un **filtro de fechas personalizado**. Esto te permitirá analizar periodos específicos de interés.

    El análisis incluye métricas como la **calificación promedio** y el **volumen de reseñas**, con visualizaciones disponibles de forma **mensual o anual**, lo que facilita la detección de tendencias y patrones.

    Las reseñas pueden ser filtradas también por **sentimiento** (positivo, neutro o negativo).

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
        st.subheader(f"📊 Analisis - El Camino Real")

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

if opcion == "Análisis de Competencia":
    import streamlit as st
    import pandas as pd
    import matplotlib.pyplot as plt
    from wordcloud import WordCloud
    from sklearn.feature_extraction.text import CountVectorizer

    # 📌 Descripción de la funcionalidad
    st.title("### Explorar Reseñas – La Competencia")
    st.write(
        """
        En esta sección podrás explorar las reseñas de otros restaurantes, ya sea que pertenezcan a la misma categoría gastronómica que El Camino Real o a una diferente. Para comenzar, seleccioná una categoría de comida, aplicá filtros de fecha, y luego elegí un restaurante específico dentro de esa categoría.

        Una vez seleccionado, podrás visualizar sus reseñas más recientes, junto con el análisis del rating promedio y su evolución en el tiempo. Esta herramienta te permite comparar el desempeño y la percepción del público entre distintos competidores del mercado, facilitando la detección de oportunidades o amenazas.

        Explorar cómo los clientes valoran a otros restaurantes te brindará una perspectiva más amplia y estratégica del entorno competitivo.
        """
    )

    # 📦 Cargar los negocios
    @st.cache_data
    def cargar_negocios_por_categoria(categoria):
        try:
            query = f"""
            SELECT business_id, business_name
            FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
            WHERE categories = '{categoria}'
            ORDER BY business_name
            """
            df = run_query(query)
            return df
        except Exception as e:
            st.error(f"Error al cargar los negocios para la categoría '{categoria}': {e}")
            return pd.DataFrame()

    # 🔍 Obtener las 10 categorías más populares
    @st.cache_data
    def cargar_top_categorias():
        try:
            query = """
            SELECT categories
            FROM `shining-rampart-455602-a7.dw_restaurantes.dim_business`
            WHERE categories IS NOT NULL
            GROUP BY categories
            ORDER BY COUNT(*) DESC
            LIMIT 10
            """
            categorias_raw = run_query(query)
            # Devuelvo las categorías más populares
            return categorias_raw["categories"].tolist()
        except Exception as e:
            st.error(f"Error al cargar las categorías: {e}")
            return []

    # 🔍 Mostrar menú con las 10 categorías más reseñadas
    categorias_top10 = cargar_top_categorias()

    categoria_seleccionada = st.selectbox(
        "Elegí una categoría de comida ",
        categorias_top10
    )

    # 📦 Filtros de tiempo
    fecha_inicio = st.date_input("Fecha de inicio", min_value=pd.to_datetime("2010-01-01"))
    fecha_fin = st.date_input("Fecha de fin", max_value=pd.to_datetime("today"))
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
          AND r.review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        """
        return run_query(query)
    except Exception as e:
        st.error(f"Error al cargar las reseñas: {e}")
        return pd.DataFrame()

df_negocios = cargar_negocios_por_categoria(categoria_seleccionada)

if not df_negocios.empty:
    negocios_opciones = df_negocios['business_name'].tolist()
    negocio_seleccionado = st.selectbox("Selecciona un negocio", negocios_opciones)
    business_id_seleccionado = df_negocios[df_negocios['business_name'] == negocio_seleccionado]['business_id'].values[0]
else:
    st.warning("No hay negocios para esta categoría.")
    st.stop()

tipo_reseña = st.selectbox("Selecciona el tipo de reseña", ("Positiva", "Negativa", "Neutra", "Todas"))

if tipo_reseña == "Positiva":
    stars_filter = "r.stars >= 4"
elif tipo_reseña == "Negativa":
    stars_filter = "r.stars <= 2"
elif tipo_reseña == "Neutra":
    stars_filter = "r.stars = 3"
else:
    stars_filter = "1=1"

df = cargar_datos(business_id_seleccionado, stars_filter)

# Verificar si el negocio tiene más de 20 reseñas antes de continuar
if df.empty:
    st.warning("No se encontraron reseñas para este negocio con el tipo seleccionado.")
elif len(df) > 20:
    # Procesar reseñas solo si hay más de 20
    df['review_text'] = df['review_text'].fillna('').str.lower().str.replace(r'[^\w\s]', '', regex=True)
    vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
    X = vectorizer.fit_transform(df['review_text'])
    sum_words = X.sum(axis=0)
    phrases_freq = [(phrase, int(sum_words[0, idx])) for phrase, idx in vectorizer.vocabulary_.items()]
    phrases_freq = sorted(phrases_freq, key=lambda x: x[1], reverse=True)
else:
    st.warning("El negocio seleccionado tiene menos de 20 reseñas.")


    # 📈 Distribución de Sentimientos por Año
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
    AND r.review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
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

    query_estrellas = f"""
    SELECT r.stars, COUNT(*) AS cantidad
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    WHERE r.business_id = '{business_id_seleccionado}'
    AND r.review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    GROUP BY r.stars
    ORDER BY r.stars DESC
    """
    df_estrellas = run_query(query_estrellas)

    st.subheader("📊 Distribución de Estrellas")
    if not df_estrellas.empty:
        fig_pie, ax_pie = plt.subplots(figsize=(6, 6))
        ax_pie.pie(df_estrellas['cantidad'], labels=df_estrellas['stars'], autopct='%1.1f%%',
                    colors=['#ff9999','#66b3ff','#99ff99','#ffcc99','#c2c2f0'])
        ax_pie.set_title(f"Distribución de Estrellas - {negocio_seleccionado}")
        st.pyplot(fig_pie)
    else:
        st.info("No hay suficientes datos para mostrar la distribución de estrellas.")

    st.subheader("🗣️ Reseñas de Usuarios")
    query_resenas = f"""
    SELECT r.review_text, r.stars, r.review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
    WHERE r.business_id = '{business_id_seleccionado}'
    AND r.review_text IS NOT NULL
    AND r.review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY r.review_date DESC
    LIMIT 10
    """
    df_resenas = run_query(query_resenas)
    if not df_resenas.empty:
        st.table(df_resenas)
    else:
        st.info("No hay reseñas recientes disponibles.")



