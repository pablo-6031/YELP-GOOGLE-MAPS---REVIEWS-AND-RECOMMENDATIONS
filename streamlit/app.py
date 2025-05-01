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


# Función para ejecutar la consulta
def run_query(query):
    try:
        df = pandas_gbq.read_gbq(query, project_id="shining-rampart-455602-a7", dialect='standard')
        return df
    except Exception as e:
        st.error(f"❌ Error al ejecutar la consulta: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

# Función para mostrar el análisis de competencia
# Función para mostrar el análisis de competencia
def show_competencia():
    st.title("🔍 Análisis de Competencia por Categoría")

    # --- INPUT DINÁMICO (Selector de Categoría) ---
    categorias_disponibles = ['Mexican', 'Pizza', 'Chinese', 'Italian', 'Indian', 'Japanese', 'Thai', 'American']
    categoria = st.selectbox("🍽️ Elige una categoría", categorias_disponibles)

    # Verificar que se haya seleccionado una categoría válida
    if not categoria:
        st.warning("⚠️ Por favor selecciona una categoría para continuar.")
        return

    n_competidores = st.slider("📊 Número de competidores aleatorios a mostrar", min_value=5, max_value=50, value=10)

    # --- QUERIES DINÁMICAS ---
    query_competidores = f"""
        SELECT 
          b.business_name,
          l.latitude,
          l.longitude,
          AVG(r.stars) AS avg_rating,
          COUNT(r.review_text) AS num_reviews
        FROM 
          `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
        JOIN 
          `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
          ON b.business_id = r.business_id
        JOIN
          `shining-rampart-455602-a7.dw_restaurantes.dim_locations` l
          ON b.business_id = l.business_id
        WHERE 
          LOWER(b.categories) LIKE '%{categoria.lower()}%'
        GROUP BY 
          b.business_name, l.latitude, l.longitude
        ORDER BY 
          RAND()
        LIMIT {n_competidores}
    """
    
    # Ejecutamos la consulta y obtenemos los datos
    df_comp = run_query(query_competidores)

    # Verificar que haya datos
    if df_comp.empty:
        st.warning(f"⚠️ No se encontraron competidores para la categoría: {categoria}")
    else:
        st.write(f"Se encontraron {len(df_comp)} competidores.")
        st.write(df_comp.head())  # Muestra las primeras filas para verificar

    # --- MOSTRAR DATOS Y GRÁFICOS ---
    st.subheader(f"📋 {n_competidores} Competidores Aleatorios – Categoría: {categoria.title()}")
    st.dataframe(df_comp)

    # --- Dispersión – Reseñas vs Rating Promedio ---
    st.subheader("📈 Dispersión – Reseñas vs Rating Promedio")
    if not df_comp.empty:
        fig1, ax1 = plt.subplots()
        ax1.scatter(df_comp["num_reviews"], df_comp["avg_rating"], alpha=0.7)
        for _, row in df_comp.iterrows():
            ax1.annotate(row["business_name"], (row["num_reviews"], row["avg_rating"]),
                         fontsize=7, xytext=(3,3), textcoords='offset points')
        ax1.set_xlabel("Número de Reseñas")
        ax1.set_ylabel("Rating Promedio")
        ax1.set_title(f"Competencia – Categoría: {categoria.title()}")
        st.pyplot(fig1)
    else:
        st.info("No se encontraron competidores con esa categoría.")

    # --- Distribución de Estrellas ---
    st.subheader(f"📊 Distribución de Estrellas – {categoria.title()}")
    # Si tienes un DataFrame `df_dist` con la distribución de estrellas, puedes visualizarlo
    # Aquí te doy un ejemplo de cómo crear un DataFrame con la distribución de estrellas:
    df_dist = df_comp.groupby("avg_rating").size().reset_index(name='count')
    if not df_dist.empty:
        fig2, ax2 = plt.subplots()
        ax2.pie(df_dist['count'], labels=df_dist['avg_rating'], autopct='%1.1f%%', startangle=90)
        ax2.axis('equal')
        st.pyplot(fig2)
    else:
        st.info("No hay suficientes datos para mostrar la distribución de estrellas.")

    # --- Mapa Interactivo ---
    st.subheader("🗺️ Mapa de Competidores por Ubicación y Calificación")
    
    # Comprobamos si tenemos latitud y longitud
    if "latitude" in df_comp.columns and "longitude" in df_comp.columns and df_comp["latitude"].notnull().all() and df_comp["longitude"].notnull().all():
        # Normalizamos las estrellas para el color
        def rating_to_color(stars):
            if stars >= 4.5:
                return [0, 200, 0]    # verde
            elif stars >= 3.5:
                return [255, 165, 0]  # naranja
            else:
                return [200, 0, 0]    # rojo

        df_comp["color"] = df_comp["avg_rating"].apply(rating_to_color)

        # Creamos el mapa con pydeck
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(
                latitude=df_comp["latitude"].mean(),
                longitude=df_comp["longitude"].mean(),
                zoom=11,
                pitch=40,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=df_comp,
                    get_position='[longitude, latitude]',
                    get_color='color',
                    get_radius=150,
                    pickable=True,
                    tooltip=True
                )
            ],
            tooltip={"text": "{business_name}\n⭐ {avg_rating} estrellas"}
        ))
    else:
        st.warning("⚠️ El DataFrame no contiene columnas válidas de latitud y longitud para mostrar el mapa.")




# --- SIDEBAR ---
with st.sidebar:
    opcion = option_menu("Navegación", 
        ["Inicio", "KPIs", "Recomendador", "Análisis de Sentimiento", "Predicciones", "Distribución de Reseñas", "Competencia", "Explorar Reseñas"],
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
# --- COMPETENCIA ---
if opcion == "Competencia":
   
    show_competencia()
if opcion == "Recomendador":
    st.title("💡 Recomendador para El Camino Real")
    st.markdown("""
    Este módulo analiza las reseñas de la competencia directa de *El Camino Real* para detectar las frases más frecuentes 
    que los clientes valoran. A partir de eso, generamos recomendaciones accionables para mejorar la propuesta del local.
    """)

    st.divider()
    st.subheader("📦 Cargando reseñas de competidores...")

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

    # Seleccionar el tipo de reseña (Positiva, Negativa, Neutra)
    tipo_reseña = st.selectbox("Selecciona el tipo de reseña", ("Positiva", "Negativa", "Neutra"))

    # Obtener el rango de estrellas según la selección del usuario
    if tipo_reseña == "Positiva":
        stars_filter = "r.stars >= 4"
    elif tipo_reseña == "Negativa":
        stars_filter = "r.stars <= 2"
    else:  # Neutra (3 estrellas)
        stars_filter = "r.stars = 3"

    # Cargar reseñas de ese negocio según el tipo seleccionado
    @st.cache_data
    def cargar_datos(business_id, stars_filter):
        query = f"""
        SELECT review_text
        FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review` r
        JOIN `shining-rampart-455602-a7.dw_restaurantes.dim_business` b
          ON r.business_id = b.business_id
        WHERE b.business_id = '{business_id}'
          AND {stars_filter}
          AND r.review_text IS NOT NULL
        """
        return client.query(query).to_dataframe()

    df = cargar_datos(business_id_seleccionado, stars_filter)

    if df.empty:
        st.warning("No se encontraron reseñas para este negocio con el tipo seleccionado.")
    else:
        # --- Procesamiento ---
        df['review_text'] = df['review_text'].fillna('').str.lower().str.replace(r'[^\w\s]', '', regex=True)

        vectorizer = CountVectorizer(ngram_range=(2, 3), stop_words='english')
        X = vectorizer.fit_transform(df['review_text'])
        sum_words = X.sum(axis=0)

        phrases_freq = [(phrase, int(sum_words[0, idx])) for phrase, idx in vectorizer.vocabulary_.items()]
        phrases_freq = sorted(phrases_freq, key=lambda x: x[1], reverse=True)

        # Mostrar resultados
        st.subheader("🔍 Frases más frecuentes en reseñas")
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

    st.caption("Análisis basado en reseñas filtradas de negocios mexicanos con alta calificación.")

  
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


if opcion == "Distribución de Reseñas":
    # tu código aquí

    st.subheader("Distribución de Reseñas por Año y Sentimiento")

    # Cargar negocios con reseñas
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
    opciones_negocios = negocios_df['business_name'].tolist()
    negocio_elegido = st.selectbox("Selecciona un negocio para analizar", opciones_negocios)

    # Obtener el business_id correspondiente
    business_id = negocios_df[negocios_df['business_name'] == negocio_elegido]['business_id'].values[0]

    # Consulta a BigQuery con sentimiento clasificado
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
        pivot_df = pivot_df[["Negativo", "Neutro", "Positivo"]]  # Asegurar orden

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
        st.warning("No se encontraron reseñas para este negocio.")
        
if opcion == "Explorar Reseñas":
    st.subheader("📝 Últimas reseñas de El Camino Real")

    # Business ID fijo
    business_id = "julsvvavzvghwffkkm0nlg"  # <- usá el correcto para El Camino Real

    # Filtro por sentimiento
    sentimiento = st.selectbox("Filtrar por sentimiento", ["Todos", "Positivo", "Neutro", "Negativo"])
    filtro_sentimiento = ""
    if sentimiento == "Positivo":
        filtro_sentimiento = "AND stars >= 4"
    elif sentimiento == "Negativo":
        filtro_sentimiento = "AND stars <= 2"
    elif sentimiento == "Neutro":
        filtro_sentimiento = "AND stars = 3"

    # Filtro por fecha
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", datetime.date(2020, 1, 1))
    with col2:
        fecha_fin = st.date_input("Hasta", datetime.date.today())

    filtro_fecha = f"AND review_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"

    # Consulta SQL
    query = f"""
    SELECT review_text, stars, review_date
    FROM `shining-rampart-455602-a7.dw_restaurantes.fact_review`
    WHERE business_id = '{business_id}'
    {filtro_fecha}
    {filtro_sentimiento}
    ORDER BY review_date DESC
    LIMIT 100
    """

    reviews = run_query(query)

    if not reviews.empty:
        # Mostrar estrellas visuales
        reviews["calificación"] = reviews["stars"].apply(lambda x: "⭐" * int(round(x)))
        st.dataframe(reviews[["review_date", "calificación", "review_text"]])
        
        # Botón para ver nube de palabras
        if st.button("🔍 Ver palabras más frecuentes"):
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt

            texto = " ".join(reviews["review_text"].dropna().tolist())
            wc = WordCloud(width=800, height=400, background_color="white").generate(texto)

            st.subheader("☁️ Nube de palabras más frecuentes")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wc, interpolation='bilinear')
            ax.axis("off")
            st.pyplot(fig)

    else:
        st.warning("No hay reseñas disponibles para el período o filtro seleccionado.")

if opcion == "KPIs":
    st.title("KPIs de El Camino Real")
    
    # Nuevo ID de negocio para El Camino Real
    business_id = "julsvvavzvghwffkkm0nlg"  # Aquí usas el nuevo ID

    # Selección de rango de fechas
    fecha_desde = st.date_input("Desde:", value=pd.to_datetime("2020-01-01"))
    fecha_hasta = st.date_input("Hasta:", value=pd.to_datetime("2023-12-31"))
    
    # Botón para confirmar la selección de fechas
    if st.button('Confirmar Fechas'):
        # Construcción de la query SQL para El Camino Real
        filtro = f"WHERE business_id = '{business_id}'"

        # Ajuste del formato de la fecha dependiendo de la frecuencia seleccionada
        frecuencia = st.radio("Selecciona la frecuencia de análisis:", ('Mensual', 'Anual'))

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
        FROM shining-rampart-455602-a7.dw_restaurantes.fact_review
        {filtro}
        AND review_date BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
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

