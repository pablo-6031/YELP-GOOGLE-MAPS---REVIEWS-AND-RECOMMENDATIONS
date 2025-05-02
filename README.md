
# 📊 HYPE Analytics – Proyecto Final de Análisis de Reseñas Digitales

---

## 🧠 Introducción

**HYPE Analytics** es una iniciativa de análisis inteligente de reseñas, cuyo objetivo es transformar la 
percepción digital que los negocios gastronómicos tienen en plataformas como Yelp y Google Maps, 
convirtiéndola en una herramienta **estratégica para la toma de decisiones**.

En un contexto de creciente influencia de la reputación digital, este proyecto utiliza técnicas de NLP, 
modelos de Machine Learning y visualizaciones de alto impacto para diagnosticar el posicionamiento actual 
de los restaurantes, anticipar su evolución y sugerir acciones concretas de mejora.

---

## 🎯 Objetivo del Proyecto

Desarrollar herramientas analíticas e inteligentes que permitan:
- Evaluar la reputación digital de negocios a partir de reseñas escritas.
- Identificar los factores que influyen en la calificación y experiencia del cliente.
- Recomendar mejoras estratégicas para aumentar la satisfacción.
- Presentar esta información de forma accesible, visual y útil para la toma de decisiones.

---

## 🚀 Productos Finales

### 1. **Dashboard Interactivo (Looker Studio)**

- Visualiza métricas clave (KPI) como rating promedio, frecuencia de menciones, evolución temporal, etc.
- Permite filtrar por ubicación, tipo de negocio y comparar tendencias.
- Facilita una lectura rápida de la situación digital del negocio.

🔗 [Ver Dashboard en Looker Studio](https://lookerstudio.google.com/reporting/
df20fc98-f8fa-42bf-8734-92d4ff90e6f5)

### 2. **Aplicación Streamlit – Simulador de Reseñas**

- El usuario puede ingresar una reseña escrita.
- El sistema predice cuántas estrellas recibiría (modelo de ML).
- Brinda retroalimentación sobre cómo una reseña impactaría la reputación.
- Integra análisis de palabras clave y posibilidad de extensión a emociones.

🔗 [Probar App en Streamlit](https://yelp-app-maps---reviews-and-recommendations-jv7mxypeg2lwxovdj7.
streamlit.app/)

---

## 🧠 Machine Learning Implementado

- **Modelo principal:** Predicción de calificación (rating) basada en texto de reseñas.
- **Tipo:** Regresión supervisada.
- **Algoritmo elegido:** XGBoostRegressor.
- **Técnicas NLP usadas:** limpieza de texto, lematización, vectorización TF-IDF.
- **Entrenamiento:** Con dataset etiquetado real de Yelp / Google Maps.

Este modelo permite simular el impacto de un comentario en la reputación digital antes de que ocurra, 
ayudando a prevenir descensos de calificación.

---

## 📈 KPIs Analizados

| KPI | Descripción |
|-----|-------------|
| ⭐ Calificación Promedio | Seguimiento general de reputación |
| 📝 Volumen de Reseñas | Indicador de visibilidad y flujo |
| 💬 Palabras Clave | Factores más mencionados: servicio, comida, ambiente |
| 📉 Variación Mensual | Fluctuaciones en calificación y frecuencia |
| 🔮 Predicción de Rating | Estimación ML para nuevas reseñas |

---

## 🏗️ Stack Tecnológico

| Proceso            | Herramientas |
|--------------------|--------------|
| Extracción         | APIs Yelp / Google Maps |
| Transformación     | Python, Pandas, NLTK, SpaCy |
| Almacenamiento     | Google Cloud Storage + BigQuery |
| Machine Learning   | Scikit-learn, XGBoost |
| Visualización      | Looker Studio, Streamlit |
| Automatización     | Airflow (orquestación de procesos) |

---

## 👥 Equipo de Desarrollo

- Yanina Spina – Data Scientist
- Harry Guevara – Functional Analyst & ML Engineer
- Elvis Bernuy – Data Analyst
- Pablo Mizzau – Data Engineer
- Pablo Carrizo – Data Engineer

---

## 📄 Licencia

MIT License – Uso académico y educativo.

---

© Proyecto HYPE Analytics – 2025



