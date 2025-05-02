
# 🧾 Documentación Técnica del Proyecto – HYPE Analytics

Este documento describe la arquitectura general del proyecto, junto con las tecnologías empleadas, su 
justificación de uso y la forma en que contribuyen al desarrollo de los productos finales: **dashboard 
interactivo (Looker Studio)** y **aplicación Streamlit con modelo de ML**.

---

## 🏗️ Arquitectura General

```
                 +----------------------------+
                 |  APIs Yelp / Google Maps   |
                 +----------------------------+
                             |
                             v
                   Extracción de Datos (Python)
                             |
                             v
       +----------------> Limpieza y ETL (Pandas, NLP)
       |                     |
       |                     v
       |           Google Cloud Storage (Parquet)
       |                     |
       |                     v
+----------------+      BigQuery (Data Warehouse)
| Streamlit App  |            |
| (ML Predictivo) |           v
+----------------+   Looker Studio (Dashboard)
```

---

## ⚙️ Tecnologías y Justificación

### 🔹 **Python**
- Lenguaje principal del proyecto.
- Versátil, soporta todo el stack: ETL, ML, APIs y automatización.

### 🔹 **APIs de Yelp y Google Maps**
- Fuentes oficiales y actualizadas de reseñas, ubicación y ratings de negocios.
- Permiten construir un dataset confiable y enriquecido.

### 🔹 **Pandas / NumPy**
- Procesamiento eficiente de datos tabulares.
- Transformación y normalización previa al almacenamiento o modelado.

### 🔹 **NLTK / spaCy / TF-IDF**
- Limpieza, tokenización, lematización del texto.
- TF-IDF usado para vectorizar reseñas y alimentar el modelo ML.
- NLP permite extraer información semántica clave, base del análisis predictivo y dashboard.

### 🔹 **XGBoost**
- Algoritmo robusto de regresión.
- Alta precisión y velocidad para problemas con texto vectorizado.
- Justificación: supera a modelos lineales y naïve bayes en validación cruzada.

### 🔹 **Scikit-learn**
- Estandarización de pipelines, evaluación, entrenamiento y serialización de modelos.
- Integra fácilmente con TF-IDF y XGBoost.

### 🔹 **Google Cloud Storage (GCS)**
- Almacenamiento externo escalable para datos `.parquet` procesados.
- Evita sobrecarga local, ideal para orquestación futura.

### 🔹 **BigQuery**
- Data Warehouse con gran capacidad de consulta SQL.
- Permite estructurar datos y alimentar directamente visualizaciones.
- Justificación: rápida integración con Looker Studio.

### 🔹 **Google Cloud Workflows**
- Servicio nativo de GCP para automatizar flujos de datos y ML.
- Permite encadenar funciones como ingestión, transformación y carga al DW.
- Justificación: alta integración con otros servicios GCP, bajo costo y mantenimiento.

### 🔹 **Streamlit**
- Producto interactivo MVP.
- Permite a usuarios probar cómo una reseña afectaría su calificación.
- Ligero, rápido de desarrollar y fácil de desplegar.

### 🔹 **Looker Studio**
- Dashboard empresarial gratuito, conectado a BigQuery.
- Visualización de KPI’s, evolución de calificaciones y palabras clave.
- Justificación: compatible con GCP, simple de compartir y escalar.

---

## 🤖 Modelos de ML Implementados

### ✅ Modelo de Predicción de Rating

- Entrada: texto de la reseña
- Salida: calificación estimada (1.0 – 5.0)
- Algoritmo: XGBoostRegressor
- Técnicas: NLP + TF-IDF + regresión
- Integración: aplicación Streamlit

**Ventaja para el producto:** permite anticipar el impacto de una reseña antes de publicarse, útil para 
auditoría interna, entrenamiento de personal o estrategia de reputación.

---

## 📊 KPIs y Justificación en el Producto

| KPI | Justificación de uso en el Dashboard |
|-----|--------------------------------------|
| ⭐ Calificación Promedio | Diagnóstico rápido de reputación por zona y periodo |
| 📝 Volumen de Reseñas | Mide visibilidad e interés del público |
| 💬 Palabras Clave | Identifica los aspectos que más impactan en el rating |
| 📉 Variación Temporal | Detecta mejoras o deterioro reciente |
| 🔮 Predicción de Rating | Producto ML que simula el efecto de nuevos comentarios |

---

## 📄 Licencia

MIT License – Uso académico y educativo.

---

© Proyecto HYPE Analytics – 2025
