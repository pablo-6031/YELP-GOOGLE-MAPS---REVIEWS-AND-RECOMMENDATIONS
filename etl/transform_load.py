# 📁 etl/transform_load.py

import os
import pandas as pd
import json
from datetime import datetime

# 📁 Configuración
INPUT_FOLDER = "data/google_reviews_raw"
OUTPUT_FOLDER = "data_lake/reviews"

# 🧪 Detectar archivos válidos
def list_data_files(folder):
    valid_exts = [".json", ".csv", ".parquet"]
    return [f for f in os.listdir(folder) if os.path.splitext(f)[1].lower() in valid_exts]

# 🧼 Limpieza y transformación general
import re

def clean_dataframe(df):
    if "text" in df.columns:
        df["text"] = df["text"].fillna("").astype("string")
        df["review_length"] = df["text"].apply(lambda x: len(str(x).split()))

    if "time" in df.columns:
        df["review_date"] = pd.to_datetime(df["time"], unit="ms", errors="coerce")

    for col in ["user_id", "gmap_id", "name", "state", "city"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Añadir geolocalización si está
    for geo_col in ["latitude", "longitude"]:
        if geo_col in df.columns:
            df[geo_col] = pd.to_numeric(df[geo_col], errors="coerce")

    # Añadir menciones y aspectos
    aspectos = {
        "comida": ["comida", "sabor", "plato", "menu", "cocina"],
        "servicio": ["servicio", "mesero", "atención", "amable"],
        "precio": ["precio", "costo", "caro", "barato"],
        "ambiente": ["ambiente", "lugar", "decoración", "ruido"]
    }

    # Listado simple de palabras positivas/negativas
    positivas = ["bueno", "excelente", "genial", "rico", "agradable"]
    negativas = ["malo", "horrible", "pésimo", "caro", "sucio"]

    for aspecto, palabras in aspectos.items():
        # mentions_aspecto: 1 si alguna palabra aparece
        df[f"mentions_{aspecto}"] = df["text"].apply(
            lambda x: int(any(pal in x.lower() for pal in palabras))
        )

        # aspect_sentiment_aspecto: detecta sentimiento
        def detectar_sentimiento(texto):
            texto = texto.lower()
            if any(w in texto for w in positivas) and any(w in texto for w in palabras):
                return "positivo"
            elif any(w in texto for w in negativas) and any(w in texto for w in palabras):
                return "negativo"
            else:
                return "neutral"

        df[f"aspect_sentiment_{aspecto}"] = df["text"].apply(detectar_sentimiento)

    df.drop_duplicates(inplace=True)
    return df

# 🧱 Lectura segura por tipo
def read_file(path):
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".json":
            with open(path, "r", encoding="utf-8") as f:
                first_line = f.readline()
                if first_line.strip().startswith("{"):
                    return pd.read_json(path, lines=True)
                else:
                    f.seek(0)
                    return pd.json_normalize(json.load(f))
        elif ext == ".csv":
            return pd.read_csv(path)
        elif ext == ".parquet":
            return pd.read_parquet(path)
    except Exception as e:
        print(f"❌ Error leyendo {path}: {e}")
    return None

# 🚀 Procesar todo
def process_all():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    files = list_data_files(INPUT_FOLDER)

    if not files:
        print("⚠️ No se encontraron archivos válidos.")
        return

    print("📄 Archivos encontrados:")
    for f in files:
        print(f" - {f}")

    confirm = input("\n¿Deseas procesar estos archivos? (S/N): ").strip().lower()
    if confirm != "s":
        print("🚫 Proceso cancelado.")
        return

    for file in files:
        path = os.path.join(INPUT_FOLDER, file)
        df = read_file(path)
        if df is None or df.empty:
            print(f"⚠️ Archivo vacío o ilegible: {file}")
            continue

        print(f"\n🔄 Transformando: {file}")
        df_clean = clean_dataframe(df)
        today = datetime.now().strftime("%Y%m%d")
        out_name = f"{os.path.splitext(file)[0]}_clean_{today}.parquet"
        df_clean.to_parquet(os.path.join(OUTPUT_FOLDER, out_name), index=False)
        print(f"✅ Guardado en: {os.path.join(OUTPUT_FOLDER, out_name)}")

# 🏁 MAIN
if __name__ == "__main__":
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    process_all()
