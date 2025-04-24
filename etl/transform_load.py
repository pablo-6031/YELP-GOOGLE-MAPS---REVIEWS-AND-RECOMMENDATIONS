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
def clean_dataframe(df):
    if "text" in df.columns:
        df["text"] = df["text"].fillna("").astype("string")
        df["review_length"] = df["text"].apply(lambda x: len(str(x).split()))
    if "time" in df.columns:
        df["review_date"] = pd.to_datetime(df["time"], unit="ms", errors="coerce")
    for col in ["user_id", "gmap_id", "name"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
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
