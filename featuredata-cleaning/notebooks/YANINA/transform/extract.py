## extract.py
import os
import pandas as pd
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

def read_file(filepath):
    ext = os.path.splitext(filepath)[-1].lower()
    
    try:
        if ext == ".csv":
            return pd.read_csv(filepath)
        elif ext == ".json":
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return pd.json_normalize(data)
        elif ext == ".parquet":
            return pd.read_parquet(filepath)
        elif ext == ".pkl":
            return pd.read_pickle(filepath)
        else:
            print(f"⚠️ Tipo de archivo no soportado: {filepath}")
            return None
    except Exception as e:
        print(f"❌ Error leyendo {filepath}: {e}")
        return None

def run():
    print("🔹 Iniciando extracción de datos desde data/raw...")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for file in os.listdir(RAW_DIR):
        raw_path = os.path.join(RAW_DIR, file)
        if os.path.isfile(raw_path):
            df = read_file(raw_path)
            if df is not None:
                filename = os.path.splitext(file)[0] + ".parquet"
                processed_path = os.path.join(PROCESSED_DIR, filename)
                df.to_parquet(processed_path, index=False)
                print(f"✅ {file} → {filename}")
    
    print("✅ Extracción completada.")

if __name__ == "__main__":
    run()
