
import json
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar .env si existe
load_dotenv()

def run():
    input_path = os.getenv("REVIEWS_WY_INPUT_PATH", "data/raw/google/review-Wyoming.json")
    output_dir = os.getenv("REVIEWS_WY_OUTPUT_PATH", "data/processed")
    os.makedirs(output_dir, exist_ok=True)

    reviews = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                review = json.loads(line.strip())
                reviews.append({
                    "user_id": review.get("user_id"),
                    "name": review.get("name"),
                    "time": review.get("time"),
                    "rating": review.get("rating"),
                    "text": review.get("text"),
                    "gmap_id": review.get("gmap_id")
                })
            except json.JSONDecodeError as e:
                print(f"⚠️ Error leyendo línea: {e}")

    df = pd.DataFrame(reviews)

    # Limpieza y normalización
    df["time"] = pd.to_datetime(df["time"], unit='ms', errors='coerce')
    df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce')
    df["name"] = df["name"].astype("string")
    df["text"] = df["text"].astype("string")
    df["gmap_id"] = df["gmap_id"].astype("string")
    df = df.drop_duplicates()

    # Guardado
    output_file = os.path.join(output_dir, "review-Wyoming.parquet")
    df.to_parquet(output_file, index=False)

    print(f"\n🎉 Archivo transformado guardado: {output_file}")
    print("\n📊 Dimensiones:", df.shape)
    print("\n❗ Nulos por columna (%):")
    print((df.isnull().sum() / len(df) * 100).round(2))
