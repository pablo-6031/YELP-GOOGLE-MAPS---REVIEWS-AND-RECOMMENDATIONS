import json
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()

def run():
    input_dir = os.getenv("REVIEWS_CA_INPUT_PATH", "data/raw/google/review-California")
    output_dir = os.getenv("REVIEWS_CA_OUTPUT_PATH", "data/processed")
    os.makedirs(output_dir, exist_ok=True)

    dataframes = []

    for i in range(1, 19):  # del 1 al 18 inclusive
        file_path = os.path.join(input_dir, f"{i}.json")
        
        reviews = []
        with open(file_path, 'r', encoding='utf-8') as f:
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
                    print(f"⚠️ Error leyendo línea en archivo {i}.json: {e}")

        df = pd.DataFrame(reviews)

        # Procesamiento
        df["time"] = pd.to_datetime(df["time"], unit='ms', errors='coerce')
        df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce')
        df["name"] = df["name"].astype("string")
        df["text"] = df["text"].astype("string")
        df["gmap_id"] = df["gmap_id"].astype("string")
        df = df.drop_duplicates()

        # Guardar archivo individual
        output_path = os.path.join(output_dir, f"review-California-{i}.parquet")
        df.to_parquet(output_path, index=False)
        print(f"✅ Guardado individual: {output_path}")

        dataframes.append(df)

    # 🔗 Unión final
    df_total = pd.concat(dataframes, ignore_index=True).drop_duplicates()

    # Guardado final
    final_output_path = os.path.join(output_dir, "review-California.parquet")
    df_total.to_parquet(final_output_path, index=False)
    print(f"\n🎉 Archivo final guardado: {final_output_path}")
    print("\n📊 Dimensiones del archivo final:", df_total.shape)
    print("\n❗ Nulos por columna (%):")
    print((df_total.isnull().sum() / len(df_total) * 100).round(2))
