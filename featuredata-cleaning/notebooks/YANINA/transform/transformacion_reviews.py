import pandas as pd
import os

def run():
    # Definir el tamaño de los chunks
    chunk_size = 10000

    # Ruta del archivo de entrada
    input_path = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\raw\yelp\review.json"

    # Ruta del archivo de salida
    output_dir = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\clean\yelp"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "reviews_processed.parquet")

    # Inicializar una lista para almacenar los chunks procesados
    df_list = []

    # Procesar en chunks
    for chunk in pd.read_json(input_path, lines=True, chunksize=chunk_size):
        chunk = chunk.astype({
            'review_id': 'string',
            'user_id': 'string',
            'business_id': 'string',
            'stars': 'int',
            'date': 'datetime64[ns]',
            'useful': 'int',
            'funny': 'int',
            'cool': 'int'
        })

        # Agregar columnas temporales
        chunk['year'] = chunk['date'].dt.year
        chunk['month'] = chunk['date'].dt.month
        chunk['day_of_week'] = chunk['date'].dt.dayofweek
        chunk['day'] = chunk['date'].dt.day
        chunk['hour'] = chunk['date'].dt.hour

        # Filtrar reseñas válidas
        chunk = chunk[chunk['stars'].between(1, 5)]

        # Normalizar los IDs
        chunk['user_id'] = chunk['user_id'].str.strip().str.lower()
        chunk['business_id'] = chunk['business_id'].str.strip().str.lower()

        df_list.append(chunk)

    # Concatenar todos los DataFrames
    df_reviews = pd.concat(df_list, ignore_index=True).drop_duplicates()

    # Guardar como Parquet
    df_reviews.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"✅ Archivo guardado en: {output_file}")
    print(f"📏 Dimensiones: {df_reviews.shape}")
    print("🧼 Nulos por columna (%):")
    print((df_reviews.isnull().sum() / len(df_reviews) * 100).round(2))
