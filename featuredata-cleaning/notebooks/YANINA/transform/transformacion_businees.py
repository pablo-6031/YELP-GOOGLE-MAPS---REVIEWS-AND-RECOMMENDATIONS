
import pandas as pd
import numpy as np
import os

def run():
    # 📁 Rutas
    input_path = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\raw\yelp\business.pkl"
    output_dir = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\clean\yelp"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "business.parquet")

    # 📦 Cargar archivo
    df_business = pd.read_pickle(input_path)

    # 🧹 Limpiar columnas duplicadas
    df_business = df_business.loc[:, ~df_business.columns.duplicated()]

    # 🔢 Convertir columnas numéricas
    columnas_numericas = ['stars', 'review_count', 'latitude', 'longitude', 'is_open']
    for columna in columnas_numericas:
        df_business[columna] = pd.to_numeric(df_business[columna], errors='coerce')

    # 🔤 Convertir columnas a string
    columnas_string = ['business_id', 'name', 'address', 'city', 'state', 'postal_code']
    df_business[columnas_string] = df_business[columnas_string].astype('string')

    # 💾 Guardar en Parquet limpio
    df_business.to_parquet(output_path, engine="pyarrow", compression="snappy")
    print(f"✅ Archivo guardado en: {output_path}")

    # 📊 Reporte rápido
    print("\n📏 Dimensiones:", df_business.shape)
    print("\n🧼 Nulos por columna (%):")
    print((df_business.isnull().sum() / len(df_business) * 100).round(2))
