import json 
import pandas as pd
import numpy as np
import re
import os

# Configuración de rutas desde variable de entorno (con fallback)
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw/google/metadata-sitios")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/processed/restaurantes_CA_WY.parquet")

carpeta = os.path.abspath(RAW_DATA_PATH)
output_path = os.path.abspath(OUTPUT_PATH)

def extraer_estado(address):
    if isinstance(address, str):
        match = re.search(r',\s*([A-Z]{2})\s*\d{5}', address)
        if match:
            return match.group(1)
    return np.nan

def extraer_ciudad(address):
    if isinstance(address, str):
        partes = address.split(",")
        if len(partes) >= 3:
            return partes[-2].strip()
    return np.nan

def run():
    lista_df = []

    for archivo in os.listdir(carpeta):
        if archivo.endswith(".json"):
            file_path = os.path.join(carpeta, archivo)
            print(f"📂 Procesando: {archivo}")

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                fixed_content = "[\n" + ",\n".join(line.strip() for line in lines if line.strip()) + "\n]"
                file_fixed_path = file_path.replace(".json", "_fixed.json")

                with open(file_fixed_path, "w", encoding="utf-8") as f:
                    f.write(fixed_content)

                with open(file_fixed_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                print(f"✅ Archivo corregido guardado en: {file_fixed_path}")

            df = pd.DataFrame(data)

            df = df[df['category'].apply(lambda x: any('restaurant' in cat.lower() for cat in x) if isinstance(x, list) else False)]

            umbral_nulos = 0.40
            columnas_nulas = df.columns[df.isnull().mean() > umbral_nulos].tolist()
            df = df.drop(columns=columnas_nulas)

            df_temp = df.applymap(lambda x: str(x) if isinstance(x, (list, dict)) else x)
            df = df[~df_temp.duplicated()]

            df['state_code'] = df['address'].apply(extraer_estado)
            df['city_name'] = df['address'].apply(extraer_ciudad)

            df['name'] = df['name'].astype('string')
            df['address'] = df['address'].astype('string')
            df['gmap_id'] = df['gmap_id'].astype('string')
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            df['category'] = df['category'].astype('string')
            df['avg_rating'] = pd.to_numeric(df['avg_rating'], errors='coerce')
            df['num_of_reviews'] = pd.to_numeric(df['num_of_reviews'], errors='coerce').astype('Int64')
            df['state_code'] = df['state_code'].astype('string')
            df['city_name'] = df['city_name'].astype('string')

            columnas_a_eliminar = ['MISC', 'url', 'state', 'relative_results', 'price']
            df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])

            lista_df.append(df)

    df_total = pd.concat(lista_df, ignore_index=True)
    df_filtrado = df_total[df_total['state_code'].isin(['CA', 'WY'])]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_filtrado.to_parquet(output_path, index=False)

    print(f"✅ Total de restaurantes en CA y WY: {df_filtrado.shape[0]}")
    print(f"💾 Archivo guardado en: {output_path}")
