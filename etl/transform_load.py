## Preprocesamiento, limpieza y carga a GCS

"""
ETL - Transform and Load Script

1. (Opcional) Descargar archivos desde Google Drive o API externa
2. Leer archivos desde disco local
3. Aplicar transformaciones básicas
4. Guardar en formato .parquet en el bucket del data lake
"""

import pandas as pd
import os
from datetime import datetime

# 1. [OPCIONAL] BLOQUE PARA DESCARGAR DESDE GOOGLE DRIVE O API
def optional_download_data():
    """
    Esta función es opcional. Puede implementar la descarga desde Google Drive o una API externa
    usando gdown, pydrive, requests, etc.
    """
    print("✔️ Descarga opcional no implementada aún.")
    pass

# 1.2. DEFINIR LA RUTA DE LOS DATOS YA DESCARGADOS
LOCAL_DATA_FOLDER = "data/"  # Carpeta local donde están los .csv o .parquet
INPUT_FILE = os.path.join(LOCAL_DATA_FOLDER, "reviews_alabama.csv")  # Ajusta según el archivo real

# 2. Detectar archivos válidos
def list_data_files(folder):
    valid_extensions = [".csv", ".json", ".parquet"]
    return [f for f in os.listdir(folder) if os.path.splitext(f)[1] in valid_extensions]

# 3. Transformación básica
def transform_data(df):
    df = df.copy()
    if "text" in df.columns:
        df["text"] = df["text"].fillna("")
        df["review_length"] = df["text"].apply(lambda x: len(str(x).split()))
    if "time" in df.columns:
        df["review_date"] = pd.to_datetime(df["time"], unit="s", errors="coerce")
    return df

# 4. Procesar todos los archivos
def process_all_files(folder, file_list):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for file_name in file_list:
        full_path = os.path.join(folder, file_name)
        print(f"\n🔄 Procesando: {file_name}")
        ext = os.path.splitext(file_name)[1]

        if ext == ".csv":
            df = pd.read_csv(full_path)
        elif ext == ".json":
            df = pd.read_json(full_path, lines=True)
        elif ext == ".parquet":
            df = pd.read_parquet(full_path)
        else:
            print(f"⛔ Tipo de archivo no soportado: {file_name}")
            continue

        df_transformed = transform_data(df)
        today = datetime.now().strftime("%Y-%m-%d")
        output_file = f"{os.path.splitext(file_name)[0]}_clean_{today}.parquet"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)
        df_transformed.to_parquet(output_path, index=False)
        print(f"✅ Guardado en: {output_path}")

# MAIN
if __name__ == "__main__":
    print(f"🔍 Buscando archivos en: {LOCAL_DATA_FOLDER}")
    archivos = list_data_files(LOCAL_DATA_FOLDER)

    if not archivos:
        print("⚠️ No se encontraron archivos válidos (.csv, .json, .parquet).")
        exit()

    print("📄 Archivos encontrados:")
    for i, f in enumerate(archivos, 1):
        print(f"{i}. {f}")

    confirm = input("\n¿Deseas procesar estos archivos? (S/N): ").strip().lower()

    if confirm == "s":
        process_all_files(LOCAL_DATA_FOLDER, archivos)
        print("\n✅ Proceso completado con éxito.")
    else:
        print("\n🚫 Proceso cancelado por el usuario.")
