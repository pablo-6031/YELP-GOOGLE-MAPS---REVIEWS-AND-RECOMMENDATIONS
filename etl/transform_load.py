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

# 2. DEFINIR LA RUTA DE LOS DATOS YA DESCARGADOS
LOCAL_DATA_FOLDER = "data/"  # Carpeta local donde están los .csv o .parquet
INPUT_FILE = os.path.join(LOCAL_DATA_FOLDER, "reviews_alabama.csv")  # Ajusta según el archivo real

# 3. CARGAR LOS DATOS
def load_local_data():
    print(f"📂 Cargando archivo desde: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"✅ {len(df)} registros cargados.")
    return df

# 4. TRANSFORMACIONES BÁSICAS
def transform_data(df):
    print("🔧 Aplicando transformaciones...")

    df = df.copy()
    df["text"] = df["text"].fillna("")
    df["review_length"] = df["text"].apply(lambda x: len(x.split()))
    df["review_date"] = pd.to_datetime(df["time"], unit="s", errors="coerce")

    # Aquí puedes aplicar otras limpiezas según necesidades
    return df

# 5. GUARDAR COMO .PARQUET EN EL DATA LAKE
def save_to_datalake(df):
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = f"data_lake/clean_reviews_{today}.parquet"  # Local, luego se sube a GCS
    df.to_parquet(output_path, index=False)
    print(f"📁 Archivo guardado como: {output_path}")

# MAIN
if __name__ == "__main__":
    print("🚀 Iniciando proceso ETL...")

    # [1] Descarga opcional (no implementada)
    optional_download_data()

    # [2] Cargar datos locales
    df = load_local_data()

    # [3] Transformar
    df_transformed = transform_data(df)

    # [4] Guardar en formato .parquet
    save_to_datalake(df_transformed)

    print("✅ Proceso ETL finalizado.")
