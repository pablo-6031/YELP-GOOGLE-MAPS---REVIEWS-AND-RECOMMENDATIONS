import os
import pandas as pd

def run():
    # 📥 Ruta original del archivo
    df_users_path = r"C:\Users\yanin\OneDrive\Desktop\proyecto final\archivos\yelp\user.parquet"

    # 📤 Ruta de salida
    output_path = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\clean\yelp\user_minimal.parquet"

    # Verificar si el archivo existe
    if os.path.exists(df_users_path):
        print("✅ Archivo encontrado: user.parquet")
        
        # Cargar el archivo
        df_users = pd.read_parquet(df_users_path)
        
        # Filtrar columnas necesarias
        df_users = df_users[["user_id", "name"]]

        # Normalizar el nombre
        df_users["name"] = df_users["name"].str.lower().str.strip()

        # Eliminar nulos y duplicados
        df_users.dropna(inplace=True)
        df_users.drop_duplicates(inplace=True)

        # Renombrar columna
        df_users.rename(columns={"name": "name_id"}, inplace=True)

        # Convertir tipos
        df_users = df_users.astype({"user_id": "string", "name_id": "string"})

        # Guardar archivo limpio
        df_users.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

        print(f"✅ user_minimal.parquet guardado en: {output_path}")
        print(df_users.head())

    else:
        print("❌ El archivo user.parquet no fue encontrado.")
