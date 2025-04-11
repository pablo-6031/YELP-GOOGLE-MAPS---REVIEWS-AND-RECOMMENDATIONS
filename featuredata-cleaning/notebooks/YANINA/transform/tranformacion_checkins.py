import pandas as pd
import os

def run():
    # Ruta del archivo original
    input_path = r"C:\Users\yanin\OneDrive\Desktop\proyecto final\archivos\yelp\checkin.json"

    # Ruta del archivo de salida
    output_dir = r"C:\Users\yanin\OneDrive\Desktop\etl\data_lake\clean\yelp"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "checkins.parquet")

    # 📥 Cargar el archivo JSON
    df_checkins = pd.read_json(input_path, lines=True)

    # 📋 Información general
    print("📊 Primeras filas:")
    print(df_checkins.head())
    print("\n📊 Información del DataFrame:")
    print(df_checkins.info())
    print("\n📊 Tipos de datos:")
    print(df_checkins.dtypes)
    print(f"\n📏 Tamaño: {df_checkins.shape}")
    print("\n🧼 Nulos por columna:")
    print(df_checkins.isnull().sum())
    print(f"\n🔁 Filas duplicadas: {df_checkins.duplicated().sum()}")

    # Mostrar duplicados si existen
    df_duplicados = df_checkins[df_checkins.duplicated()]
    if not df_duplicados.empty:
        print("\n🔍 Duplicados encontrados:")
        print(df_duplicados)

    # 📈 Estadísticas generales
    print("\n📈 Estadísticas:")
    print(df_checkins.describe(include="all"))

    # 🛠️ Normalización de fechas separadas por coma
    rows = []
    for entry in df_checkins.to_dict(orient="records"):
        business_id = entry["business_id"]
        if pd.notna(entry["date"]):
            dates = entry["date"].split(", ")
            for date in dates:
                rows.append({"business_id": business_id, "checkin_date": date})

    # 🧾 Crear DataFrame limpio
    df_checkin = pd.DataFrame(rows)

    # 💾 Guardar como Parquet
    df_checkin.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"\n✅ Archivo guardado correctamente en: {output_file}")
    print(f"📏 Dimensiones finales: {df_checkin.shape}")
