from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from datetime import datetime

PROJECT_ID = "shining-rampart-455602-a7"
DATASET_ID = "dw_restaurantes"

client = bigquery.Client(project=PROJECT_ID)

# --------------------------------------------
# FUNCIONES AUXILIARES
# --------------------------------------------

def log(msg):
    print(f"[{datetime.now().isoformat()}] {msg}")

def validate_table_types(table_id, expected_schema):
    """Valida que los tipos de columnas coincidan con lo esperado"""
    table = client.get_table(table_id)
    for field, expected_type in expected_schema.items():
        actual_field = next((f for f in table.schema if f.name == field), None)
        if not actual_field:
            raise Exception(f"Campo '{field}' no existe en {table_id}")
        if actual_field.field_type != expected_type:
            raise Exception(f"Tipo incorrecto en {field}: {actual_field.field_type} (esperado: {expected_type})")
    log(f"✓ Tipos validados para {table_id}")

def truncate_table(table_id):
    """Elimina todos los datos de una tabla"""
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    log(f"✓ Tabla limpiada: {table_id}")

def insert_new_rows(staging_table, target_table, key_fields):
    """
    Inserta datos nuevos de staging en target comparando por key_fields
    """
    join_conditions = " AND ".join([f"s.{k} = t.{k}" for k in key_fields])

    # Obtener todas las columnas de la tabla
    table = client.get_table(staging_table)
    all_fields = [field.name for field in table.schema]
    
    # Prefijar con 's.' para evitar ambigüedades en el SELECT
    all_fields_str = ", ".join([f"s.{field}" for field in all_fields])

    query = f"""
        INSERT INTO `{target_table}` ({", ".join(all_fields)})
        SELECT {all_fields_str}
        FROM `{staging_table}` s
        LEFT JOIN `{target_table}` t
        ON {join_conditions}
        WHERE {" AND ".join([f"t.{k} IS NULL" for k in key_fields])}
    """
    client.query(query).result()
    log(f"✓ Datos nuevos insertados en {target_table} desde {staging_table}")

def check_duplicates(table_id, key_fields):
    """Verifica duplicados en staging por claves"""
    key_str = ", ".join(key_fields)
    query = f"""
        SELECT {key_str}, COUNT(*) as count
        FROM `{table_id}`
        GROUP BY {key_str}
        HAVING COUNT(*) > 1
        LIMIT 1
    """
    result = client.query(query).result()
    rows = list(result)
    if rows:
        raise Exception(f"⚠️ Duplicados detectados en {table_id} por claves: {key_fields}")
    log(f"✓ No hay duplicados en {table_id}")

# --------------------------------------------
# DEFINICIONES DE TABLAS
# --------------------------------------------

TABLES = [
    {
        "name": "dim_business",
        "key_fields": ["business_id"],
        "schema": {
            "business_id": "STRING",
            "business_name": "STRING",
            "categories": "STRING",
            "business_stars": "FLOAT",
            "review_count": "INTEGER"
        }
    },
    {
        "name": "dim_cities",
        "key_fields": ["city_id"],
        "schema": {
            "city_id": "INTEGER",
            "city_name": "STRING",
            "state_id": "INTEGER"
        }
    },
    {
        "name": "dim_locations",
        "key_fields": ["business_id", "address"],
        "schema": {
            "business_id": "STRING",
            "address": "STRING",
            "latitude": "FLOAT",
            "longitude": "FLOAT",
            "city_id": "INTEGER"
        }
    },
    {
        "name": "dim_states",
        "key_fields": ["state_id"],
        "schema": {
            "state_id": "INTEGER",
            "state_code": "STRING",
            "state_name": "STRING"
        }
    },
    {
        "name": "dim_users",
        "key_fields": ["user_id"],
        "schema": {
            "user_id": "STRING",
            "user_name": "STRING"
        }
    },
    {
        "name": "fact_review",
        "key_fields": ["user_id", "business_id", "review_date"],
        "schema": {
            "user_id": "STRING",
            "business_id": "STRING",
            "stars": "FLOAT",
            "review_text": "STRING",
            "review_date": "DATETIME"
        }
    },
]

# --------------------------------------------
# EJECUCIÓN PRINCIPAL
# --------------------------------------------

def process_table(table_info):
    name = table_info["name"]
    staging_table = f"{PROJECT_ID}.{DATASET_ID}.staging_{name}"
    target_table = f"{PROJECT_ID}.{DATASET_ID}.{name}"
    log(f"🔄 Procesando tabla: {name}")

    validate_table_types(staging_table, table_info["schema"])
    check_duplicates(staging_table, table_info["key_fields"])
    insert_new_rows(staging_table, target_table, table_info["key_fields"])
    truncate_table(staging_table)

def main():
    log("🚀 Iniciando proceso de carga incremental...")
    for table in TABLES:
        try:
            process_table(table)
        except Exception as e:
            log(f"❌ Error en tabla {table['name']}: {e}")
    log("✅ Proceso finalizado.")

if __name__ == "__main__":
    main()
