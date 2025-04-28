import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Cargar las credenciales desde el archivo 'secrets.toml' de Streamlit
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Crear el cliente de BigQuery usando las credenciales
client = bigquery.Client(credentials=credentials, project=st.secrets["gcp_service_account"]["project_id"])

# Realizar una consulta simple a BigQuery para verificar la conexión
query = "SELECT CURRENT_TIMESTAMP() as current_time;"
query_job = client.query(query)

# Esperar a que termine la consulta
result = query_job.result()

# Mostrar el resultado en Streamlit
st.write(f"Consulta ejecutada correctamente, hora actual: {result[0].current_time}")
