# -*- coding: utf-8 -*-
"""pablo6031_(10_abr_2025,_10_47_37 a_m_).ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/embedded/projects/proyectoprueba-456419/locations/us-central1/repositories/1d20774c-e9a6-438e-be73-e54c8d516013

# Extract
"""

import pandas as pd
import ast

# Archivo de yelp
path1 = "gs://datalake-restaurantes-2025/processed/google_yelp transformado/yelp_filtrado1.parquet"
df_yelp = pd.read_parquet(path1, engine="pyarrow")
# Archivos de google
path2 = "gs://datalake-restaurantes-2025/processed/google_yelp transformado/restaurantes_CA_WY.parquet"
df_restaurantes_google = pd.read_parquet(path2, engine="pyarrow")
path3 = "gs://datalake-restaurantes-2025/processed/google_yelp transformado/review-California.parquet"
df_RevCal_google = pd.read_parquet(path3, engine="pyarrow")
path4 = "gs://datalake-restaurantes-2025/processed/google_yelp transformado/review-Wyoming.parquet"
df_RevWy_google = pd.read_parquet(path4, engine="pyarrow")
path5 = "gs://datalake-restaurantes-2025/processed/google_yelp transformado/states.parquet"
df_states = pd.read_parquet(path5, engine="pyarrow")

"""# Transform

Seleccionar columnas yelp
"""

df_yelp.columns

df_yelp.drop(columns=['postal_code', 'review_count_x',
       'review_id', 'useful_x', 'funny_x', 'cool_x', 'year', 'month', 'day_of_week', 'day',
       'checkin_count', 'yelping_since',
       'useful_y', 'funny_y', 'cool_y', 'fans','hour'], inplace=True)

df_yelp.columns

df_yelp.rename(columns={'name_x': 'business_name','stars_x': 'business_stars','stars_y': 'stars','review_count_y': 'review_count', 'text_x': 'review_text','date': 'review_date', 'name_y': 'user_name'}, inplace=True)

"""Seleccionar columnas google"""

df_restaurantes_google.columns

df_restaurantes_google.drop(columns=['hours'], inplace=True)

df_restaurantes_google.rename(columns={'name': 'business_name','gmap_id': 'business_id','category': 'categories','num_of_reviews': 'review_count', 'avg_rating': 'business_stars','state_code': 'state'}, inplace=True)

df_Review_google = pd.concat([df_RevCal_google, df_RevWy_google], ignore_index=True)

df_Review_google.columns

df_Review_google.rename(columns={'name': 'user_name','time': 'review_date','rating': 'stars','text': 'review_text', 'gmap_id': 'business_id'}, inplace=True)

"""Separar en tablas

Usuario
"""

df_user_yelp = df_yelp[['user_id', 'user_name']]

df_user_google = df_Review_google[['user_id', 'user_name']]

df_user = pd.concat([df_user_yelp, df_user_google], ignore_index=True)

"""Review"""

df_review_yelp = df_yelp[['user_id','business_id', 'stars','review_text','review_date']]

df_review_google = df_Review_google[['user_id','business_id', 'stars','review_text','review_date']]

df_review = pd.concat([df_review_yelp, df_review_google], ignore_index=True)

"""Business"""

df_business_yelp = df_yelp[['business_id','business_name', 'categories','business_stars','review_count']]

df_business_google = df_restaurantes_google[['business_id','business_name','categories','business_stars','review_count']]

df_business = pd.concat([df_business_yelp, df_business_google], ignore_index=True)

"""Location"""

df_location_yelp = df_yelp[['address','city','state','latitude', 'longitude','business_id']]

df_location_google = df_restaurantes_google[['address','state', 'latitude', 'longitude','business_id']]

df_location_google.loc[:'city'] = df_location_google['address'].str.extract(r'^[^,]+,\s*[^,]+,\s*([^,]+)')

df_location = pd.concat([df_location_yelp, df_location_google], ignore_index=True)

"""City"""

df_city = df_location[['city','state']]

df_city.duplicated().sum()

df_city = df_city.drop_duplicates()

df_city.columns

df_city.loc[:, 'city_id'] = range(1, len(df_city) + 1)

# Hacemos el merge (la columna city_id se suma a df_location)
df_location = df_location.merge(df_city[['city', 'city_id']], on='city', how='left')

# Eliminamos las columnas city y state
df_location = df_location.drop(columns=['city', 'state'])

"""State"""

df_states.rename(columns={'state_id': 'state_code'}, inplace=True)

df_states.loc[:, 'state_id'] = range(1, len(df_states) + 1)

# Nos aseguramos de que ambas columnas estén en formato string
df_city['state'] = df_city['state'].astype(str)
df_states['state_code'] = df_states['state_code'].astype(str)

# Hacemos el merge para traer el state_id desde df_states
df_city = df_city.merge(df_states[['state_code', 'state_id']], left_on='state', right_on='state_code', how='left')

# Eliminamos las columnas city y state
df_city = df_city.drop(columns=['state_code', 'state'])
df_city.rename(columns={'city': 'city_name'}, inplace=True)

"""Eliminar Duplicados"""

df_user['user_id'] = df_user['user_id'].fillna("").astype(str)
df_user['user_name'] = df_user['user_name'].fillna("").astype(str)

df_review['user_id'] = df_review['user_id'].fillna("").astype(str)
df_review['business_id '] = df_review['business_id'].fillna("").astype(str)
df_review['stars'] = df_review['stars'].fillna(0).astype(float)
df_review['review_text '] = df_review['review_text'].fillna("").astype(str)
df_review['review_date'] = pd.to_datetime(df_review['review_date'], errors='coerce')

df_business['business_id'] = df_business['business_id'].fillna("").astype(str)
df_business['business_name'] = df_business['business_name'].fillna("").astype(str)
df_business['categories'] = df_business['categories'].astype(str).str.replace(r"[\[\]']", '', regex=True).str.strip()
df_business['business_stars'] = df_business['business_stars'].fillna(0).astype(float)
df_business['review_count'] = df_business['review_count'].fillna(0).astype(int)

df_location['business_id'] = df_location['business_id'].fillna("").astype(str)
df_location['address '] = df_location['address'].fillna("").astype(str)
df_location['latitude'] = df_location['latitude'].fillna(0).astype(float)
df_location['longitude'] = df_location['longitude'].fillna(0).astype(float)
df_location['city_id'] = df_location['city_id'].fillna(0).astype(int)

df_city['city_name'] = df_city['city_name'].fillna("").astype(str)
df_city['city_id'] = df_city['city_id'].fillna(0).astype(int)
df_city['state_id'] = df_city['state_id'].fillna(0).astype(int)

df_states['state_code'] = df_states['state_code'].fillna("").astype(str)
df_states['state_name'] = df_states['state_name'].fillna("").astype(str)
df_states['state_id'] = df_states['state_id'].fillna(0).astype(int)

df_review = df_review.drop_duplicates()
df_location = df_location.drop_duplicates()

df_user = df_user.drop_duplicates(subset='user_id', keep='first')
df_business = df_business.drop_duplicates(subset='business_id', keep='first')

"""# Load"""

from google.cloud import bigquery

client = bigquery.Client()

# ID del proyecto: analisis-yelp
# Nombre del dataset: yelp_dataset
dataset_id = "shining-rampart-455602-a7.dw_restaurantes"

dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"  # también puede ser "us-central1", "europe-west1", etc.

dataset = client.create_dataset(dataset, exists_ok=True)
print(f"✅ Dataset creado: {dataset.dataset_id}")

from pandas_gbq import to_gbq

"""Crear y cargar tabla usuario"""

to_gbq(
    dataframe=df_user,
    destination_table='dw_restaurantes.dim_users',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)

"""Crear y cargar tabla states"""

to_gbq(
    dataframe=df_states,
    destination_table='dw_restaurantes.dim_states',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)

"""Crear y cargar tabla business"""

to_gbq(
    dataframe=df_business,
    destination_table='dw_restaurantes.dim_business',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)

"""Crear y cargar tabla reviews"""

to_gbq(
    dataframe=df_review,
    destination_table='dw_restaurantes.fact_review',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)

"""Crear y cargar tabla cities"""

to_gbq(
    dataframe=df_city,
    destination_table='dw_restaurantes.dim_cities',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)

"""Crear y cargar tabla locations"""

to_gbq(
    dataframe=df_location,
    destination_table='dw_restaurantes.dim_locations',           # dataset.tabla
    project_id='shining-rampart-455602-a7',              # tu ID de proyecto
    if_exists='replace'                              # 'replace', 'append', o 'fail'
)