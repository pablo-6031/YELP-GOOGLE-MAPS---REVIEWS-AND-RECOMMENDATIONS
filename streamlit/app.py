import streamlit as st
import pickle

st.title("Inspector de modelo .joblib")

joblib_file = "ml/models/modelo_sentimiento.joblib"

try:
    with open(joblib_file, 'rb') as f:
        while True:
            try:
                obj = pickle.load(f)
                st.write("Tipo de objeto:", type(obj))
            except EOFError:
                st.success("Lectura completada.")
                break
            except ModuleNotFoundError as e:
                st.error(f"ModuleNotFoundError: {e}")
                break
            except Exception as e:
                st.error(f"Otro error: {e}")
                break
except FileNotFoundError:
    st.error(f"Archivo no encontrado: {joblib_file}")

