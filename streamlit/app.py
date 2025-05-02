import streamlit as st
import time

# Usar st.empty() para controlar la actualización de la UI
container = st.empty()

for i in range(10):
    container.text(f"Mensaje {i}")
    time.sleep(1)  # Pausa para simular un tiempo de espera
