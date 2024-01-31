import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import time
import json

st.title('Rayo Mcqueen vs Wally')

# Cargar datos
with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/01_coche.geojson", "r") as file:
    data = json.load(file)
with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/03_coche.geojson", "r") as file_02:
    data_02 = json.load(file_02)

# Obtener coordenadas
coordinates = data['features'][0]['geometry']['coordinates']
coordinates_02 = data_02['features'][0]['geometry']['coordinates']

# Inicializar DataFrames vacíos
df = pd.DataFrame(columns=['lat', 'lon', 'vehicle'])
df_02 = pd.DataFrame(columns=['lat', 'lon', 'vehicle'])

# Crear un mapa inicial
fig = px.line_mapbox(df, lat='lat', lon='lon', color='vehicle',
                     zoom=12, height=700,
                     mapbox_style="carto-positron")
map_container = st.plotly_chart(fig, use_container_width=True)

# Iterar sobre las coordenadas
max_length = max(len(coordinates), len(coordinates_02))
for i in range(max_length):
    if i < len(coordinates):
        # Añadir punto a la ruta de Rayo Mcqueen
        new_point = pd.DataFrame({'lat': [coordinates[i][1]], 'lon': [coordinates[i][0]], 'vehicle': ['Rayo Mcqueen']})
        df = pd.concat([df, new_point], ignore_index=True)
    
    if i < len(coordinates_02):
        # Añadir punto a la ruta de Wally
        new_point_02 = pd.DataFrame({'lat': [coordinates_02[i][1]], 'lon': [coordinates_02[i][0]], 'vehicle': ['Wally']})
        df_02 = pd.concat([df_02, new_point_02], ignore_index=True)

    # Combinar DataFrames
    df_combined = pd.concat([df, df_02])

    # Actualizar gráfico
    fig = px.line_mapbox(df_combined, lat='lat', lon='lon', color='vehicle',
                         zoom=12, height=700,
                         mapbox_style="carto-positron")
    map_container.plotly_chart(fig, use_container_width=True)

    # Esperar antes de añadir el siguiente punto
    time.sleep(0.1)  # Ajusta este valor para cambiar la velocidad de actualización
