import streamlit as st
import plotly.express as px
import pandas as pd
import time
import json
import numpy as np

st.title('Rayo Mcqueen vs Wally')

# Opción para auto ajustar el mapa
auto_zoom = st.checkbox("Auto ajustar mapa a vehículos")

# Cargar datos
with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/01_coche.geojson", "r") as file:
    data = json.load(file)
with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/02_coche.geojson", "r") as file_02:
    data_02 = json.load(file_02)

# Obtener coordenadas
coordinates = data['features'][0]['geometry']['coordinates']
coordinates_02 = data_02['features'][0]['geometry']['coordinates']

# Configurar el centro del mapa
center_lat = np.mean([coord[1] for coord in coordinates])
center_lon = np.mean([coord[0] for coord in coordinates])

# Inicializar DataFrames vacíos
df = pd.DataFrame(columns=['lat', 'lon', 'vehicle'])
df_02 = pd.DataFrame(columns=['lat', 'lon', 'vehicle'])

# Crear un mapa inicial
fig = px.scatter_mapbox(df, lat='lat', lon='lon', color='vehicle',
                        zoom=12, height=700,
                        center={"lat": center_lat, "lon": center_lon},
                        mapbox_style="carto-positron")
map_container = st.plotly_chart(fig, use_container_width=True)

# Iterar sobre las coordenadas
max_length = max(len(coordinates), len(coordinates_02))
for i in range(max_length):
    if i < len(coordinates):
        # Añadir punto actual de Rayo Mcqueen
        df = pd.DataFrame({'lat': [coordinates[i][1]], 'lon': [coordinates[i][0]], 'vehicle': ['Rayo Mcqueen']})
    
    if i < len(coordinates_02):
        # Añadir punto actual de Wally
        df_02 = pd.DataFrame({'lat': [coordinates_02[i][1]], 'lon': [coordinates_02[i][0]], 'vehicle': ['Wally']})

    # Combinar DataFrames
    df_combined = pd.concat([df, df_02])

    # Actualizar gráfico
    if auto_zoom:
        fig = px.scatter_mapbox(df_combined, lat='lat', lon='lon', color='vehicle',
                                zoom=12, height=700,
                                mapbox_style="carto-positron")
    else:
        fig = px.scatter_mapbox(df_combined, lat='lat', lon='lon', color='vehicle',
                                zoom=12, height=700,
                                center={"lat": center_lat, "lon": center_lon},
                                mapbox_style="carto-positron")
    map_container.plotly_chart(fig, use_container_width=True)

    # Esperar antes de añadir el siguiente punto
    time.sleep(0.1)  # Ajusta este valor para cambiar la velocidad de actualización
