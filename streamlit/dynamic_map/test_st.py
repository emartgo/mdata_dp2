import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import time
import json

st.title('Rayo Mcqueen vs Wally')

with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/01_coche.geojson", "r") as file:
    data = json.load(file)
with open("/Users/emart/Documents/GitHub/mdata_dp2/streamlit/02_coche.geojson", "r") as file_02:
    data_02 = json.load(file_02)

# Obtener las coordenadas
coordinates = data['features'][0]['geometry']['coordinates']
coordinates_02 = data_02['features'][0]['geometry']['coordinates']

# Inicializar datos
df = pd.DataFrame({
    'lat': [data['features'][1]['geometry']['coordinates'][1]],
    'lon': [data['features'][1]['geometry']['coordinates'][0]]
})
df_02 = pd.DataFrame({
    'lat': [data_02['features'][1]['geometry']['coordinates'][1]],
    'lon': [data_02['features'][1]['geometry']['coordinates'][0]]
})

# Crear un mapa más grande con un estilo de mapa más simple
fig = px.line_mapbox(df, lat='lat', lon='lon',
                     zoom=12, height=700,  # Tamaño más grande
                     mapbox_style="carto-positron")  # Estilo más simple
map_container = st.plotly_chart(fig, use_container_width=True)
# Simular la adición de nuevos puntos a la ruta
for coord in coordinates:
    # Generar nuevas coordenadas
    new_lat = coord[1]
    new_lon = coord[0]
    # Añadir las nuevas coordenadas al DataFrame
    new_point = pd.DataFrame({'lat': [new_lat], 'lon': [new_lon]})
    df = pd.concat([df, new_point], ignore_index=True)
    time.sleep(0.01)
    # Actualizar gráfico
    fig = px.line_mapbox(df, lat='lat', lon='lon',
                         zoom=12, height=700,  # Tamaño más grande
                         mapbox_style="carto-positron")  # Estilo más simple
    map_container.plotly_chart(fig, use_container_width=True)