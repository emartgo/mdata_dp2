import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import time
import json

st.title('Rayo Mcqueen vs Wally')

with open("01_coche.geojson", "r") as file:
    data = json.load(file)

# Obtener las coordenadas
coordinates = data['features'][0]['geometry']['coordinates']
print(coordinates)

# Inicializar datos
df = pd.DataFrame({
    'lat': [data['features'][1]['geometry']['coordinates'][1]],
    'lon': [data['features'][1]['geometry']['coordinates'][0]]
})

print(df)

# Crear un mapa más grande con un estilo de mapa más simple
fig = px.line_mapbox(df, lat='lat', lon='lon',
                     zoom=12, height=900,  # Tamaño más grande
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
                         zoom=12, height=900,  # Tamaño más grande
                         mapbox_style="carto-positron")  # Estilo más simple
    map_container.plotly_chart(fig, use_container_width=True)