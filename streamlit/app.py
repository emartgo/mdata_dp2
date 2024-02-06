import streamlit as st
import folium
from streamlit_folium import folium_static
import json

# Cargar el JSON con las coordenadas
with open("01_coche.geojson", "r") as file:
    data = json.load(file)

# Obtener las coordenadas
coordinates = data['features'][0]['geometry']['coordinates']

# Crear un mapa con folium
m = folium.Map(location=[coordinates[0][1], coordinates[0][0]], zoom_start=14)

# Agregar la ruta al mapa
ruta = folium.PolyLine(locations=[[point[1], point[0]] for point in coordinates], color='blue')
ruta.add_to(m)

# Mostrar el mapa usando Streamlit
st.title('Ruta de Viaje')
st.markdown('Mapa que muestra la ruta del viaje.')

# Mostrar el mapa de folium usando folium_static
folium_static(m)