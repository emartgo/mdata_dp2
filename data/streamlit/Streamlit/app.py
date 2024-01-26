import streamlit as st
import json
import folium

# Cargar el JSON con la ruta del viaje
with open('/Users/pablomartinomdedeu/Desktop/EDEM Master/GitHub/MDATAPROJECT2/mdata_dp2/data/streamlit/Streamlit/rutas/01_coche.geojson', 'r') as file:
    data = json.load(file)

# Crear un mapa con folium
m = folium.Map(location=[data['route'][0]['latitude'], data['route'][0]['longitude']], zoom_start=12)

# Agregar la ruta al mapa
folium.PolyLine(locations=[[point['latitude'], point['longitude']] for point in data['route']],
               color='blue').add_to(m)

# Mostrar el mapa usando Streamlit
st.title('Ruta de Viaje')
st.markdown('Mapa que muestra la ruta del viaje.')

# Mostrar el mapa de folium usando el componente de Streamlit
folium_static(m)
