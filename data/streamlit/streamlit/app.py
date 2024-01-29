import streamlit as st
import json
import folium

# Ruta al archivo geoJSON
ruta_geojson = '/Users/pablomartinomdedeu/Desktop/EDEM Master/GitHub/MDATAPROJECT2/mdata_dp2/data/streamlit/streamlit/01_coche.geojson'

# Cargar el geoJSON con la ruta del viaje
with open(ruta_geojson, 'r') as file:
    data = json.load(file)

# Acceder a las coordenadas de la geometría en el geoJSON
coordinates = data['features'][0]['geometry']['coordinates']

# Crear un mapa con folium
m = folium.Map(location=[coordinates[0][1], coordinates[0][0]], zoom_start=12)

# Agregar la ruta al mapa
folium.PolyLine(locations=[[point[1], point[0]] for point in coordinates],
               color='blue').add_to(m)

# Mostrar el mapa usando Streamlit
st.title('Ruta de Viaje')
st.markdown('Mapa que muestra la ruta del viaje.')

# Mostrar el mapa de folium usando el componente de Streamlit
stfolium.map(m)
