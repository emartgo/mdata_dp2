import streamlit as st
from google.cloud import bigquery
from googlemaps import Client
import geopandas as gpd 
from shapely.geometry import Point
from shapely import wkt
import folium
from streamlit_folium import folium_static

# Задайте свои данные для BigQuery и Google Maps
google_maps_api_key = 'AIzaSyB25U6Swr9pp5ajgkzFs5XNHTDRLNcC08k'
bq_project_id = 'awesome-ridge-411708'
bq_dataset_id = 'datasetedem'

# Инициализация клиента BigQuery
bq_client = bigquery.Client(project=bq_project_id)

# SQL-запрос для получения данных маршрута машины
car_route_query = """
    SELECT
        car_id,
        ST_AsText(ST_SnapToGrid(ST_GeogPoint(lon, lat), 0.001)) AS location
    FROM
        `{}.{}.route car`
""".format(bq_project_id, bq_dataset_id)

# SQL-запрос для получения данных маршрута пешехода
pedestrian_route_query = """
    SELECT
        walker_id,
        ST_AsText(ST_SnapToGrid(ST_GeogPoint(lon, lat), 0.001)) AS location
    FROM
        `{}.{}.route walker`
""".format(bq_project_id, bq_dataset_id)

# Получение данных маршрута машины
car_route_df = bq_client.query(car_route_query).to_dataframe()

# Получение данных маршрута пешехода
pedestrian_route_df = bq_client.query(pedestrian_route_query).to_dataframe()

# Преобразование строки WKT в геометрический объект для маршрута машины
car_route_df['geometry'] = car_route_df['location'].apply(wkt.loads)
car_route_gdf = gpd.GeoDataFrame(car_route_df, geometry='geometry')

# Преобразование строки WKT в геометрический объект для маршрута пешехода
pedestrian_route_df['geometry'] = pedestrian_route_df['location'].apply(wkt.loads)
pedestrian_route_gdf = gpd.GeoDataFrame(pedestrian_route_df, geometry='geometry')

# Создание карты
m = folium.Map(location=[car_route_gdf['geometry'].iloc[0].y, car_route_gdf['geometry'].iloc[0].x], zoom_start=15)

# Добавление маршрута машины на карту
car_route_line = folium.PolyLine(
    locations=[[point.y, point.x] for point in car_route_gdf['geometry']],
    color='blue',
    weight=5,
    opacity=0.7,
    tooltip='Ruta del automóvil'
).add_to(m)

# Добавление маршрута пешехода на карту
pedestrian_route_line = folium.PolyLine(
    locations=[[point.y, point.x] for point in pedestrian_route_gdf['geometry']],
    color='green',
    weight=5,
    opacity=0.7,
    tooltip='Ruta del peatón'
).add_to(m)

# Добавление ID машины и ID пешехода в таблицу
folium.Marker([car_route_gdf['geometry'].iloc[0].y, car_route_gdf['geometry'].iloc[0].x],
              icon=folium.DivIcon(html=f'<div style="font-size: 12pt; color: blue;">Car: {car_route_df["car_id"].iloc[0]}</div>')).add_to(m)

folium.Marker([pedestrian_route_gdf['geometry'].iloc[0].y, pedestrian_route_gdf['geometry'].iloc[0].x],
              icon=folium.DivIcon(html=f'<div style="font-size: 12pt; color: green;">Walker: {pedestrian_route_df["walker_id"].iloc[0]}</div>')).add_to(m)

# Отображение карты в Streamlit
st.markdown(folium_static(m))
