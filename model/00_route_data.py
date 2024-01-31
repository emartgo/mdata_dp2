import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from functions import *
import math as m
from google.cloud import bigquery
from google.cloud.exceptions import Conflict


ROOT = "data/"
AVG_CAR_SPEED = 13.89 # m/s
AVG_WALKER_SPEED = 1.389 # m/s --  10 less than the car speed

# Calculate the distance between two points
def haversine(coord1, coord2):
    # Radius of the Earth in km
    R = 6371.0
    lon1, lat1 = coord1
    lon2, lat2 = coord2
    # Convert coordinates to radians
    phi1, phi2 = m.radians(lat1), m.radians(lat2)
    delta_phi = m.radians(lat2 - lat1)
    delta_lambda = m.radians(lon2 - lon1)
    # Haversine formula
    a = m.sin(delta_phi / 2)**2 + m.cos(phi1) * m.cos(phi2) * m.sin(delta_lambda / 2)**2
    c = 2 * m.atan2(m.sqrt(a), m.sqrt(1 - a))
    # Distance in meters
    distance = R * c * 1000

    return distance
# Read GeoJSON
def read_geojson(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data
# Get Coordinates of the point A and B
def get_points_coordinates(data):
    coords_a = data['features'][1]['geometry']['coordinates']
    coords_b = data['features'][2]['geometry']['coordinates']
    coords = [coords_a, coords_b]
    return coords
# Get Coordinates of the route
def get_coordinates(data):
    coords = data['features'][0]['geometry']['coordinates']
    return coords
# Transform coordinates to meters
def get_coords_to_meters(data):
    distances = []
    for i in range(len(data)):
        distance = haversine(data[i], data[i+1]) if i+1 < len(data) else 0
        distances.append(distance)
        
    return distances
# Get total distance
def get_total_distance(data):
    total_distance = round(sum(data),3)
    return total_distance
# Get estimated time of the total route
def get_estimated_time(data):
    time = round(data/AVG_CAR_SPEED,3) # in seconds
    time_minutes = round(time/60,3) # in minutes
    return time_minutes
# Get the right format of the json with all the driver route information
def transform_json(data):
    driver_id = data["driver"]["id"]
    point_coordinates = get_points_coordinates(data)
    coordinates = get_coordinates(data)
    distances = get_coords_to_meters(coordinates)
    total_distance = get_total_distance(distances)
    estimated_time = get_estimated_time(total_distance)
    
    transformed_coordinates = [{"latitude": lat, "longitude": lon} for lat, lon in coordinates]
    
    structure = {
        "driver": {"driver_id": driver_id},
        "route": {
            "points": {
                "point_a": {
                    "coordinates": point_coordinates[0]
                },
                "point_b": {
                    "coordinates": point_coordinates[1]
                }
            },
            # "coordinates": transformed_coordinates,
        },
        "route_info": {
            # "distances": distances,
            "total_distance": total_distance,
            "estimated_time": estimated_time
        }
    }
    
    return structure

"""
After the driver has uploaded all the route information in the APP, we receive a GeoJSON file with the route. Then, we extract all
the information about the time and distances and send them back to the APP. 
Now, the user (pedestrian) is able to see a list of drivers when he set an arrival point for his route. 
The list is sorted by the estimated time of the route. The user can choose the driver that he wants.
------------------------------------
In this first part, we do not manage times, so we understand that they do not have conflicts. A driver upload a route and he 
wait until a pedestrian choose him.
"""

"""project_id = 'mdata-dp2'
dataset_id = 'blablacar'
table_id = 'driver_route_information'

schema = [
    bigquery.SchemaField("driver", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("driver_id", "STRING", mode="NULLABLE"),
    ]),
    # bigquery.SchemaField("route", "RECORD", mode="NULLABLE", fields=[
    #     bigquery.SchemaField("points", "RECORD", mode="NULLABLE", fields=[
    #         bigquery.SchemaField("point_a", "RECORD", mode="NULLABLE", fields=[
    #             bigquery.SchemaField("coordinates", "FLOAT", mode="REPEATED"),
    #         ]),
    #         bigquery.SchemaField("point_b", "RECORD", mode="NULLABLE", fields=[
    #             bigquery.SchemaField("coordinates", "FLOAT", mode="REPEATED"),
    #         ]),
    #     ]),
    #     # Esta es la parte cambiada para reflejar una lista de pares de coordenadas
    #     bigquery.SchemaField("coordinates", "RECORD", mode="REPEATED", fields=[
    #         bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    #         bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
    #     ]),
    # ]),
    bigquery.SchemaField("route_info", "RECORD", mode="NULLABLE", fields=[
        # bigquery.SchemaField("distances", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("total_distance", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("estimated_time", "FLOAT", mode="NULLABLE"),
    ]),
]


# Configura el cliente BigQuery
client_bq = bigquery.Client(project=project_id)
dataset_ref = client_bq.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
# Verifica si la tabla ya existe, si no, créala con los esquemas especificados
table_ref = dataset_ref.table(table_id)
table = bigquery.Table(table_ref, schema=schema)

try:
    table = client_bq.create_table(table)
    print(f"Tabla {table.table_id} creada correctamente.")
except Conflict as e:
    print(f"La tabla {table.table_id} ya existe.")
except Exception as e:
    print(f"Error al crear la tabla: {e}")"""

# Get the information about the routes of the cars
for file in get_geojson(ROOT+"car"): # Change to walker to get the walker route
    with beam.Pipeline(options=PipelineOptions()) as p:
        """
        Read the GeoJSON file and get the coordinates of the route. Then, transform the coordinates to meters and calculate the time of the route.
        Each data will be sent to BigQuery. With all the information, once there is a match, the route will start and we will manage the timing.
        """
        (
            p 
            | 'Create' >> beam.Create([file])
            | 'ReadGeoJSON' >> beam.Map(read_geojson)
            | 'GetJSON' >> beam.Map(transform_json)
            | 'Print' >> beam.Map(print)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(   
                table="mdata-dp2:blablacar.driver_route_information",
                schema="driver:RECORD, route_info:RECORD",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
                )
        )
