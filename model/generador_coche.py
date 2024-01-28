import os
import geojson
import time
import random
import math as m
import json
from google.cloud import pubsub_v1

AVG_CAR_SPEED = 13.89

TOPIC_NAME = "driver"
SUBSCRIPTION_NAME = "driver-sub"

def create_topic_subscription():
    # Crear un cliente de Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    # Crear el nombre completo del tema y la suscripción
    topic_path = publisher.topic_path('titanium-gantry-411715', TOPIC_NAME)
    subscription_path = subscriber.subscription_path('titanium-gantry-411715', SUBSCRIPTION_NAME)

    # Intentar crear el tema (ignorar si ya existe)
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Topic {topic.name} created.")
    except Exception as e:
        print(f"Topic {topic_path} already exists.")

    # Intentar crear la suscripción (ignorar si ya existe)
    try:
        subscription = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"Subscription {subscription.name} created.")
    except Exception as e:
        print(f"Subscription {subscription_path} already exists.")
        
def publish_message(message_data):
    # Publicar un mensaje en el tema
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('titanium-gantry-411715', TOPIC_NAME)
    
    # Convertir el mensaje a bytes
    message_bytes = json.dumps(message_data).encode("utf-8")

    # Publicar el mensaje
    future = publisher.publish(topic_path, data=message_bytes)
    print(f"Published message: {message_data}")
    future.result()

def get_geojson(ruta):
    archivos_geojson = [archivo for archivo in os.listdir(ruta) if archivo.endswith('.geojson')]
    datos_geojson = []

    for archivo in archivos_geojson:
        ruta_completa = os.path.join(ruta, archivo)
        
        with open(ruta_completa, 'r') as f:
            geojson_data = geojson.load(f)
            datos_geojson.append(geojson_data)

    return datos_geojson

def get_points_coordinates(data):
    coords_a = data['features'][1]['geometry']['coordinates']
    coords_b = data['features'][2]['geometry']['coordinates']
    coords = [coords_a, coords_b]
    return coords

def get_coordinates(data):
    coords = data['features'][0]['geometry']['coordinates']
    return coords

def get_coords_to_meters(data):
    return [haversine(data[i], data[i+1]) for i in range(len(data)-1)]

        
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

def get_total_distance(data):
    total_distance = round(sum(data),3)
    return total_distance

def get_estimated_time(data):
    time = round(data/AVG_CAR_SPEED,3) # in seconds
    time_minutes = round(time/60,3) # in minutes
    return time_minutes

def transform_json(data):
    id = data['driver']['id']
    point_coordinates = get_points_coordinates(data)
    coordinates = get_coordinates(data)
    distances = get_coords_to_meters(coordinates)
    total_distance = get_total_distance(distances)
    estimated_time = get_estimated_time(total_distance)
    
    structure = {
        "id": {"id": id},
        "route": {
            "points": {
                "point_a": point_coordinates[0],
                "point_b": point_coordinates[1]
            },
            "route_info": {
                "total_distance": total_distance,
                "estimated_time": estimated_time
            }
        },
    }
    return structure

def calculate_time(coord1, coord2, speed):
    distance = haversine(coord1,coord2)
    time_hours = distance / speed
    return time_hours


geojson_path = "./model/data/car/"
geojson_list = get_geojson(geojson_path)
current_geojson = random.choice(geojson_list)
current_json = transform_json(current_geojson)
current_json['coordenada_actual'] = current_json['route']['points']['point_a']
coordinates_array = current_geojson['features'][0]['geometry']['coordinates']
first_coord = coordinates_array.pop(0)

while coordinates_array:
    speed_kmh = 50
    wait_time = calculate_time(first_coord, current_json['coordenada_actual'], speed_kmh)

    # Espera el tiempo calculado
    time.sleep(wait_time)

    # Actualiza coordenada_actual y elimina el valor de la posición 0 del array
    current_json['coordenada_actual'] = coordinates_array[0]
    coordinates_array.pop(0)

    # Verifica si es la primera vez y crea el tema y la suscripción si es necesario
    if 'pubsub_initialized' not in locals():
        create_topic_subscription()
        locals()['pubsub_initialized'] = True

    # Publicar el mensaje con el valor actualizado
    publish_message(current_json)