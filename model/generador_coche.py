import os
import geojson
import time
import json
import math as m
from google.cloud import pubsub_v1

AVG_CAR_SPEED = 13.89
TOPIC_NAME = "driver"
SUBSCRIPTION_NAME = "driver-sub"

def create_topic_subscription():
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path('awesome-ridge-411708', TOPIC_NAME)
    subscription_path = subscriber.subscription_path('awesome-ridge-411708', SUBSCRIPTION_NAME)

    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Topic {topic.name} created.")
    except Exception as e:
        print(f"Topic {topic_path} already exists.")

    try:
        subscription = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"Subscription {subscription.name} created.")
    except Exception as e:
        print(f"Subscription {subscription_path} already exists.")

def publish_message(message_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('awesome-ridge-411708', TOPIC_NAME)

    message_bytes = json.dumps(message_data).encode("utf-8")

    future = publisher.publish(topic_path, data=message_bytes)
    print(f"Published message: {message_data}")
    future.result()

def get_geojson(ruta):
    archivos_geojson = [archivo for archivo in os.listdir(ruta) if archivo.endswith('.geojson')]
    datos_geojson = [geojson.load(open(os.path.join(ruta, archivo), 'r')) for archivo in archivos_geojson]

    return datos_geojson

def haversine(coord1, coord2):
    R = 6371.0
    lon1, lat1 = coord1
    lon2, lat2 = coord2

    phi1, phi2 = m.radians(lat1), m.radians(lat2)
    delta_phi = m.radians(lat2 - lat1)
    delta_lambda = m.radians(lon2 - lon1)

    a = m.sin(delta_phi / 2)**2 + m.cos(phi1) * m.cos(phi2) * m.sin(delta_lambda / 2)**2
    c = 2 * m.atan2(m.sqrt(a), m.sqrt(1 - a))

    distance = R * c * 1000
    return distance

def calculate_time(coord1, coord2, speed):
    distance = haversine(coord1, coord2)
    time_hours = distance / speed
    return time_hours

def transform_json(data, current_coordinates):
    id = data['driver']['id']
    point_coordinates = [data['features'][1]['geometry']['coordinates'], data['features'][2]['geometry']['coordinates']]
    coordinates = data['features'][0]['geometry']['coordinates']
    distances = [haversine(coordinates[i], coordinates[i+1]) for i in range(len(coordinates)-1)]
    total_distance = round(sum(distances), 3)
    estimated_time = round(total_distance / AVG_CAR_SPEED / 60, 3)

    return {
        "driver": {"id": id},
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
        "coordenada_actual": current_coordinates
    }

geojson_path = "./model/data/car/"
geojson_list = get_geojson(geojson_path)

# Crear el tema y la suscripción si es necesario
create_topic_subscription()

while True:
    for current_geojson in geojson_list:
        current_coordinates = current_geojson['features'][0]['geometry']['coordinates'][0]
        current_json = transform_json(current_geojson, current_coordinates)
        coordinates_array = current_geojson['features'][0]['geometry']['coordinates']
        coordinates_array.pop(0)  # Eliminar el primer elemento, ya que ya está asignado como coordenada_actual

        if coordinates_array:
            current_json['coordenada_actual'] = coordinates_array[0]
            coordinates_array.pop(0)

            publish_message(current_json)

    time.sleep(1)
