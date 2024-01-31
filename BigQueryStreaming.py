from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
from google.cloud.pubsub_v1.types import ReceivedMessage
import time
import math as m

project_id = 'titanium-gantry-411715'
dataset_id = 'blablacar_project'
driver_table_id = 'driver'
pedestrian_table_id = 'pedestrian'
driver_subscription_name = 'driver-sub'
pedestrian_subscription_name = 'pedestrian-sub'
match_table_id = 'match'

# Agregar diccionarios para almacenar temporalmente el último valor de pedestrian y driver
last_pedestrian_values = {}
last_driver_values = {}

# Configura el cliente BigQuery
client_bq = bigquery.Client(project=project_id)

# Verifica si el conjunto de datos ya existe, si no, créalo
dataset_ref = client_bq.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

try:
    dataset = client_bq.create_dataset(dataset)
    print(f"Conjunto de datos {dataset.dataset_id} creado correctamente.")
except Exception as e:
    print(f"Error al crear el conjunto de datos: {e}")

# Define los esquemas de las tablas
driver_schema = [
    bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Inicio", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Distancia_total", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Tiempo_estimado", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual", "FLOAT", mode="REQUIRED"),
]

pedestrian_schema = [
    bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Inicio", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Distancia_total", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Tiempo_estimado", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual", "FLOAT", mode="REQUIRED"),
]

# Define el esquema de la tabla 'match'
match_schema = [
    bigquery.SchemaField("pedestrian_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("driver_id", "INTEGER", mode="REQUIRED"),
]

# Verifica y crea las tablas si no existen
driver_table_ref = dataset_ref.table(driver_table_id)
driver_table = bigquery.Table(driver_table_ref, schema=driver_schema)

pedestrian_table_ref = dataset_ref.table(pedestrian_table_id)
pedestrian_table = bigquery.Table(pedestrian_table_ref, schema=pedestrian_schema)

match_table_ref = dataset_ref.table(match_table_id)
match_table = bigquery.Table(match_table_ref, schema=match_schema)

for table, table_ref, subscription_name in [(driver_table, driver_table_ref, driver_subscription_name),
                                            (pedestrian_table, pedestrian_table_ref, pedestrian_subscription_name)]:
    try:
        table = client_bq.create_table(table)
        print(f"Tabla {table.table_id} creada correctamente.")
    except Exception as e:
        print(f"Error al crear la tabla {table.table_id}: {e}")

# Configura el cliente de Pub/Sub y suscripciones
subscriber = pubsub_v1.SubscriberClient()

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

# Define la función de callback
def callback(received_message: ReceivedMessage, table: bigquery.Table, subscription_path: str) -> None:
    data = json.loads(received_message.message.data.decode("utf-8"))

    # Extrae los campos del mensaje
    id = int(data["driver"]["id"]) if "driver" in data else int(data["pedestrian"]["id"])
    inicio = data["route"]["points"]["point_a"]
    fin = data["route"]["points"]["point_b"]
    distancia_total = float(data["route"]["route_info"]["total_distance"])
    tiempo_estimado = data["route"]["route_info"]["estimated_time"]
    coordenada_actual = data["coordenada_actual"]

    # Verifica si ya existe un valor para este id
    if id in last_pedestrian_values and "pedestrian" in data:
        # Actualiza los valores existentes
        last_pedestrian_values[id] = (id, inicio, fin, distancia_total, tiempo_estimado, coordenada_actual)
    elif id in last_driver_values and "driver" in data:
        # Actualiza los valores existentes
        last_driver_values[id] = (id, inicio, fin, distancia_total, tiempo_estimado, coordenada_actual)
    else:
        # Si no existe, agrega un nuevo valor
        if "pedestrian" in data:
            last_pedestrian_values[id] = (id, inicio, fin, distancia_total, tiempo_estimado, coordenada_actual)
        elif "driver" in data:
            last_driver_values[id] = (id, inicio, fin, distancia_total, tiempo_estimado, coordenada_actual)

        # Inserta los datos en BigQuery
        rows_to_insert = [last_pedestrian_values[id]] if "pedestrian" in data else [last_driver_values[id]]
        errors = client_bq.insert_rows(table, rows_to_insert)

        if errors:
            print(f"Error al insertar filas en BigQuery: {errors}")
        else:
            print(f"Datos insertados correctamente en {table.table_id}: {rows_to_insert}")

        # Marca el mensaje como procesado utilizando acknowledge
        if table == driver_table:
            subscriber.acknowledge(driver_subscription_path, [received_message.ack_id])
        elif table == pedestrian_table:
            subscriber.acknowledge(pedestrian_subscription_path, [received_message.ack_id])

            # Busca coincidencias en la tabla 'driver' con distancia < 50 metros
            query_driver = f"""
                SELECT Id, Fin, coordenada_actual
                FROM `{project_id}.{dataset_id}.{driver_table_id}`
            """
            query_job_driver = client_bq.query(query_driver)
            results_driver = query_job_driver.result()

            for row_driver in results_driver:
                driver_id = row_driver['Id']
                driver_coordinate = row_driver['coordenada_actual']

                # Calcula la distancia entre las coordenadas actuales
                distance = haversine(current_coordinate, driver_coordinate)

                if distance < 50:
                    # Guarda la coincidencia en la tabla 'match'
                    match_row = [(id, driver_id)]

                    # Verifica que no existan duplicados en la tabla 'match'
                    query_match = f"""
                        SELECT *
                        FROM `{project_id}.{dataset_id}.{match_table_id}`
                        WHERE pedestrian_id = {id} AND driver_id = {driver_id}
                    """
                    query_job_match = client_bq.query(query_match)
                    results_match = query_job_match.result()

                    if not list(results_match):
                        # No hay duplicados, inserta la nueva fila en 'match'
                        errors_match = client_bq.insert_rows(match_table, match_row)

                        if errors_match:
                            print(f"Error al insertar filas en la tabla {match_table.table_id}: {errors_match}")
                        else:
                            print(f"Coincidencia registrada correctamente en {match_table.table_id}: {match_row}")

# Configura las suscripciones de Pub/Sub
driver_subscription_path = f"projects/{project_id}/subscriptions/{driver_subscription_name}"
pedestrian_subscription_path = f"projects/{project_id}/subscriptions/{pedestrian_subscription_name}"

print(f"Escuchando mensajes de Pub/Sub en la suscripción {driver_subscription_path} y {pedestrian_subscription_path}...")

try:
    while True:
        # Espera mensajes de ambas suscripciones
        driver_response = subscriber.pull(
            subscription=driver_subscription_path,
            max_messages=1,
            timeout=30
        )

        pedestrian_response = subscriber.pull(
            subscription=pedestrian_subscription_path,
            max_messages=1,
            timeout=30
        )

        for received_message in driver_response.received_messages:
            callback(received_message, driver_table, driver_subscription_path)  # Pasa subscription_path como argumento

        for received_message in pedestrian_response.received_messages:
            callback(received_message, pedestrian_table, pedestrian_subscription_path)  # Pasa subscription_path como argumento

        # Descanso para evitar un bucle infinito sin procesar mensajes
        time.sleep(1)

except KeyboardInterrupt:
    # Detiene la suscripción al recibir una interrupción de teclado
    pass
finally:
    subscriber.close()