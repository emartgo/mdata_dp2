from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
from google.cloud.pubsub_v1.types import ReceivedMessage
import time
import math as m
from google.api_core import exceptions as google_exceptions

project_id = 'titanium-gantry-411715'
dataset_id = 'blablacar_project'
driver_table_id = 'driver'
walker_table_id = 'walker'
driver_subscription_name = 'driver-sub'
walker_subscription_name = 'walker-sub'
match_table_id = 'match'

# Agregar diccionarios para almacenar temporalmente el último valor de walker y driver
last_walker_values = {}
last_driver_values = {}

# Configura el cliente BigQuery
client_bq = bigquery.Client(project=project_id)

# Verifica si el conjunto de datos ya existe, si no, créalo
dataset_ref = client_bq.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

try:
    dataset = client_bq.create_dataset(dataset)
    print(f"Conjunto de datos {dataset.dataset_id} creado correctamente.")
except google_exceptions.Conflict:
    print(f"El conjunto de datos {dataset.dataset_id} ya existe.")

# Define los esquemas de las tablas con el campo 'orden'
driver_schema = [
    bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Inicio_latitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Inicio_longitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin_latitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin_longitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual_latitud", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual_longitud", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("orden", "INTEGER", mode="REQUIRED"),
]

walker_schema = [
    bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Inicio_latitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Inicio_longitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin_latitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Fin_longitud", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual_latitud", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("coordenada_actual_longitud", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("orden", "INTEGER", mode="REQUIRED"),
]

# Define el esquema de la tabla 'match'
match_schema = [
    bigquery.SchemaField("walker_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("driver_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
]

# Verifica y crea las tablas si no existen
driver_table_ref = dataset_ref.table(driver_table_id)
driver_table = bigquery.Table(driver_table_ref, schema=driver_schema)

walker_table_ref = dataset_ref.table(walker_table_id)
walker_table = bigquery.Table(walker_table_ref, schema=walker_schema)

match_table_ref = dataset_ref.table(match_table_id)
match_table = bigquery.Table(match_table_ref, schema=match_schema)

for table, table_ref in [(driver_table, driver_table_ref),
                        (walker_table, walker_table_ref),
                        (match_table, match_table_ref)]:
    try:
        table = client_bq.create_table(table)
        print(f"Tabla {table.table_id} creada correctamente.")
    except google_exceptions.Conflict:
        print(f"La tabla {table.table_id} ya existe.")

# Configura el cliente de Pub/Sub y suscripciones
subscriber = pubsub_v1.SubscriberClient()

def haversine(coord1, coord2):
    # Radius of the Earth in km
    R = 6371.0
    lon1, lat1 = coord1
    lon2, lat2 = coord2
    lon1, lat1 = float(lon1), float(lat1)
    lon2, lat2 = float(lon2), float(lat2)
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
    id = int(data["driver"]["id"]) if "driver" in data else int(data["walker"]["id"])
    inicio_lat = data["route"]["points"]["point_a"][1]
    inicio_lon = data["route"]["points"]["point_a"][0]
    fin_lat = data["route"]["points"]["point_b"][1]
    fin_lon = data["route"]["points"]["point_b"][0]
    coordenada_actual_latitud = data["coordenada_actual"][1]
    coordenada_actual_longitud = data["coordenada_actual"][0]

    # Obtén el último valor de 'orden' y aumenta en 1
    query = f"""
        SELECT MAX(orden) as max_orden FROM `{project_id}.{dataset_id}.{table.table_id}`
    """
    query_job = client_bq.query(query)
    for row in query_job.result():
        max_orden = row["max_orden"]
        break
    orden = max_orden + 1 if max_orden else 1

    # Verifica si ya existe un valor para este id
    if id in last_walker_values and "walker" in data:
        last_walker_values[id] = (id, inicio_lat, inicio_lon, fin_lat, fin_lon, coordenada_actual_latitud, coordenada_actual_longitud, orden)
        table = walker_table
    elif id in last_driver_values and "driver" in data:
        last_driver_values[id] = (id, inicio_lat, inicio_lon, fin_lat, fin_lon, coordenada_actual_latitud, coordenada_actual_longitud, orden)
        table = driver_table
    else:
        if "walker" in data:
            last_walker_values[id] = (id, inicio_lat, inicio_lon, fin_lat, fin_lon, coordenada_actual_latitud, coordenada_actual_longitud, orden)
            table = walker_table
        elif "driver" in data:
            last_driver_values[id] = (id, inicio_lat, inicio_lon, fin_lat, fin_lon, coordenada_actual_latitud, coordenada_actual_longitud, orden)
            table = driver_table

    rows_to_insert = [last_walker_values[id]] if "walker" in data else [last_driver_values[id]]
    errors = client_bq.insert_rows(table, rows_to_insert)

    if errors:
        print(f"Error al insertar filas en BigQuery: {errors}")
    else:
        print(f"Datos insertados correctamente en {table.table_id}: {rows_to_insert}")

    # Marca el mensaje como procesado utilizando acknowledge
    if table == driver_table:
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])
    elif table == walker_table:
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

        # Busca coincidencias en la tabla 'driver' con distancia < 50 metros
        query_driver = f"""
            SELECT Id, Fin_latitud, Fin_longitud, coordenada_actual_latitud, coordenada_actual_longitud
            FROM `{project_id}.{dataset_id}.{driver_table_id}`
        """
        query_job_driver = client_bq.query(query_driver)
        results_driver = query_job_driver.result()

        for row_driver in results_driver:
            driver_id = row_driver['Id']
            final_driver = [row_driver['Fin_latitud'], row_driver['Fin_longitud']]
            coordenada_driver = [row_driver['coordenada_actual_latitud'], row_driver['coordenada_actual_longitud']]

            # Calcula la distancia entre las coordenadas actuales
            distance = haversine([coordenada_actual_latitud, coordenada_actual_longitud], coordenada_driver)

            if distance < 50:
                # Verifica si la coincidencia ya existe en la tabla 'match'
                query_match = f"""
                    SELECT *
                    FROM `{project_id}.{dataset_id}.{match_table_id}`
                    WHERE walker_id = {id} OR driver_id = {driver_id}
                """
                query_job_match = client_bq.query(query_match)
                results_match = query_job_match.result()

                if not list(results_match):
                    # No existe la coincidencia, guarda en la tabla 'match'
                    match_row = [(id, driver_id, 1.2*haversine(coordenada_driver, final_driver)/1000)]

                    # Inserta el registro en la tabla 'match'
                    errors_match = client_bq.insert_rows(match_table, match_row)

                    if errors_match:
                        print(f"Error al insertar filas en la tabla {match_table.table_id}: {errors_match}")
                    else:
                        print(f"Coincidencia registrada correctamente en {match_table.table_id}: {match_row}")

    # Marca el mensaje como procesado utilizando acknowledge
    subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

# Configura las suscripciones de Pub/Sub
driver_subscription_path = f"projects/{project_id}/subscriptions/{driver_subscription_name}"
walker_subscription_path = f"projects/{project_id}/subscriptions/{walker_subscription_name}"

print(f"Escuchando mensajes de Pub/Sub en la suscripción {driver_subscription_path} y {walker_subscription_path}...")

try:
    while True:
        # Espera mensajes de ambas suscripciones
        driver_response = subscriber.pull(
            subscription=driver_subscription_path,
            max_messages=4,
            timeout=10
        )

        walker_response = subscriber.pull(
            subscription=walker_subscription_path,
            max_messages=4,
            timeout=10
        )

        for received_message in driver_response.received_messages:
            callback(received_message, driver_table, driver_subscription_path)  # Pasa subscription_path como argumento

        for received_message in walker_response.received_messages:
            callback(received_message, walker_table, walker_subscription_path)  # Pasa subscription_path como argumento

        # Descanso para evitar un bucle infinito sin procesar mensajes
        time.sleep(1)

except KeyboardInterrupt:
    # Detiene la suscripción al recibir una interrupción de teclado
    pass
finally:
    subscriber.close()
