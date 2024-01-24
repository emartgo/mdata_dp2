from google.cloud import bigquery
from google.cloud import pubsub_v1
import json

project_id = 'titanium-gantry-411715'
dataset_id = 'blablacar_project'
table_id = 'conductor'

# Configura el cliente BigQuery
client_bq = bigquery.Client(project=project_id)

# Verifica si el conjunto de datos ya existe, si no, créalo
dataset_ref = client_bq.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
if not client_bq.get_dataset(dataset):
    dataset = client_bq.create_dataset(dataset)

# Verifica si la tabla ya existe, si no, créala con los esquemas especificados
table_ref = dataset_ref.table(table_id)
schema = [
    bigquery.SchemaField("Driver_information", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Route_coordinates", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Route_information", "STRING", mode="REQUIRED"),
]
table = bigquery.Table(table_ref, schema=schema)
if not client_bq.get_table(table):
    table = client_bq.create_table(table)

# Configura el cliente de Pub/Sub y suscripción
subscription_name = 'projects/titanium-gantry-411715/subscriptions/driver-sub'
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Función para procesar mensajes de Pub/Sub
def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    # Extrae los campos del mensaje
    driver_info = data["Driver_information"]
    route_coords = float(data["Route_coordinates"])
    route_info = data["Route_information"]

    # Inserta los datos en BigQuery
    rows_to_insert = [(driver_info, route_coords, route_info)]
    errors = client_bq.insert_rows(table, rows_to_insert)

    if errors:
        print(f"Error al insertar filas en BigQuery: {errors}")
    else:
        print(f"Datos insertados correctamente en BigQuery: {rows_to_insert}")

    # Marca el mensaje como procesado
    message.ack()

# Configura la suscripción de Pub/Sub
subscriber.subscribe(subscription_path, callback=callback)

print(f"Escuchando mensajes de Pub/Sub en la suscripción {subscription_path}...")
try:
    # Mantiene el script en ejecución para escuchar mensajes de Pub/Sub
    subscriber.start()
    subscriber.wait_for_termination()
except KeyboardInterrupt:
    # Detiene la suscripción al recibir una interrupción de teclado
    subscriber.stop()
