from google.cloud import bigquery
from google.cloud import pubsub_v1
import json

project_id = 'titanium-gantry-411715'
dataset_id = 'blablacar_project'
table_id = 'conductor'
subscription_name = 'usuario-sub'

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

# Verifica si la tabla ya existe, si no, créala con los esquemas especificados
table_ref = dataset_ref.table(table_id)
schema = [
    bigquery.SchemaField("Driver_information", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Route_coordinates", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Route_information", "STRING", mode="REQUIRED"),
]
table = bigquery.Table(table_ref, schema=schema)

try:
    table = client_bq.create_table(table)
    print(f"Tabla {table.table_id} creada correctamente.")
except Exception as e:
    print(f"Error al crear la tabla: {e}")

# Configura el cliente de Pub/Sub y suscripción
subscriber = pubsub_v1.SubscriberClient()

# Define la función de callback
def callback(received_message):
    data = json.loads(received_message.message.data.decode("utf-8"))

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

        # Marca el mensaje como procesado utilizando acknowledge
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

# Configura la suscripción de Pub/Sub
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"

print(f"Escuchando mensajes de Pub/Sub en la suscripción {subscription_path}...")
try:
    while True:
        # Espera mensajes
        response = subscriber.pull(
            subscription=subscription_path,
            max_messages=1,
            timeout=30
        )

        for received_message in response.received_messages:
            callback(received_message)

            # Marca el mensaje como procesado utilizando acknowledge
            subscriber.acknowledge(subscription_path, [received_message.ack_id])
except KeyboardInterrupt:
    # Detiene la suscripción al recibir una interrupción de teclado
    pass
finally:
    subscriber.close()
