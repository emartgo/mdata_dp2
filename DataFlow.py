from apache_beam import Pipeline, DoFn, io, Flatten, Map, ParDo
from apache_beam.options.pipeline_options import PipelineOptions
import google.api_core.exceptions
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import math as m


class ProcessMessages(DoFn):
    def __init__(self, last_values):
        self.last_values = last_values


    def process(self, element):
        # Parsear el mensaje JSON
        message = json.loads(element.data.decode('utf-8'))
        entity_id = message['Id']
        coordinates = message['coordenada_actual']

        if entity_id in self.last_values:
            # Actualizar la entrada existente
            self.last_values[entity_id] = coordinates
        else:
            # Agregar una nueva entrada
            self.last_values[entity_id] = coordinates

        yield message


def haversine(coord1, coord2):
    # Radio de la Tierra en km
    R = 6371.0
    lon1, lat1 = coord1
    lon2, lat2 = coord2
    # Convertir coordenadas a radianes
    phi1, phi2 = m.radians(lat1), m.radians(lat2)
    delta_phi = m.radians(lat2 - lat1)
    delta_lambda = m.radians(lon2 - lon1)
    # Fórmula de Haversine
    a = m.sin(delta_phi / 2)**2 + m.cos(phi1) * m.cos(phi2) * m.sin(delta_lambda / 2)**2
    c = 2 * m.atan2(m.sqrt(a), m.sqrt(1 - a))
    # Distancia en metros
    distance = R * c * 1000

    return distance


def calculate_distance(element, last_values):
    entity_id = element['Id']
    coordinates = element['coordenada_actual']
    point_b = element.get('point_b', None)

    if entity_id.startswith('driver') and point_b is not None:
        # Calcular distancia entre coordenada_actual de driver y point_b de driver
        distance_to_point_b = haversine(coordinates, point_b)

        # Verificar peatones dentro de 50 metros con el mismo valor de point_b
        for pedestrian_id, pedestrian_data in last_values.items():
            if pedestrian_id.startswith('pedestrian'):
                pedestrian_coords = pedestrian_data['coordenada_actual']
                pedestrian_point_b = pedestrian_data.get('point_b', None)

                if pedestrian_point_b is not None and pedestrian_point_b == point_b:
                    distance_to_pedestrian = haversine(coordinates, pedestrian_coords)

                    if distance_to_pedestrian < 0.05:  # 50 metros en kilómetros
                        yield {
                            'driver_id': entity_id,
                            'pedestrian_id': pedestrian_id,
                            'distance_to_point_b': distance_to_point_b,
                        }

class ProcessDriverMessages(DoFn):
    def __init__(self, last_values):
        self.last_values = last_values

    def process(self, element):
        # Parsear el mensaje JSON
        message = json.loads(element.data.decode('utf-8'))
        entity_id = message['Id']
        coordinates = message['coordenada_actual']

        if entity_id in self.last_values:
            # Actualizar la entrada existente
            self.last_values[entity_id] = coordinates
        else:
            # Agregar una nueva entrada
            self.last_values[entity_id] = coordinates

        yield message


def run(argv=None):
    # Configuración de las opciones del pipeline
    pipeline_options = PipelineOptions(argv)
    p = Pipeline(options=pipeline_options)

    project_id = 'titanium-gantry-411715'
    dataset_id = 'blablacar_project'
    driver_table_id = 'driver'
    pedestrian_table_id = 'pedestrian'
    match_table_id = 'match'
    driver_subscription_name = 'projects/titanium-gantry-411715/subscriptions/driver_sub'
    pedestrian_subscription_name = 'projects/titanium-gantry-411715/subscriptions/pedestrian_sub'

    last_pedestrian_values = {}
    last_driver_values = {}

    # Configuración de BigQuery
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    table_ref_driver = f"{project_id}.{dataset_id}.{driver_table_id}"
    table_ref_pedestrian = f"{project_id}.{dataset_id}.{pedestrian_table_id}"
    table_ref_match = f"{project_id}.{dataset_id}.{match_table_id}"

    if not client.get_dataset(dataset_ref):
        client.create_dataset(dataset)

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

    match_schema = [
        bigquery.SchemaField("pedestrian_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("driver_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("distance", "FLOAT", mode="REQUIRED"), 
    ]

    driver_schema_dict = {field.name: field.field_type for field in driver_schema}
    pedestrian_schema_dict = {field.name: field.field_type for field in pedestrian_schema}
    match_schema_dict = {field.name: field.field_type for field in match_schema}
    
    # Verificación y creación de la tabla de conductores
    driver_tables = list(client.list_tables(dataset_ref))
    if table_ref_driver not in [table.table_id for table in driver_tables]:
        try:
            client.create_table(bigquery.Table(table_ref_driver, schema=driver_schema))
        except google.api_core.exceptions.Conflict:
            print(f"La tabla {table_ref_driver} ya existe en BigQuery.")

    # Verificación y creación de la tabla de peatones
    pedestrian_tables = list(client.list_tables(dataset_ref))
    if table_ref_pedestrian not in [table.table_id for table in pedestrian_tables]:
        try:
            client.create_table(bigquery.Table(table_ref_pedestrian, schema=pedestrian_schema))
        except google.api_core.exceptions.Conflict:
            print(f"La tabla {table_ref_pedestrian} ya existe en BigQuery.")

    # Verificación y creación de la tabla de coincidencias
    match_tables = list(client.list_tables(dataset_ref))
    if table_ref_match not in [table.table_id for table in match_tables]:
        try:
            client.create_table(bigquery.Table(table_ref_match, schema=match_schema))
        except google.api_core.exceptions.Conflict:
            print(f"La tabla {table_ref_match} ya existe en BigQuery.")

    # Configuración de Pub/Sub
    subscriber = pubsub_v1.SubscriberClient()
    driver_subscription_path = subscriber.subscription_path(project_id, driver_subscription_name)
    pedestrian_subscription_path = subscriber.subscription_path(project_id, pedestrian_subscription_name)

    # Definición del pipeline
    driver_messages = (
        p
        | 'ReadDriverMessages' >> io.ReadFromPubSub(subscription=driver_subscription_path)
        | 'ParseDriverMessages' >> Map(lambda x: json.loads(x.decode('utf-8')))
        | 'ProcessDriverMessages' >> ParDo(ProcessDriverMessages(last_driver_values))
        | 'WriteDriverToBigQuery' >> io.WriteToBigQuery(
            table=table_ref_driver,
            schema=driver_schema_dict,  # Utiliza el diccionario en lugar de la lista
            write_disposition=io.BigQueryDisposition.WRITE_APPEND
        )
    )

    pedestrian_messages = (
        p
        | 'ReadPedestrianMessages' >> io.ReadFromPubSub(subscription=pedestrian_subscription_path)
        | 'ParsePedestrianMessages' >> Map(lambda x: json.loads(x.decode('utf-8')))
        | 'ProcessPedestrianMessages' >> DoFn(ProcessMessages(last_pedestrian_values))
        | 'WritePedestrianToBigQuery' >> io.WriteToBigQuery(
            table=table_ref_pedestrian,
            schema=pedestrian_schema_dict,  # Utiliza el diccionario en lugar de la lista
            write_disposition=io.BigQueryDisposition.WRITE_APPEND
        )
    )

    matching_results = (
        (pedestrian_messages, driver_messages)
        | 'Flatten' >> Flatten()
        | 'CalculateDistance' >> io.FlatMap(calculate_distance, last_values=last_driver_values)
        | 'WriteMatchToBigQuery' >> io.WriteToBigQuery(
            table=table_ref_match,
            schema=match_schema_dict,  # Utiliza el diccionario en lugar de la lista
            write_disposition=io.BigQueryDisposition.WRITE_APPEND
        )
    )

    # Ejecución del pipeline
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
