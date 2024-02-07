from apache_beam import Pipeline, DoFn, io, Flatten, Map
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from google.cloud import bigquery
from google.cloud import pubsub_v1
from apache_beam.transforms import FlatMap
import json
import math as m
import apache_beam as beam

class ProcessMessages(DoFn):
    def __init__(self, last_values):
        self.last_values = last_values

    def process(self, element):
        # Análisis del mensaje JSON
        message = json.loads(element.decode('utf-8'))
        entity_info = message.get('driver', {}) or message.get('walker', {})  # Extracción de información sobre la entidad
        entity_id = entity_info.get('id', None)
        coordinates = message.get('coordenada_actual', [])

        if entity_id is not None:
            if entity_id in self.last_values:
                # Actualización del registro existente
                self.last_values[entity_id] = coordinates
            else:
                # Agregación de un nuevo registro
                self.last_values[entity_id] = coordinates

        yield message

def haversine(coord1, coord2):
    # Radio de la Tierra en km
    R = 6371.0
    lon1, lat1 = coord1
    lon2, lat2 = coord2
    # Conversión de coordenadas a radianes
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
    entity_info = (element or {}).get('driver', {}) or (element or {}).get('walker', {}) # Extracción de información sobre la entidad
    entity_id = entity_info.get('id', None)
    coordinates = (element or {}).get('coordenada_actual', [])
    point_b = element.get('route', {}).get('points', {}).get('point_b', None)


    if entity_id is not None and point_b is not None:
        # Cálculo de la distancia entre las coordenadas actuales de la entidad y point_b de la entidad
        distance_to_point_b = haversine(coordinates, point_b)

        # Verificación de la presencia de otras entidades en un radio de 50 metros con el mismo valor de point_b
        for other_id, other_data in last_values.items():
            if other_id != entity_id and (other_id.startswith('driver') or other_id.startswith('walker')):
                other_coords = other_data.get('coordenada_actual', [])
                other_point_b = other_data.get('route', {}).get('points', {}).get('point_b', None)

                if other_point_b is not None and other_point_b == point_b:
                    distance_to_other = haversine(coordinates, other_coords)

                    if distance_to_other < 0.05:  # 50 metros en kilómetros
                        yield {
                            'entity_id': entity_id,
                            'other_id': other_id,
                            'distance_to_point_b': distance_to_point_b,
                        }

def run(argv=None):
    # Configuración de los parámetros del pipeline
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True  
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = Pipeline(options=pipeline_options)


    project_id = 'awesome-ridge-411708'
    dataset_id = "blablacar_project"
    driver_table_id = 'driver'
    walker_table_id = 'walker'
    match_table_id = 'match'
    driver_subscription_name = 'driver-sub'
    walker_subscription_name = 'walker-sub'

    last_walker_values = {}
    last_driver_values = {}

    # Configuración de BigQuery
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    
    # Uso del método get_dataset y comprobación de si existe el dataset
    dataset = client.get_dataset(dataset_ref)
    if not dataset:
        dataset = client.create_dataset(dataset_ref)

    table_ref_driver = f"{project_id}.{dataset_id}.{driver_table_id}"
    table_ref_walker = f"{project_id}.{dataset_id}.{walker_table_id}"
    table_ref_match = f"{project_id}.{dataset_id}.{match_table_id}"

    driver_schema = [
        bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Inicio", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Fin", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Distancia_total", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Tiempo_estimado", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("coordenada_actual", "RECORD", mode="REQUIRED",)
    ]

    walker_schema = [
        bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Inicio", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Fin", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Distancia_total", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Tiempo_estimado", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("coordenada_actual", "RECORD", mode="REQUIRED"), 
    ]

    match_schema = [
        bigquery.SchemaField("walker_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("driver_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("distance", "FLOAT", mode="REQUIRED"), 
    ]

    if not client.get_table(table_ref_driver):
        client.create_table(bigquery.Table(table_ref_driver, schema=driver_schema))

    if not client.get_table(table_ref_walker):
        client.create_table(bigquery.Table(table_ref_walker, schema=walker_schema))

    if not client.get_table(table_ref_match):
        client.create_table(bigquery.Table(table_ref_match, schema=match_schema))

    # Configuración de Pub/Sub
    subscriber = pubsub_v1.SubscriberClient()
    driver_subscription_path = subscriber.subscription_path(project_id, driver_subscription_name)
    walker_subscription_path = subscriber.subscription_path(project_id, walker_subscription_name)

    # Convertir el esquema a un formato compatible con JSON
    driver_schema_json = [field.to_api_repr() for field in driver_schema]

    # Definición del pipeline, utilizando driver_schema_json para el esquema
    driver_messages = (
        p
        | 'ReadDriverMessages' >> io.ReadFromPubSub(subscription=driver_subscription_path)
        #| 'ParseDriverMessages' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        |  'ParseDriverMessages' >> Map(lambda x: x)
        | 'ProcessDriverMessages' >> beam.ParDo(ProcessMessages(last_driver_values))
        | "Print Data1" >> beam.Map(print) # Ahora imprime los resultados para entender cómo van los datos, donde hay errores
     

    )

    walker_messages = (
        p
        | 'ReadwalkerMessages' >> io.ReadFromPubSub(subscription=walker_subscription_path)
        #| 'ParsewalkerMessages' >> Map(lambda x: json.loads(x.decode('utf-8')))
        |  'ParsewalkerMessages' >> Map(lambda x: x)
        | 'ProcesswalkerMessages' >> beam.ParDo(ProcessMessages(last_walker_values))
        | "Print Data2" >> beam.Map(print) # Ahora imprime los resultados para entender cómo van los datos, donde hay errores

    )

    matching_results = (
        (walker_messages, driver_messages)
        | 'Flatten' >> Flatten()
        | 'CalculateDistance' >> FlatMap(calculate_distance, last_values=last_driver_values)

    )

    # Ejecución del pipeline
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
