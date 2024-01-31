import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import math as m

class ProcessMessages(beam.DoFn):
    def __init__(self, last_values):
        self.last_values = last_values

    def process(self, element):
        message = json.loads(element.data.decode('utf-8'))
        entity_id = message['Id']
        coordinates = message['coordenada_actual']

        if entity_id in self.last_values:
            # Update existing entry
            self.last_values[entity_id] = coordinates
        else:
            # Add new entry
            self.last_values[entity_id] = coordinates

        yield message

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

def calculate_distance(element, last_values):
    entity_id = element['Id']
    coordinates = element['coordenada_actual']
    
    if entity_id.startswith('driver'):
        # Check for pedestrians within 50 meters
        for pedestrian_id, pedestrian_coords in last_values.items():
            if pedestrian_id.startswith('pedestrian'):
                distance = haversine(coordinates, pedestrian_coords)
                if distance < 0.05:  # 50 meters in kilometers
                    yield {
                        'driver_id': entity_id,
                        'pedestrian_id': pedestrian_id
                    }

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    p = beam.Pipeline(options=pipeline_options)

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
    table_ref_driver = dataset_ref.table(driver_table_id)
    table_ref_pedestrian = dataset_ref.table(pedestrian_table_id)
    table_ref_match = dataset_ref.table(match_table_id)

    if not client.dataset(dataset_ref).exists():
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
    ]

    if not client.table(table_ref_driver).exists():
        client.create_table(bigquery.Table(table_ref_driver, schema=driver_schema))

    if not client.table(table_ref_pedestrian).exists():
        client.create_table(bigquery.Table(table_ref_pedestrian, schema=pedestrian_schema))

    if not client.table(table_ref_match).exists():
        client.create_table(bigquery.Table(table_ref_match, schema=match_schema))

    # Configuración de Pub/Sub
    subscriber = pubsub_v1.SubscriberClient()
    driver_subscription_path = subscriber.subscription_path(project_id, driver_subscription_name)
    pedestrian_subscription_path = subscriber.subscription_path(project_id, pedestrian_subscription_name)

    # Definición del pipeline
    driver_messages = (
        p
        | 'ReadDriverMessages' >> beam.io.ReadFromPubSub(subscription=driver_subscription_path)
        | 'ParseDriverMessages' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        | 'ProcessDriverMessages' >> beam.ParDo(ProcessMessages(last_driver_values))
        | 'WriteDriverToBigQuery' >> beam.io.WriteToBigQuery(
            table=table_ref_driver,
            schema=driver_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    pedestrian_messages = (
        p
        | 'ReadPedestrianMessages' >> beam.io.ReadFromPubSub(subscription=pedestrian_subscription_path)
        | 'ParsePedestrianMessages' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        | 'ProcessPedestrianMessages' >> beam.ParDo(ProcessMessages(last_pedestrian_values))
        | 'WritePedestrianToBigQuery' >> beam.io.WriteToBigQuery(
            table=table_ref_pedestrian,
            schema=pedestrian_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    matching_results = (
        (pedestrian_messages, driver_messages)
        | 'Flatten' >> beam.Flatten()
        | 'CalculateDistance' >> beam.FlatMap(calculate_distance, last_values=last_driver_values)
        | 'WriteMatchToBigQuery' >> beam.io.WriteToBigQuery(
            table=table_ref_match,
            schema=match_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    # Ejecución del pipeline
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
