from google.cloud import bigquery

def create_bigquery_table(project_id, dataset_id, table_id, driver_data):
    # Configura el cliente de BigQuery
    client = bigquery.Client(project=project_id)

    # Define el conjunto de datos y la tabla
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)

    # Define el esquema
    schema = [
        bigquery.SchemaField("Driver_information", "STRING"),
    ]

    # Crea la tabla con el esquema especificado
    table_ref = dataset.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Inserta los datos en la tabla
    rows_to_insert = [{"Driver_information": driver.get("Driver_information")} for driver in driver_data]

    # Inserta las filas en la tabla
    errors = client.insert_rows(table, rows_to_insert)

    if errors:
        print(f"Error al insertar filas: {errors}")
    else:
        print("Datos insertados correctamente.")