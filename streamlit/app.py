import streamlit as st
import plotly.express as px
import pandas as pd
import time
from google.cloud import bigquery

st.title('Rayo Mcqueen vs Wally')

project_id = 'titanium-gantry-411715'
dataset_id = 'blablacar_project'
driver_table_id = 'driver'
walker_table_id = 'walker'
match_table_id = 'match'

# Set up the BigQuery client
client_bq = bigquery.Client(project=project_id)
map_container = st.empty()

# Function to determine hover_name based on Tipo
def get_hover_name(row):
    if row['Tipo'] == 'Conductor' or row['Tipo'] == 'Conductor Match':
        return row['id_driver']
    else:
        return row['id_walker']

# Loop to update data and map every second
while True:
    # SQL query to get the latest coordinates of drivers
    query_drivers = f"""
    SELECT Id as id_driver, coordenada_actual_latitud AS latitud, coordenada_actual_longitud AS longitud, 'Conductor' AS Tipo
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY Id ORDER BY orden DESC) AS rn
        FROM {project_id}.{dataset_id}.{driver_table_id}
    )
    WHERE rn = 1
    """

    # Execute drivers query
    df_drivers = client_bq.query(query_drivers).to_dataframe()

    # SQL query to get the latest coordinates of walkers
    query_walkers = f"""
    SELECT Id as id_walker, coordenada_actual_latitud AS latitud, coordenada_actual_longitud AS longitud, 'Peaton' AS Tipo
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY Id ORDER BY orden DESC) AS rn
        FROM {project_id}.{dataset_id}.{walker_table_id}
    )
    WHERE rn = 1
    """

    # Execute walkers query
    df_walkers = client_bq.query(query_walkers).to_dataframe()

    # SQL query to get matches
    query_matches = f"""
    SELECT driver_id
    FROM {project_id}.{dataset_id}.{match_table_id}
    """

    # Fetch matches
    df_matches = client_bq.query(query_matches).to_dataframe()

    # Filter out drivers with matches
    matched_drivers = df_matches['driver_id'].unique()
    df_drivers.loc[df_drivers['id_driver'].isin(matched_drivers), 'Tipo'] = 'Conductor Match'

    # Combine drivers and walkers DataFrames, filtering out walkers with matches
    df_combined = pd.concat([df_drivers, df_walkers[~df_walkers['id_walker'].isin(matched_drivers)]], ignore_index=True)

    # Create a map with Plotly Express
    fig = px.scatter_mapbox(df_combined, lat="latitud", lon="longitud",
                            color="Tipo",  # Use the 'Tipo' column to differentiate the points
                            zoom=11, height=600,
                            mapbox_style="open-street-map")

    # Update the map container in Streamlit with the new chart
    map_container.plotly_chart(fig, use_container_width=True)

    # Wait for 1 second before the next update
    time.sleep(1)
