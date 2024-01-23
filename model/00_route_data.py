import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from functions import *
import math as m

ROOT = "data/"
AVG_CAR_SPEED = 13.89 # m/s
AVG_WALKER_SPEED = 1.389 # m/s --  10 less than the car speed

# Calculate the distance between two points
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
# Read GeoJSON
def read_geojson(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data
# Get Coordinates of the point A and B
def get_points_coordinates(data):
    coords_a = data['features'][1]['geometry']['coordinates']
    coords_b = data['features'][2]['geometry']['coordinates']
    coords = [coords_a, coords_b]
    return coords
# Get Coordinates of the route
def get_coordinates(data):
    coords = data['features'][0]['geometry']['coordinates']
    return coords
# Transform coordinates to meters
def get_coords_to_meters(data):
    distances = []
    for i in range(len(data)):
        distance = haversine(data[i], data[i+1]) if i+1 < len(data) else 0
        distances.append(distance)
        
    return distances
# Get total distance
def get_total_distance(data):
    total_distance = round(sum(data),3)
    return total_distance
# Get estimated time of the total route
def get_estimated_time(data):
    time = round(data/AVG_CAR_SPEED,3) # in seconds
    time_minutes = round(time/60,3) # in minutes
    return time_minutes

"""
After the driver has uploaded all the route information in the APP, we receive a GeoJSON file with the route. Then, we extract all
the information about the time and distances and send them back to the APP. 
Now, the user (pedestrian) is able to see a list of drivers when he set an arrival point for his route. 
The list is sorted by the estimated time of the route. The user can choose the driver that he wants.
------------------------------------
In this first part, we do not manage times, so we understand that they do not have conflicts. A driver upload a route and he 
wait until a pedestrian choose him.
"""

# Get the information about the routes of the cars
for file in get_geojson(ROOT+"car"): # Change to walker to get the walker route
    with beam.Pipeline(options=PipelineOptions()) as p:
        """
        Read the GeoJSON file and get the coordinates of the route. Then, transform the coordinates to meters and calculate the time of the route.
        Each data will be sent to BigQuery. With all the information, once there is a match, the route will start and we will manage the timing.
        """
        (
            p 
            | 'Create' >> beam.Create([file])
            | 'ReadGeoJSON' >> beam.Map(read_geojson)
            | 'GetCoords' >> beam.Map(get_coordinates)
            | 'GetDistances' >> beam.Map(get_coords_to_meters)
            | 'GetTotalDistance' >> beam.Map(get_total_distance)
            | 'GetEstimatedTime' >> beam.Map(get_estimated_time)
            | 'Print' >> beam.Map(print)
        )
