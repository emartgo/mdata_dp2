import os
import googlemaps

# Get files in a directory
def get_geojson(file_dir):
    geojson_file = []
    for file in os.listdir(file_dir):
        if file.endswith(".geojson"):
            dir = os.path.join(file_dir, file)
            geojson_file.append(dir)
    return geojson_file


def get_route_gmaps(origen, destino, clave_api):
    # Inicializa el cliente de Google Maps con tu clave de API
    gmaps = googlemaps.Client(key=clave_api)

    # Solicita las direcciones desde Google Maps
    direcciones = gmaps.directions(origen, destino)

    # Extrae las coordenadas de la ruta
    pasos = direcciones[0]['legs'][0]['steps']
    ruta = [(paso['start_location']['lat'], paso['start_location']['lng']) for paso in pasos]
    ruta.append((pasos[-1]['end_location']['lat'], pasos[-1]['end_location']['lng']))

    return ruta
