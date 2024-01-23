import folium

# Define las coordenadas de la ruta
ruta = [
    (39.4785137, -0.4078111),
    (39.4786614, -0.408136),
    (39.47890820000001, -0.4070553),
    (39.4746511, -0.406153),
    (39.4726538, -0.405628),
    (39.4707696, -0.4053922),
    (39.4700777, -0.4059443),
    (39.4637001, -0.405923),
    (39.4536192, -0.3997992),
    (39.4498062, -0.3950212),
    (39.44643730000001, -0.3866538),
    (39.4447042, -0.3726504),
    (39.45295249999999, -0.3524188),
    (39.4558459, -0.3478766),
    (39.4575031, -0.3454116),
    (39.4573528, -0.344843),
    (39.45637869999999, -0.3452043)
]

# Crea un mapa centrado en la primera coordenada
mapa = folium.Map(location=ruta[0], zoom_start=14)

# Añade las coordenadas a un objeto PolyLine y luego al mapa
folium.PolyLine(ruta, color="red", weight=2.5, opacity=1).add_to(mapa)

# Guarda el mapa en un archivo HTML
mapa.save("ruta_valencia.html")
