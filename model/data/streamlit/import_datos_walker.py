from google.cloud import bigquery

# Задайте свои данные для авторизации в BigQuery
project_id = 'awesome-ridge-411708'
dataset_id = 'datasetedem'
table_id = 'route walker'  # Замените на имя вашей таблицы

# Инициализация клиента BigQuery
client = bigquery.Client(project=project_id)

# Пример данных для внесения в таблицу
data_to_insert = [
    {"walker_id": 2, "lat": 39.46828, "lon": -0.37386},
    {"walker_id": 2, "lat": 39.46822, "lon": -0.37386},
    {"walker_id": 2, "lat": 39.46827, "lon": -0.37473},
    {"walker_id": 2, "lat": 39.46829, "lon": -0.37526},
    {"walker_id": 2, "lat": 39.46828, "lon": -0.37549},
    {"walker_id": 2, "lat": 39.46828, "lon": -0.37572},
    {"walker_id": 2, "lat": 39.46828, "lon": -0.37585},
    {"walker_id": 2, "lat": 39.46829, "lon": -0.37606},
    {"walker_id": 2, "lat": 39.46831, "lon": -0.37628},
    {"walker_id": 2, "lat": 39.46832, "lon": -0.37648},
    {"walker_id": 2, "lat": 39.46832, "lon": -0.3765},
    {"walker_id": 2, "lat": 39.46833, "lon": -0.37672},
    {"walker_id": 2, "lat": 39.46836, "lon": -0.37695},
    {"walker_id": 2, "lat": 39.46836, "lon": -0.37709},
    {"walker_id": 2, "lat": 39.4684, "lon": -0.3773},
    {"walker_id": 2, "lat": 39.46849, "lon": -0.37777},
    {"walker_id": 2, "lat": 39.46864, "lon": -0.37852},
    {"walker_id": 2, "lat": 39.4688, "lon": -0.37942},
    {"walker_id": 2, "lat": 39.46884, "lon": -0.37947},
    {"walker_id": 2, "lat": 39.46886, "lon": -0.3795},
    {"walker_id": 2, "lat": 39.46889, "lon": -0.37953},
    {"walker_id": 2, "lat": 39.46901, "lon": -0.3796},
    {"walker_id": 2, "lat": 39.46893, "lon": -0.37962},
    {"walker_id": 2, "lat": 39.4689, "lon": -0.37964},
    {"walker_id": 2, "lat": 39.46885, "lon": -0.37966},
    {"walker_id": 2, "lat": 39.46896, "lon": -0.37989},
    {"walker_id": 2, "lat": 39.46913, "lon": -0.38023},
    {"walker_id": 2, "lat": 39.46934, "lon": -0.38069},
    {"walker_id": 2, "lat": 39.46953, "lon": -0.38105},
    {"walker_id": 2, "lat": 39.46958, "lon": -0.38119},
    {"walker_id": 2, "lat": 39.46972, "lon": -0.38151},
    {"walker_id": 2, "lat": 39.4698, "lon": -0.3818},
    {"walker_id": 2, "lat": 39.46987, "lon": -0.38206},
    {"walker_id": 2, "lat": 39.47, "lon": -0.38237},
    {"walker_id": 2, "lat": 39.47, "lon": -0.38244},
    {"walker_id": 2, "lat": 39.47001, "lon": -0.3825},
    {"walker_id": 2, "lat": 39.47015, "lon": -0.38268},
    {"walker_id": 2, "lat": 39.47016, "lon": -0.38271},
    {"walker_id": 2, "lat": 39.47019, "lon": -0.38273},
    {"walker_id": 2, "lat": 39.47022, "lon": -0.38278},
    {"walker_id": 2, "lat": 39.47003, "lon": -0.383},
    {"walker_id": 2, "lat": 39.47033, "lon": -0.38339},
    {"walker_id": 2, "lat": 39.47034, "lon": -0.38341},
    {"walker_id": 2, "lat": 39.47036, "lon": -0.38343},
    {"walker_id": 2, "lat": 39.47035, "lon": -0.38443},
    {"walker_id": 2, "lat": 39.47034, "lon": -0.38463},
    {"walker_id": 2, "lat": 39.47033, "lon": -0.38476},
    {"walker_id": 2, "lat": 39.47032, "lon": -0.38484},
    {"walker_id": 2, "lat": 39.47032, "lon": -0.38505},
    {"walker_id": 2, "lat": 39.47031, "lon": -0.38524},
    {"walker_id": 2, "lat": 39.47037, "lon": -0.38528},
    {"walker_id": 2, "lat": 39.47039, "lon": -0.38529},
    {"walker_id": 2, "lat": 39.4705, "lon": -0.38537},
    {"walker_id": 2, "lat": 39.47174, "lon": -0.38626},
    {"walker_id": 2, "lat": 39.47187, "lon": -0.38637},
    {"walker_id": 2, "lat": 39.47213, "lon": -0.38654},
    {"walker_id": 2, "lat": 39.47226, "lon": -0.38663},
    {"walker_id": 2, "lat": 39.47242, "lon": -0.38675},
    {"walker_id": 2, "lat": 39.47336, "lon": -0.38742},
    {"walker_id": 2, "lat": 39.47348, "lon": -0.3875},
    {"walker_id": 2, "lat": 39.4738, "lon": -0.38772},
    {"walker_id": 2, "lat": 39.47396, "lon": -0.38783},
    {"walker_id": 2, "lat": 39.47411, "lon": -0.38794},
    {"walker_id": 2, "lat": 39.47425, "lon": -0.38804},
    {"walker_id": 2, "lat": 39.4743, "lon": -0.38807},
    {"walker_id": 2, "lat": 39.47434, "lon": -0.3881},
    {"walker_id": 2, "lat": 39.47446, "lon": -0.38818},
    {"walker_id": 2, "lat": 39.47455, "lon": -0.38825},
    {"walker_id": 2, "lat": 39.47452, "lon": -0.3884},
    {"walker_id": 2, "lat": 39.47448, "lon": -0.38864},
    {"walker_id": 2, "lat": 39.47426, "lon": -0.38999},
    {"walker_id": 2, "lat": 39.47424, "lon": -0.39011},
    {"walker_id": 2, "lat": 39.4741, "lon": -0.39098},
    {"walker_id": 2, "lat": 39.47408, "lon": -0.3911},
    {"walker_id": 2, "lat": 39.47404, "lon": -0.39135},
    {"walker_id": 2, "lat": 39.47396, "lon": -0.39161},
    {"walker_id": 2, "lat": 39.47415, "lon": -0.39173},
]


# Определение схемы таблицы
schema = [
    bigquery.SchemaField("walker_id", "INTEGER"),
    bigquery.SchemaField("lat", "FLOAT64"),
    bigquery.SchemaField("lon", "FLOAT64"),
]

# Определение таблицы
table_ref = client.dataset(dataset_id).table(table_id)
table = bigquery.Table(table_ref, schema=schema)

# Создание таблицы, если она не существует
try:
    table = client.create_table(table)  # Создаем таблицу, если её нет
    print(f"Создана таблица {table_id}")
except Exception as e:
    print(f"Таблица {table_id} уже существует")

# Внесение данных в таблицу
errors = client.insert_rows(table, data_to_insert)

if errors == []:
    print("Данные успешно внесены в таблицу.")
else:
    print(f"Произошла ошибка при внесении данных: {errors}")
