import csv
from pymongo import MongoClient

# Conexión a la base de datos MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['tennis_db']
collection = db['tournaments']

# Leer el archivo CSV y guardar los datos en la base de datos
with open('atp_tennis.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        collection.insert_one(row)

# Consulta en MongoDB para obtener los campeonatos por país
pipeline = [
    {'$group': {'_id': '$Winner', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}}
]

results = collection.aggregate(pipeline)

# Mostrar los resultados por consola
print('Campeonatos por país:')
for result in results:
    print(f'{result["_id"]}: {result["count"]}')
