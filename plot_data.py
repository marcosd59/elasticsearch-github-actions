import pandas as pd
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch

ELASTICSEARCH_URL = 'http://localhost:9200'
INDEX_NAME = 'stock_data'

# ✅ Conexión a Elasticsearch
es = Elasticsearch(ELASTICSEARCH_URL)

# ✅ Consulta para obtener los datos
query = {
    "size": 1000,
    "query": {
        "match_all": {}
    }
}

# ✅ Obtener los datos desde Elasticsearch
response = es.search(index=INDEX_NAME, body=query)
data = [doc["_source"] for doc in response['hits']['hits']]
df = pd.DataFrame(data)

# ✅ Asegurar que la columna 'Fecha' sea tipo fecha para ordenar correctamente
df['Fecha'] = pd.to_datetime(df['Fecha'])

# ✅ Ordenar por fecha
df = df.sort_values(by='Fecha')

# ✅ Graficar datos financieros
plt.figure(figsize=(12, 8))

plt.plot(df['Fecha'], df['Precio Cierre'],
         label='Precio Cierre', linestyle='-', marker='o')
plt.plot(df['Fecha'], df['Apertura'],
         label='Precio Apertura', linestyle='--', marker='x')
plt.plot(df['Fecha'], df['Máximo'], label='Máximo', linestyle='-.', marker='^')
plt.plot(df['Fecha'], df['Mínimo'], label='Mínimo', linestyle=':', marker='v')

# ✅ Añadir etiquetas y título
plt.title('Histórico de precios de acciones (AAPL)')
plt.xlabel('Fecha')
plt.ylabel('Precio (USD)')
plt.legend()

# ✅ Añadir cuadrícula para mayor claridad
plt.grid(True)

# ✅ Guardar el gráfico como archivo PNG
plt.savefig('plot.png')

print("✅ Gráfico generado correctamente como 'plot.png'")
