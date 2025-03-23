import uuid
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

# Conexión a Elasticsearch
ELASTICSEARCH_URL = 'http://localhost:9200'
INDEX_NAME = 'stock_data'

# ✅ Leer CSV correctamente
df = pd.read_csv('stock_data.csv')

# ✅ Renombrar columnas a español (opcional para consistencia)
df.columns = ['Fecha', 'Precio Cierre',
              'Máximo', 'Mínimo', 'Apertura', 'Volumen']

# ✅ Limpieza de datos
df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
df['Precio Cierre'] = pd.to_numeric(
    df['Precio Cierre'], errors='coerce').fillna(0)
df['Máximo'] = pd.to_numeric(df['Máximo'], errors='coerce').fillna(0)
df['Mínimo'] = pd.to_numeric(df['Mínimo'], errors='coerce').fillna(0)
df['Apertura'] = pd.to_numeric(df['Apertura'], errors='coerce').fillna(0)
df['Volumen'] = pd.to_numeric(
    df['Volumen'], errors='coerce').fillna(0).astype(int)

# ✅ Crear índice con mapping en Elasticsearch
if not Elasticsearch(ELASTICSEARCH_URL).indices.exists(index=INDEX_NAME):
    mapping = {
        "mappings": {
            "properties": {
                "Fecha": {"type": "date"},
                "Precio Cierre": {"type": "float"},
                "Máximo": {"type": "float"},
                "Mínimo": {"type": "float"},
                "Apertura": {"type": "float"},
                "Volumen": {"type": "integer"}
            }
        }
    }
    Elasticsearch(ELASTICSEARCH_URL).indices.create(
        index=INDEX_NAME, body=mapping)
    print(f"✅ Índice '{INDEX_NAME}' creado correctamente")

# ✅ Generar documentos para Elasticsearch


def generate_docs(df):
    for _, row in df.iterrows():
        yield {
            "_index": INDEX_NAME,
            "_id": str(uuid.uuid4()),  # Genera un ID único para cada documento
            "_source": row.to_dict()
        }


# ✅ Insertar datos en lotes pequeños para evitar sobrecarga
for success, info in parallel_bulk(Elasticsearch(ELASTICSEARCH_URL), generate_docs(df), chunk_size=50):
    if not success:
        print(f"❌ Documento fallido: {info}")

print(f"✅ Datos cargados correctamente en el índice '{INDEX_NAME}'")
