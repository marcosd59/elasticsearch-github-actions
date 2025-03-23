import uuid
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
import time

# ✅ Parámetros de conexión
ELASTICSEARCH_URL = 'http://localhost:9200'
INDEX_NAME = 'stock_data'

# ✅ Leer CSV
print("📥 Reading CSV file...")
df = pd.read_csv('stock_data.csv')

# ✅ Renombrar columnas (opcional)
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

# ✅ Intentar conectar a Elasticsearch con reintentos
RETRIES = 5
connected = False

for i in range(RETRIES):
    try:
        print(
            f"🚀 Attempting to connect to Elasticsearch (try {i + 1}/{RETRIES})...")
        es = Elasticsearch(ELASTICSEARCH_URL)
        if es.ping():
            connected = True
            print("✅ Connected to Elasticsearch!")
            break
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        time.sleep(5)

if not connected:
    raise ConnectionError(
        "❌ Could not connect to Elasticsearch after multiple attempts")

# ✅ Crear índice si no existe
if not es.indices.exists(index=INDEX_NAME):
    print(f"📢 Creating index '{INDEX_NAME}'...")
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
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"✅ Index '{INDEX_NAME}' created successfully")

# ✅ Generar documentos para Elasticsearch


def generate_docs(df):
    for _, row in df.iterrows():
        yield {
            "_index": INDEX_NAME,
            # Generar un ID único para cada documento
            "_id": str(uuid.uuid4()),
            "_source": row.to_dict()
        }


# ✅ Insertar datos en Elasticsearch en lotes
print("🚀 Inserting data into Elasticsearch...")
for success, info in parallel_bulk(es, generate_docs(df), chunk_size=50):
    if not success:
        print(f"❌ Failed to insert document: {info}")

print(f"✅ Data loaded successfully into '{INDEX_NAME}'!")
