# src/loader/load_data.py

from src.logger import getLogger
import pandas as pd
from pymongo import MongoClient
import redis
from datetime import datetime
import time

log = getLogger(__name__)

MONGO_HOST = "mongo"
REDIS_HOST = "redis"
DB_NAME = "aseguradora_db"

CSV_BASE_PATH = "csv/"

log.info("Iniciando script de carga de datos...")

try:
    mongo_client = MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()
    db = mongo_client[DB_NAME]
    log.info("Conexión a MongoDB exitosa.")

    redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    redis_client.ping()
    log.info("Conexión a Redis exitosa.")

except Exception as e:
    log.error(f"Error al conectar con las bases de datos: {e}")
    exit()

log.info("Limpiando colecciones y claves existentes para una carga limpia...")
try:
    for collection_name in db.list_collection_names():
        db[collection_name].drop()
    log.info("Colecciones de MongoDB limpiadas.")

    redis_client.flushdb()
    log.info("Claves de Redis limpiadas.")

except Exception as e:
    log.error(f"Error durante la limpieza de bases de datos: {e}")

try:
    # Leemos todos los datasets con Pandas
    df_clientes = pd.read_csv(CSV_BASE_PATH + 'clientes.csv')
    df_vehiculos = pd.read_csv(CSV_BASE_PATH + 'vehiculos.csv')
    df_agentes = pd.read_csv(CSV_BASE_PATH + 'agentes.csv')
    df_polizas = pd.read_csv(CSV_BASE_PATH + 'polizas.csv')
    df_siniestros = pd.read_csv(CSV_BASE_PATH + 'siniestros.csv')
    log.info("Archivos CSV leídos correctamente.")

    log.info("Procesando y cargando clientes con sus vehículos...")
    clientes_collection = db.clientes
    for index, cliente in df_clientes.iterrows():
        cliente_doc = cliente.to_dict()
        
        vehiculos_cliente = df_vehiculos[df_vehiculos['id_cliente'] == cliente['id_cliente']]
        
        cliente_doc['vehiculos'] = vehiculos_cliente.drop(columns=['id_cliente']).to_dict('records')
        
        clientes_collection.insert_one(cliente_doc)
    log.info(f"-> {clientes_collection.count_documents({})} clientes cargados en MongoDB.")

    log.info("Procesando y cargando agentes...")
    agentes_collection = db.agentes
    agentes_collection.insert_many(df_agentes.to_dict('records'))
    log.info(f"-> {agentes_collection.count_documents({})} agentes cargados en MongoDB.")

    log.info("Procesando y cargando siniestros...")
    siniestros_collection = db.siniestros
    siniestros_collection.insert_many(df_siniestros.to_dict('records'))
    log.info(f"-> {siniestros_collection.count_documents({})} siniestros cargados en MongoDB.")

    log.info("Procesando y cargando pólizas (lógica políglota)...")
    polizas_collection = db.polizas
    for index, poliza in df_polizas.iterrows():
        poliza_doc = poliza.to_dict()
        
        polizas_collection.insert_one(poliza_doc)
        
        try:
            id_agente_key = str(int(poliza['id_agente']))
            redis_client.hincrby('agente:stats', id_agente_key, 1)
            
            id_cliente_key = str(int(poliza['id_cliente']))
            redis_client.zincrby('ranking:clientes:cobertura', 
                                  poliza['cobertura_total'], 
                                  id_cliente_key)

        except ValueError:
            log.warning(f"ID de agente/cliente no numérico en póliza {poliza['nro_poliza']}. Saltando.")

        if poliza['estado'].lower() == 'activa': 
            try:
                fecha_inicio_dt = datetime.strptime(poliza['fecha_inicio'], '%d/%m/%Y') 
                timestamp = int(time.mktime(fecha_inicio_dt.timetuple()))
                
                redis_client.zadd('idx:polizas:activas', {str(poliza['nro_poliza']): timestamp})
            
            except ValueError as e:
                log.warning(f"Fecha en formato incorrecto para póliza {poliza['nro_poliza']}: {e}")
            except Exception as e:
                log.error(f"Error procesando timestamp para póliza {poliza['nro_poliza']}: {e}")

    log.info(f"-> {polizas_collection.count_documents({})} pólizas cargadas en MongoDB.")
    log.info("-> Contadores y rankings de Redis actualizados (corregido).")
    log.info("¡Carga de datos completada con éxito!")

except FileNotFoundError as e:
    log.error(f"No se encontró el archivo {e.filename}. Asegúrate de que la carpeta 'csv' está en la raíz.")
except KeyError as e:
    log.error(f"Error de columna no encontrada: {e}. Revisa los nombres de las columnas en tus CSVs.")
except Exception as e:
    log.error(f"Ocurrió un error inesperado durante la carga de datos: {e}")

finally:
    mongo_client.close()
    log.info("Conexión a MongoDB cerrada.")