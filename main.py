# main.py

import sys
import os
from pymongo import MongoClient
import redis
from pprint import pprint # Para imprimir diccionarios y listas de forma legible

# --- Hack de Ruta para Imports ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
# ---------------------------------

from src.logger import getLogger
from src.service.services import ServicioAseguradora

# --- Configuración de Conexión ---
MONGO_HOST = "mongo"
REDIS_HOST = "redis"
DB_NAME = "aseguradora_db"

# --- Conexión Global ---
log = getLogger("QUERY_RUNNER")

try:
    log.info("Conectando a bases de datos...")
    mongo_client = MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()
    db = mongo_client[DB_NAME]
    
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    redis_client.ping()
    log.info("Conexiones exitosas.")
    
except Exception as e:
    log.error(f"FATAL: No se pudo conectar a las bases de datos: {e}")
    sys.exit(1)


# --- PUNTO DE ENTRADA PRINCIPAL ---
if __name__ == "__main__":
    
    servicio = ServicioAseguradora(db, redis_client)
    
    # Verificamos si se pasó un argumento
    if len(sys.argv) < 2:
        log.error("¡Error! Debes especificar un número de query para correr.")
        log.error("Usa el menú 'Run and Debug' de VS Code para elegir una query.")
        sys.exit(1)
        
    # El primer argumento (sys.argv[0]) es el nombre del script,
    # el que nos interesa es el segundo (sys.argv[1])
    query_num = sys.argv[1]
    
    log.info(f"--- Ejecutando Query/Servicio N° {query_num} ---")
    
    # --- Mapeo de argumentos a funciones ---
    
    if query_num == '1':
        pprint(servicio.q1_clientes_activos_con_polizas())
    
    elif query_num == '2':
        pprint(servicio.q2_siniestros_abiertos_con_cliente())
    
    elif query_num == '3':
        pprint(servicio.q3_vehiculos_asegurados_con_cliente_poliza())
        
    elif query_num == '4':
        pprint(servicio.q4_clientes_sin_polizas_activas())
        
    elif query_num == '5':
        pprint(servicio.q5_agentes_activos_con_polizas())
        
    elif query_num == '6':
        pprint(servicio.q6_polizas_vencidas_con_cliente())
        
    elif query_num == '7':
        pprint(servicio.q7_top_10_clientes_cobertura())
        
    elif query_num == '8':
        pprint(servicio.q8_siniestros_accidente_ultimo_anio())
        
    elif query_num == '9':
        pprint(servicio.q9_vista_polizas_activas_ordenadas())
        
    elif query_num == '10':
        pprint(servicio.q10_polizas_suspendidas_estado_cliente())
        
    elif query_num == '11':
        pprint(servicio.q11_clientes_con_mas_de_un_vehiculo())
        
    elif query_num == '12':
        pprint(servicio.q12_agentes_y_siniestros_asociados())
        
    elif query_num == '13':
        log.info("--- (ABM Clientes): Dando de ALTA a cliente 999 ---")
        pprint(servicio.q13_abm_clientes('alta', datos={
            "id_cliente": 999, "nombre": "Cliente", "apellido": "Prueba ABM", "dni": 123456,
            "email": "prueba@demo.com", "telefono": "555-1234", "activo": True, "vehiculos": []
        }))
        log.info("--- (ABM Clientes): MODIFICANDO teléfono de cliente 999 ---")
        pprint(servicio.q13_abm_clientes('modificar', cliente_id=999, datos={'telefono': '555-9876'}))
        log.info("--- (ABM Clientes): Dando de BAJA (lógica) a cliente 999 ---")
        pprint(servicio.q13_abm_clientes('baja', cliente_id=999))
        
    elif query_num == '14':
        log.info("--- (Alta Siniestro) ---")
        pprint(servicio.q14_alta_siniestro({
            "id_siniestro": 901, "nro_poliza": 1001, "fecha": "2025-10-31", "tipo": "Robo",
            "monto_estimado": 7500, "descripcion": "Robo en vía pública (demo)", "estado": "abierto"
        }))
        
    elif query_num == '15':
        log.info("--- (Emisión de Póliza) ---")
        pprint(servicio.q15_emitir_poliza({
            "nro_poliza": 901, "id_cliente": 1, "tipo": "Total", "fecha_inicio": "2025-11-01",
            "fecha_fin": "2026-11-01", "prima_mensual": 300, "cobertura_total": 60000,
            "id_agente": 1, "estado": "activa"
        }))
    
    else:
        log.error(f"Número de query '{query_num}' no válido. Debe ser de 1 a 15.")

    log.info(f"--- Fin de Query/Servicio N° {query_num} ---")
    
    # --- Cierre de Conexiones ---
    mongo_client.close()
    redis_client.close()
    log.info("Conexiones a BBDD cerradas.")