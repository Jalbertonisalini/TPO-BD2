# main.py

import sys
import os
from pymongo import MongoClient
import redis
from pprint import pprint

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.logger import getLogger
from src.service.services import ServicioAseguradora

MONGO_HOST = "mongo"
REDIS_HOST = "redis"
DB_NAME = "aseguradora_db"

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

def ejecutar_demo_abm(servicio: ServicioAseguradora):
    """Lanza un mini-wizard interactivo para probar el ABM de Clientes."""
    log.info("--- (S13: ABM Clientes Interactivo) ---")
    print("\n--- Módulo ABM de Clientes ---")
    
    accion = input("¿Qué acción desea realizar? (alta / modificar / baja): ").strip().lower()

    try:
        if accion == 'alta':
            log.info("Modo: ALTA de Cliente")
            print("--- Creando nuevo cliente (Formulario Completo) ---")
            
            id_cliente = int(input("  ID Cliente (ej. 999): ").strip())
            nombre = input("  Nombre: ").strip()
            apellido = input("  Apellido: ").strip()
            dni = input("  DNI: ").strip()
            email = input("  Email: ").strip()
            telefono = input("  Teléfono: ").strip()
            direccion = input("  Dirección: ").strip()
            ciudad = input("  Ciudad: ").strip()
            provincia = input("  Provincia: ").strip()
            
            datos_nuevos = {
                "id_cliente": id_cliente,
                "nombre": nombre,
                "apellido": apellido,
                "dni": dni,
                "email": email,
                "telefono": telefono,
                "direccion": direccion,
                "ciudad": ciudad,
                "provincia": provincia,
                "activo": True,
                "vehiculos": []
            }
            
            resultado = servicio.q13_abm_clientes(accion='alta', datos=datos_nuevos)
            pprint(resultado)

        elif accion == 'modificar':
            log.info("Modo: MODIFICAR Cliente")
            print("--- Modificando cliente existente ---")
            
            cliente_id = int(input("  ID del Cliente a modificar: ").strip())
            campo = input("  Nombre del campo a modificar (ej. telefono, email, ciudad): ").strip().lower()
            valor = input(f"  Nuevo valor para '{campo}': ").strip()
            
            datos_modificados = { campo: valor }
            
            resultado = servicio.q13_abm_clientes(accion='modificar', cliente_id=cliente_id, datos=datos_modificados)
            pprint(resultado)

        elif accion == 'baja':
            log.info("Modo: BAJA (lógica) de Cliente")
            print("--- Dando de baja a un cliente ---")
            
            cliente_id = int(input("  ID del Cliente a dar de baja (lógica): ").strip())
            
            resultado = servicio.q13_abm_clientes(accion='baja', cliente_id=cliente_id)
            pprint(resultado)
            
        else:
            log.warning(f"Acción '{accion}' no reconocida.")
            print(f"Error: Acción no válida. Debe ser 'alta', 'modificar' o 'baja'.")

    except ValueError:
        log.error("Error: El ID del cliente debe ser un número.")
        print("Error: El ID del cliente debe ser un número.")
    except Exception as e:
        log.error(f"Error inesperado en el wizard ABM: {e}")
        pprint(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    
    servicio = ServicioAseguradora(db, redis_client)
    
    if len(sys.argv) < 2:
        log.error("¡Error! Debes especificar un número de query para correr.")
        log.error("Usa el menú 'Run and Debug' de VS Code para elegir una query.")
        sys.exit(1)
        
    query_num = sys.argv[1]
    
    log.info(f"--- Ejecutando Query/Servicio N° {query_num} ---")
        
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
        ejecutar_demo_abm(servicio)
    else:
        log.error(f"Número de query '{query_num}' no válido. Debe ser de 1 a 15.")

    log.info(f"--- Fin de Query/Servicio N° {query_num} ---")
    
    mongo_client.close()
    redis_client.close()
    log.info("Conexiones a BBDD cerradas.")