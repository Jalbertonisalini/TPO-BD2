# main.py

import sys
import os
from pymongo import MongoClient
import redis
from pprint import pprint

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

log = getLogger("QUERY_RUNNER")

# --- Conexión Global ---
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

# --- Wizards interactivos para Q14 y Q15 (Q13 ya no lo usa) ---

def ejecutar_demo_siniestro(servicio: ServicioAseguradora):
    """Lanza un mini-wizard interactivo para el alta de Siniestros."""
    log.info("--- (S14: Alta Siniestro Interactivo) ---")
    print("\n--- Módulo de Alta de Siniestros ---")
    
    try:
        log.info("Modo: ALTA de Siniestro")
        print("--- Registrando nuevo siniestro ---")
        
        id_siniestro = int(input("  ID Siniestro (ej. 901): ").strip())
        nro_poliza = input("  Nro. de Póliza asociada (ej. POL1001): ").strip().upper()
        fecha = input("  Fecha (DD/MM/YYYY): ").strip()
        tipo = input("  Tipo (ej. Accidente, Robo): ").strip()
        monto_estimado = float(input("  Monto estimado (ej. 150000): ").strip())
        descripcion = input("  Descripción: ").strip()
        estado = input("  Estado (Abierto / Cerrado / En Evaluacion): ").strip()
        
        datos_nuevos = {
            "id_siniestro": id_siniestro,
            "nro_poliza": nro_poliza,
            "fecha": fecha,
            "tipo": tipo,
            "monto_estimado": monto_estimado,
            "descripcion": descripcion,
            "estado": estado
        }
        
        resultado = servicio.q14_alta_siniestro(datos_nuevos)
        pprint(resultado)

    except ValueError:
        log.error("Error: ID/Monto deben ser numéricos.")
        print("Error: El ID Siniestro y el Monto deben ser numéricos.")
    except Exception as e:
        log.error(f"Error inesperado en el wizard de Siniestros: {e}")
        pprint(f"Ocurrió un error: {e}")

def ejecutar_demo_poliza(servicio: ServicioAseguradora):
    """Lanza un mini-wizard interactivo para la Emisión de Pólizas."""
    log.info("--- (S15: Emisión de Póliza Interactivo) ---")
    print("\n--- Módulo de Emisión de Pólizas ---")
    
    try:
        log.info("Modo: ALTA de Póliza")
        print("--- Registrando nueva póliza ---")

        nro_poliza = input("  Nro. de Póliza (ej. POL901): ").strip().upper()
        id_cliente = int(input("  ID Cliente asociado: ").strip())
        id_agente = int(input("  ID Agente asociado: ").strip())
        tipo = input("  Tipo (ej. Auto, Vida): ").strip()
        fecha_inicio = input("  Fecha de Inicio (DD/MM/YYYY): ").strip()
        fecha_fin = input("  Fecha de Fin (DD/MM/YYYY): ").strip()
        prima_mensual = float(input("  Prima Mensual: ").strip())
        cobertura_total = float(input("  Cobertura Total: ").strip())
        estado = input("  Estado (ej. activa, suspendida): ").strip()

        datos_nuevos = {
            "nro_poliza": nro_poliza,
            "id_cliente": id_cliente,
            "tipo": tipo,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "prima_mensual": prima_mensual,
            "cobertura_total": cobertura_total,
            "id_agente": id_agente,
            "estado": estado
        }
        
        resultado = servicio.q15_emitir_poliza(datos_nuevos)
        pprint(resultado)

    except ValueError:
        log.error("Error: IDs/Montos deben ser numéricos.")
        print("Error: IDs y Montos deben ser numéricos.")
    except Exception as e:
        log.error(f"Error inesperado en el wizard de Pólizas: {e}")
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
        log.info("--- (S13: ABM Clientes por CLI) ---")
        try:
            accion = sys.argv[2].lower()

            if accion == 'alta':
                if len(sys.argv) < 12:
                    log.error("Error ALTA: Faltan argumentos.")
                    print("Uso: python main.py 13 alta <id_cliente> <nombre> <apellido> <dni> <email> <tel> <dir> <ciudad> <prov>")
                    sys.exit(1)
                
                datos_nuevos = {
                    "id_cliente": int(sys.argv[3]),
                    "nombre": sys.argv[4],
                    "apellido": sys.argv[5],
                    "dni": sys.argv[6],
                    "email": sys.argv[7],
                    "telefono": sys.argv[8],
                    "direccion": sys.argv[9],
                    "ciudad": sys.argv[10],
                    "provincia": sys.argv[11],
                    "activo": True,
                    "vehiculos": []
                }
                pprint(servicio.q13_abm_clientes(accion='alta', datos=datos_nuevos))

            elif accion == 'modificar':
                if len(sys.argv) < 6:
                    log.error("Error MODIFICAR: Faltan argumentos.")
                    print("Uso: python main.py 13 modificar <id_cliente> <campo> <nuevo_valor>")
                    sys.exit(1)
                
                cliente_id = int(sys.argv[3])
                campo = sys.argv[4]
                valor = sys.argv[5]
                datos_modificados = { campo: valor }
                pprint(servicio.q13_abm_clientes(accion='modificar', cliente_id=cliente_id, datos=datos_modificados))
            
            elif accion == 'baja':
                if len(sys.argv) < 4:
                    log.error("Error BAJA: Faltan argumentos.")
                    print("Uso: python main.py 13 baja <id_cliente>")
                    sys.exit(1)

                cliente_id = int(sys.argv[3])
                pprint(servicio.q13_abm_clientes(accion='baja', cliente_id=cliente_id))

            else:
                log.error(f"Acción '{accion}' no reconocida. Debe ser 'alta', 'modificar' o 'baja'.")

        except IndexError:
            log.error("Error de ABM: Faltan argumentos de línea de comando.")
            print("Error: Faltan argumentos. Verifique el uso:")
            print("  python main.py 13 alta <id> <nombre> ... (9 args + 13 + alta = 11 total)")
            print("  python main.py 13 modificar <id> <campo> <valor>")
            print("  python main.py 13 baja <id>")
        except ValueError:
            log.error("Error: El id_cliente (argumento 3) debe ser un número.")
        except Exception as e:
            log.error(f"Error inesperado en ABM: {e}")

    elif query_num == '14':
        ejecutar_demo_siniestro(servicio)
        
    elif query_num == '15':
        ejecutar_demo_poliza(servicio)
    
    else:
        log.error(f"Número de query '{query_num}' no válido. Debe ser de 1 a 15.")

    log.info(f"--- Fin de Query/Servicio N° {query_num} ---")
    
    mongo_client.close()
    redis_client.close()
    log.info("Conexiones a BBDD cerradas.")