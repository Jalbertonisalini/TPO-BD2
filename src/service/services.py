# src/service/services.py

from src.logger import getLogger
from pymongo.database import Database
from redis import Redis
from datetime import datetime, timedelta 
import time

log = getLogger(__name__)

class ServicioAseguradora:
    def __init__(self, db_mongo: Database, r_redis: Redis):
        self.db = db_mongo
        self.r = r_redis
    # --- CONSULTAS ---

    def q1_clientes_activos_con_polizas(self):
        """1. Clientes activos con sus pólizas vigentes"""
        log.info("EJECUTANDO Q1 (Mongo): Clientes activos con pólizas vigentes")
        pipeline = [
            { '$match': { 'activo': True } },
            {
                '$lookup': {
                    'from': 'polizas',
                    'localField': 'id_cliente',
                    'foreignField': 'id_cliente',
                    'pipeline': [
                        { '$match': { 'estado': { '$regex': '^activa$', '$options': 'i' } } }
                    ],
                    'as': 'polizas_vigentes'
                }
            },
            { '$match': { 'polizas_vigentes': { '$ne': [] } } },
            {
                '$project': {
                    'nombre_completo': { '$concat': ['$nombre', ' ', '$apellido'] },
                    'polizas_vigentes': '$polizas_vigentes.nro_poliza',
                    '_id': 0
                }
            }
        ]
        return list(self.db.clientes.aggregate(pipeline))
    
    def q2_siniestros_abiertos_con_cliente(self):
        log.info("EJECUTANDO Q2 (Mongo): Siniestros abiertos con cliente")
        pipeline = [
            {
                '$match': {
                    'estado': { '$regex': '^abierto$', '$options': 'i' }
                }
            },
            {
                '$lookup': {
                    'from': 'polizas',
                    'localField': 'nro_poliza',
                    'foreignField': 'nro_poliza',
                    'as': 'poliza_info'
                }
            },
            {
                '$unwind': '$poliza_info'
            },
            {
                '$lookup': {
                    'from': 'clientes',
                    'localField': 'poliza_info.id_cliente',
                    'foreignField': 'id_cliente',
                    'as': 'cliente_info'
                }
            },
            {
                '$unwind': '$cliente_info'
            },
            {
                '$project': {
                    '_id': 0,
                    'id_siniestro': '$id_siniestro',
                    'tipo_siniestro': '$tipo',
                    'monto_estimado': '$monto_estimado',
                    'estado_siniestro': '$estado',
                    'cliente_afectado': { 
                        '$concat': ['$cliente_info.nombre', ' ', '$cliente_info.apellido'] 
                    }
                }
            }
        ]
        
        return list(self.db.siniestros.aggregate(pipeline))
    
    def q3_vehiculos_asegurados_con_cliente_poliza(self):
        log.info("EJECUTANDO Q3 (Mongo): Vehículos asegurados con cliente y póliza")
        pipeline = [
            {
                '$unwind': '$vehiculos'
            },
            {
                '$match': {
                    'vehiculos.asegurado': True
                }
            },
            {
                '$lookup': {
                    'from': 'polizas',
                    'localField': 'id_cliente',
                    'foreignField': 'id_cliente',
                    'pipeline': [
                        { '$match': { 
                            'tipo': { '$regex': '^auto$', '$options': 'i' }
                          } 
                        }
                    ],
                    'as': 'polizas_info'
                }
            },
            {
                '$match': {
                    'polizas_info': { '$ne': [] }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'cliente': { '$concat': ['$nombre', ' ', '$apellido'] },
                    'vehiculo': {
                        'marca': '$vehiculos.marca',
                        'modelo': '$vehiculos.modelo',
                        'patente': '$vehiculos.patente'
                    },
                    'polizas_del_cliente': '$polizas_info.nro_poliza'
                }
            }
        ]
        
        return list(self.db.clientes.aggregate(pipeline))
    
    def q4_clientes_sin_polizas_activas(self):
        log.info("EJECUTANDO Q4 (Mongo): Clientes sin pólizas activas")
        
        pipeline = [
            {
                '$lookup': {
                    'from': 'polizas',
                    'localField': 'id_cliente',
                    'foreignField': 'id_cliente',
                    'pipeline': [
                        { '$match': { 
                            'estado': { '$regex': '^activa$', '$options': 'i' }
                          } 
                        }
                    ],
                    'as': 'polizas_activas'
                }
            },
            {
                '$match': {
                    'polizas_activas': { '$size': 0 } 
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'id_cliente': '$id_cliente',
                    'dni': '$dni',
                    'telefono': '$telefono',
                    'nombre': '$nombre',
                    'apellido': '$apellido',
                    'email': '$email',
                    'direccion': '$direccion',
                    'ciudad': '$ciudad',
                    'provincia': '$provincia',
                    'telefono': '$telefono',
                    'activo': '$activo'
                }
            }
        ]
        
        return list(self.db.clientes.aggregate(pipeline))
    
    def q5_agentes_activos_con_polizas(self):
        log.info("EJECUTANDO Q5 (Mongo + Redis): Agentes activos y conteo de pólizas")
        
        pipeline_mongo = [
            {
                '$match': { 'activo': True }
            },
            {
                '$project': {
                    '_id': 0,
                    'id_agente': '$id_agente',
                    'nombre_completo': { '$concat': ['$nombre', ' ', '$apellido'] },
                    'matricula': '$matricula'
                }
            }
        ]
        agentes_activos = list(self.db.agentes.aggregate(pipeline_mongo))
        
        try:
            conteo_polizas_hash = self.r.hgetall('agente:stats')
        except Exception as e:
            log.error(f"Error al obtener 'agente:stats' de Redis: {e}")
            return [] 

        resultado_final = []
        for agente in agentes_activos:
            id_agente_str = str(agente['id_agente'])
            conteo_str = conteo_polizas_hash.get(id_agente_str, '0')
            agente['cantidad_polizas'] = int(conteo_str) 
            resultado_final.append(agente)
            
        return resultado_final
    
    def q6_polizas_vencidas_con_cliente(self):
        log.info("EJECUTANDO Q6 (Mongo): Pólizas vencidas con cliente")
        
        pipeline = [
            {
                '$match': {
                    'estado': { '$regex': '^vencida$', '$options': 'i' }
                }
            },
            {
                '$lookup': {
                    'from': 'clientes',
                    'localField': 'id_cliente',
                    'foreignField': 'id_cliente',
                    'as': 'cliente_info'
                }
            },
            {
                '$unwind': '$cliente_info'
            },
            {
                '$project': {
                    '_id': 0,
                    'nro_poliza': '$nro_poliza',
                    'estado_poliza': '$estado',
                    'fecha_fin': '$fecha_fin',
                    'cliente': { 
                        '$concat': ['$cliente_info.nombre', ' ', '$cliente_info.apellido'] 
                    }
                }
            }
        ]
        
        return list(self.db.polizas.aggregate(pipeline))
    
    
    def q7_top_10_clientes_cobertura(self):
        log.info("EJECUTANDO Q7 (Redis + Mongo): Top 10 clientes por cobertura")
        
        try:
            top_10_raw = self.r.zrevrange(
                'ranking:clientes:cobertura', 
                0, 
                9,
                withscores=True 
            )
        except Exception as e:
            log.error(f"Error al consultar ranking en Redis: {e}")
            return []

        if not top_10_raw:
            log.warning("No se encontraron datos en el ranking 'ranking:clientes:cobertura' de Redis.")
            return []

    
        top_10_ids_str = [id_cliente for id_cliente, score in top_10_raw]
        try:
            top_10_ids_int = [int(id_str) for id_str in top_10_ids_str]
        except ValueError as e:
            log.error(f"Error convirtiendo IDs de cliente de Redis a int: {e}. IDs: {top_10_ids_str}")
            return [] 

        pipeline_mongo = [
            {
                '$match': { 'id_cliente': { '$in': top_10_ids_int } }
            },
            {
                '$project': {
                    '_id': 0,
                    'id_cliente': '$id_cliente',
                    'nombre_completo': { '$concat': ['$nombre', ' ', '$apellido'] }
                }
            }
        ]
        clientes_info_list = list(self.db.clientes.aggregate(pipeline_mongo))
        
        clientes_lookup = {
            cliente['id_cliente']: cliente['nombre_completo'] 
            for cliente in clientes_info_list
        }

        resultado_final = []
        for id_cliente_str, score in top_10_raw:
            id_cliente_int = int(id_cliente_str)
            
            nombre_cliente = clientes_lookup.get(id_cliente_int, "Nombre No Encontrado")
            
            resultado_final.append({
                'id_cliente': id_cliente_int,
                'nombre_cliente': nombre_cliente,
                'cobertura_total_acumulada': float(score)
            })
            
        return resultado_final
    
    def q8_siniestros_accidente_ultimo_anio(self):
        log.info("EJECUTANDO Q8 (Mongo PURO): Siniestros 'Accidente' último año")
        
        pipeline = [
            {
                '$match': {
                    'tipo': { '$regex': '^accidente$', '$options': 'i' }
                }
            },
            {
                '$addFields': {
                    'fecha_dt': {
                        '$dateFromString': {
                            'dateString': '$fecha',
                            'format': '%d/%m/%Y', 
                            'onError': None 
                        }
                    }
                }
            },
            {
                '$match': {
                    '$expr': {
                        '$let': {
                            'vars': {
                                'hoy': '$$NOW',
                                'hace_un_anio': {
                                    '$dateSubtract': {
                                        'startDate': '$$NOW',
                                        'unit': 'day',
                                        'amount': 365
                                    }
                                }
                            },
                            'in': {
                                '$and': [
                                    { '$ne': ['$fecha_dt', None] }, 
                                    { '$gte': ['$fecha_dt', '$$hace_un_anio'] },
                                    { '$lte': ['$fecha_dt', '$$hoy'] }
                                ]
                            }
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'id_siniestro': '$id_siniestro',
                    'tipo': '$tipo',
                    'fecha_siniestro': '$fecha', 
                    'monto_estimado': '$monto_estimado',
                    'estado': '$estado'
                }
            }
        ]
        
        return list(self.db.siniestros.aggregate(pipeline))

    def q9_vista_polizas_activas_ordenadas(self):
        log.info("EJECUTANDO Q9 (Redis + Mongo): Pólizas activas ordenadas")
        
        try:
            poliza_numeros_ordenados = self.r.zrange('idx:polizas:activas', 0, -1)
            
            if not poliza_numeros_ordenados:
                log.warning("No se encontraron pólizas en 'idx:polizas:activas' de Redis.")
                return []
                
        except Exception as e:
            log.error(f"Error al consultar índice 'idx:polizas:activas' en Redis: {e}")
            return []

        pipeline_mongo = [
            {
                '$match': { 
                    'nro_poliza': { '$in': poliza_numeros_ordenados } 
                }
            },
            {
                '$addFields': {
                    'sort_order': {
                        '$indexOfArray': [poliza_numeros_ordenados, '$nro_poliza']
                    }
                }
            },
            {
                '$sort': { 'sort_order': 1 }
            },
            {
                '$project': {
                    '_id': 0,
                    'nro_poliza': 1,
                    'tipo': 1,
                    'fecha_inicio': 1,
                    'fecha_fin': 1,
                    'cobertura_total': 1,
                    'estado': 1
                }
            }
        ]
        
        return list(self.db.polizas.aggregate(pipeline_mongo))
    
    def q10_polizas_suspendidas_estado_cliente(self):
        log.info("EJECUTANDO Q10 (Mongo): Pólizas suspendidas y estado del cliente")
        
        pipeline = [
            {
                '$match': {
                    'estado': { '$regex': '^suspendida$', '$options': 'i' }
                }
            },
            {
                '$lookup': {
                    'from': 'clientes',
                    'localField': 'id_cliente',
                    'foreignField': 'id_cliente',
                    'as': 'cliente_info'
                }
            },
            {
                '$unwind': '$cliente_info'
            },
            {
                '$project': {
                    '_id': 0,
                    'nro_poliza': '$nro_poliza',
                    'estado_poliza': '$estado',
                    'cliente': { 
                        '$concat': ['$cliente_info.nombre', ' ', '$cliente_info.apellido'] 
                    },
                    'estado_cliente_activo': '$cliente_info.activo'
                }
            }
        ]
        
        return list(self.db.polizas.aggregate(pipeline))

    def q11_clientes_con_mas_de_un_vehiculo(self):
        log.info("EJECUTANDO Q11 (Mongo): Clientes con más de un vehículo asegurado")
        
        pipeline = [
            {
                '$match': {
                    '$expr': {
                        '$gt': [
                            {
                                '$size': {
                                    '$filter': {
                                        'input': '$vehiculos', 
                                        'as': 'v', 
                                        'cond': { '$eq': ['$$v.asegurado', True] } 
                                    }
                                }
                            },
                            1 
                        ]
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'id_cliente': '$id_cliente',
                    'nombre_completo': { '$concat': ['$nombre', ' ', '$apellido'] },
                    'vehiculos_asegurados': {
                        '$filter': {
                            'input': '$vehiculos',
                            'as': 'v',
                            'cond': { '$eq': ['$$v.asegurado', True] }
                        }
                    }
                }
            }
        ]
        
        return list(self.db.clientes.aggregate(pipeline))
    
    def q12_agentes_y_siniestros_asociados(self):
        log.info("EJECUTANDO Q12 (Mongo): Conteo de siniestros por agente")
        
        pipeline = [
            {
                '$lookup': {
                    'from': 'polizas',
                    'localField': 'nro_poliza',
                    'foreignField': 'nro_poliza',
                    'as': 'poliza_info'
                }
            },
            {
                '$unwind': '$poliza_info'
            },
            {
                '$group': {
                    '_id': '$poliza_info.id_agente', 
                    'cantidad_siniestros': { '$sum': 1 } 
                }
            },
            {
                '$lookup': {
                    'from': 'agentes',
                    'localField': '_id',
                    'foreignField': 'id_agente',
                    'as': 'agente_info'
                }
            },
            {
                '$unwind': '$agente_info'
            },
            {
                '$project': {
                    '_id': 0,
                    'id_agente': '$agente_info.id_agente',
                    'nombre_agente': { 
                        '$concat': ['$agente_info.nombre', ' ', '$agente_info.apellido'] 
                    },
                    'matricula': '$agente_info.matricula',
                    'cantidad_siniestros': '$cantidad_siniestros'
                }
            },
            {
                '$sort': {
                    'cantidad_siniestros': -1 
                }
            }
        ]
        
        return list(self.db.siniestros.aggregate(pipeline))
    
    def q13_abm_clientes(self, accion, datos=None, cliente_id=None):
            log.info(f"EJECUTANDO S13 (Mongo): ABM Cliente - {accion}")
            
            try:
                # --- ALTA (Crear) ---
                if accion == 'alta' and datos:
                    if self.db.clientes.find_one({'id_cliente': datos['id_cliente']}):
                        log.warning(f"ABM Alta: id_cliente {datos['id_cliente']} ya existe.")
                        return f"Error: id_cliente {datos['id_cliente']} ya existe."
                    
                    if 'activo' not in datos:
                        datos['activo'] = True
                    if 'vehiculos' not in datos:
                        datos['vehiculos'] = []

                    result = self.db.clientes.insert_one(datos)
                    return f"Cliente creado con ID de Mongo: {result.inserted_id}"

                # --- MODIFICACIÓN (Actualizar) ---
                elif accion == 'modificar' and cliente_id and datos:
                    result = self.db.clientes.update_one(
                        { 'id_cliente': cliente_id },
                        { '$set': datos }
                    )
                    if result.matched_count == 0:
                        log.warning(f"ABM Modificar: No se encontró cliente con ID {cliente_id}.")
                        return f"Error: No se encontró el cliente con ID {cliente_id} para modificar."
                    
                    return f"Cliente ID {cliente_id} modificado. Documentos afectados: {result.modified_count}"

                # --- BAJA (Borrado Lógico) ---
                elif accion == 'baja' and cliente_id:
                    result = self.db.clientes.update_one(
                        { 'id_cliente': cliente_id },
                        { '$set': { 'activo': False } }
                    )
                    if result.matched_count == 0:
                        log.warning(f"ABM Baja: No se encontró cliente con ID {cliente_id}.")
                        return f"Error: No se encontró el cliente con ID {cliente_id} para dar de baja."
                    
                    return f"Cliente ID {cliente_id} dado de baja (lógica). Documentos afectados: {result.modified_count}"

                # --- Error de input ---
                else:
                    log.error(f"ABM Acción '{accion}' no válida o faltan datos.")
                    return "Acción ABM no válida o faltan datos (se requiere accion, cliente_id y/o datos)."

            except Exception as e:
                log.error(f"Error inesperado en ABM Clientes: {e}")
                return f"Error inesperado en ABM Clientes: {e}"
        
    def q14_alta_siniestro(self, datos_siniestro):
        log.info("EJECUTANDO S14 (Mongo): Alta Siniestro")
            
        try:
            poliza = self.db.polizas.find_one({'nro_poliza': datos_siniestro['nro_poliza']})
            if not poliza:
                log.warning(f"Alta Siniestro: Nro de póliza {datos_siniestro['nro_poliza']} no existe.")
                return "Error: La póliza asociada no existe."

            if self.db.siniestros.find_one({'id_siniestro': datos_siniestro['id_siniestro']}):
                log.warning(f"Alta Siniestro: id_siniestro {datos_siniestro['id_siniestro']} ya existe.")
                return "Error: id_siniestro ya existe."

            ESTADOS_VALIDOS = ['Abierto', 'Cerrado', 'En Evaluacion']
            estado_normalizado = datos_siniestro['estado'].strip().title()
            if estado_normalizado not in ESTADOS_VALIDOS:
                log.warning(f"Alta Siniestro: Estado '{datos_siniestro['estado']}' no es válido.")
                return f"Error: Estado no válido. Debe ser uno de: {ESTADOS_VALIDOS}"
            datos_siniestro['estado'] = estado_normalizado
            
            try:
                datetime.strptime(datos_siniestro['fecha'], '%d/%m/%Y')
            except ValueError:
                log.error(f"Alta Siniestro: Formato de fecha incorrecto. Use DD/MM/YYYY.")
                return "Error: Formato de fecha incorrecto. Use DD/MM/YYYY."
                
            result = self.db.siniestros.insert_one(datos_siniestro)
            return f"Siniestro creado con ID de Mongo: {result.inserted_id}"
        
        except Exception as e:
            log.error(f"Error en Alta Siniestro: {e}")
            return f"Error en Alta Siniestro: {e}"
        
    def q15_emitir_poliza(self, datos_poliza):
            log.info("EJECUTANDO S15 (Mongo + Redis): Emisión de Póliza")
            
            try:
                cliente = self.db.clientes.find_one({ 'id_cliente': datos_poliza['id_cliente'] })
                agente = self.db.agentes.find_one({ 'id_agente': datos_poliza['id_agente'] })
                
                if not cliente or not agente:
                    return "Error: Cliente o Agente no existen."
                if not cliente['activo'] or not agente['activo']:
                    return "Error: Cliente o Agente no están activos."
                
                if self.db.polizas.find_one({'nro_poliza': datos_poliza['nro_poliza']}):
                    return "Error: nro_poliza ya existe."

                try:
                    fecha_inicio_dt = datetime.strptime(datos_poliza['fecha_inicio'], '%d/%m/%Y')
                    datetime.strptime(datos_poliza['fecha_fin'], '%d/%m/%Y')
                except ValueError:
                    log.error(f"Emisión Póliza: Formato de fecha incorrecto (ej: {datos_poliza['fecha_inicio']}). Use DD/MM/YYYY.")
                    return "Error: Formato de fecha incorrecto. Use DD/MM/YYYY."

                ESTADOS_VALIDOS = ['Activa', 'Vencida', 'Suspendida']
                
                estado_normalizado = datos_poliza['estado'].strip().title()
                
                if estado_normalizado not in ESTADOS_VALIDOS:
                    log.warning(f"Emisión Póliza: Estado '{datos_poliza['estado']}' no es válido.")
                    return f"Error: Estado no válido. Debe ser uno de: {ESTADOS_VALIDOS}"
                
                datos_poliza['estado'] = estado_normalizado

                result = self.db.polizas.insert_one(datos_poliza)
                poliza_id_mongo = result.inserted_id
                log.info(f"Póliza {datos_poliza['nro_poliza']} insertada en MongoDB.")
                
                try:
                    self.r.hincrby('agente:stats', str(datos_poliza['id_agente']), 1)
                    
                    self.r.zincrby('ranking:clientes:cobertura', 
                                datos_poliza['cobertura_total'], 
                                str(datos_poliza['id_cliente']))
                    
                    if estado_normalizado.lower() == 'activa':
                        timestamp = int(time.mktime(fecha_inicio_dt.timetuple()))
                        self.r.zadd('idx:polizas:activas', {str(datos_poliza['nro_poliza']): timestamp})
                    
                    log.info(f"Póliza {datos_poliza['nro_poliza']} actualizada en vistas de Redis.")
                    return f"Póliza emitida. Mongo ID: {poliza_id_mongo}. Vistas de Redis actualizadas."
                    
                except Exception as e_redis:
                    log.error(f"Error CRÍTICO actualizando Redis. Póliza {poliza_id_mongo} insertada en Mongo pero Redis falló: {e_redis}")
                    return f"Error CRÍTICO: Póliza insertada en Mongo ({poliza_id_mongo}) pero falló la actualización en Redis."
            
            except Exception as e:
                log.error(f"Error en Emisión de Póliza: {e}")
                return f"Error en Emisión de Póliza: {e}"