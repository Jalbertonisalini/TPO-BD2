# src/service/services.py

from src.logger import getLogger
from pymongo.database import Database
from redis import Redis

log = getLogger(__name__)

class ServicioAseguradora:
    def __init__(self, db_mongo: Database, r_redis: Redis):
        self.db = db_mongo
        self.r = r_redis
        log.info("ServicioAseguradora inicializado.")

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
    