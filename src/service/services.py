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
