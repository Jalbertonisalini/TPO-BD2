# src/logger.py

import logging
import sys

def getLogger(name):
    FORMAT = '[%(asctime)s] - %(name)s - %(levelname)s - %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    # Configura el logger
    logging.basicConfig(
        level=logging.INFO,
        format=FORMAT,
        datefmt=DATE_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(name)
    return logger