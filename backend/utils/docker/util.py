import os
from logs.loggers.logger import logger_config


logger = logger_config(__name__)

def is_docker():
    path = '/proc/self/cgroup'
    res = False
    if os.path.isfile(path):
        with open(path, 'r') as f:
            res = any('docker' in line for line in f)
            logger.info(f"Is docker: {res}")
    logger.info(f"Is docker: {res}")
    return os.path.exists('/.dockerenv') or res