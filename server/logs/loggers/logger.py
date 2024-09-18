import logging
import logging.config

logging.config.fileConfig(fname='logs/config.ini', disable_existing_loggers=False)


def logger_config(module):
    '''
    loggers:
    
    @parameter debug
    @parameter info
    @parameter warning
    @parameter error
    @parameter critical
    @parameter exception
    @returns logger
    '''
    logger = logging.getLogger(module)
    return logger