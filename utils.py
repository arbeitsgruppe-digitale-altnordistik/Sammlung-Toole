import logging


def get_logger(name: str) -> logging.Logger:
    """
    returns a preconfigured logger
    """
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('warnings.log', mode='w')
    f_handler2 = logging.FileHandler('log.log', mode='w')
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.WARNING)
    f_handler2.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format2 = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    f_handler2.setFormatter(f_format2)
    log.addHandler(c_handler)
    log.addHandler(f_handler)
    log.addHandler(f_handler2)
    return log
