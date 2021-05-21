import logging

logger = logging.getLogger('__main__')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(asctime)s][%(name)s] %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)