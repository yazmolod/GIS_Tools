import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(asctime)s][%(name)s] %(levelname)s - %(message)s")

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

threads_logger = logging.getLogger('ThreadsUtils')
threads_logger.addHandler(handler)
threads_logger.setLevel(logging.INFO)