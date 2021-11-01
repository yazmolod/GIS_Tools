import logging

FORMATTER = logging.Formatter("[%(asctime)s][%(name)s] %(levelname)s - %(message)s")
HANDLER = logging.StreamHandler()
HANDLER.setLevel(logging.DEBUG)
HANDLER.setFormatter(FORMATTER)

def get_logger(name):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	logger.addHandler(HANDLER)
	return logger