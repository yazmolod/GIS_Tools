import logging

FORMATTER = logging.Formatter("[%(asctime)s][%(name)s] %(levelname)s - %(message)s")
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setLevel(logging.DEBUG)
STREAM_HANDLER.setFormatter(FORMATTER)


def getLogger(name, stream=True, file=None, filemode="a+"):
    """быстро включает логгер, когда лень настраивать нормальный"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if stream:
        logger.addHandler(STREAM_HANDLER)
    if file:
        FILE_HANDLER = logging.FileHandler(f"{file}", filemode, encoding="utf-8")
        FILE_HANDLER.setLevel(logging.INFO)
        FILE_HANDLER.setFormatter(FORMATTER)
        logger.addHandler(FILE_HANDLER)
    return logger
