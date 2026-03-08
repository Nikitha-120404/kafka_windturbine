import logging
import os

from src.utils.config import PIPELINE


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, PIPELINE.log_level.upper(), logging.INFO))
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_dir = os.path.dirname(PIPELINE.log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    file_handler = logging.FileHandler(PIPELINE.log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
