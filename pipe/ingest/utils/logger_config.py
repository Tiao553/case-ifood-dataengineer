import logging
import os
from config.settings import LOG_FILE, LOCAL_LOG_DIR

def setup_logger() -> logging.Logger:
    os.makedirs(LOCAL_LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOCAL_LOG_DIR, LOG_FILE)

    logger = logging.getLogger("TLCDownloader")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(fh)
        logger.addHandler(sh)

    logger.log_path = log_path
    return logger
