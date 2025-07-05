import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import (
    S3_BUCKET,
    S3_PREFIX,
    S3_LOG_PREFIX,
    LOCAL_TEMP_DIR
)
from utils.logger_config import setup_logger
from utils.url_builder import build_urls
from downloader.parquet_downloader import download_to_local
from s3_utils.s3_client import s3_file_exists, upload_to_s3, upload_log_to_s3

logger = setup_logger()


def process_file(url: str):
    filename = os.path.basename(url)
    local_path = os.path.join(LOCAL_TEMP_DIR, filename)

    taxi_type = filename.split('_')[0]
    s3_key = f"{S3_PREFIX}{taxi_type}/{filename}"

    if s3_file_exists(S3_BUCKET, s3_key):
        logger.info(f"Pulando (já existe no S3): {s3_key}")
        return

    try:
        logger.info(f"Iniciando download: {url}")
        download_to_local(url, local_path)

        logger.info(f"Enviando para S3: {s3_key}")
        upload_to_s3(local_path, S3_BUCKET, s3_key)

    except Exception as e:
        logger.error(f"Erro no processamento de {url}: {e}")

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

if __name__ == "__main__":
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    urls = build_urls()

    max_threads = 8 

    logger.info(f"Iniciando processamento com {max_threads} threads...")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(process_file, url) for url in urls]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Erro em thread: {e}")

    logger.info("Upload do log para S3...")
    upload_log_to_s3(S3_BUCKET, S3_LOG_PREFIX, logger.log_path)

    logger.info("Processamento finalizado com sucesso.")
