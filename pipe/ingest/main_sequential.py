import os
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

def main():
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    urls = build_urls()

    for url in urls:
        filename = os.path.basename(url)
        local_path = os.path.join(LOCAL_TEMP_DIR, filename)

        taxi_type = filename.split('_')[0]
        s3_key = f"{S3_PREFIX}{taxi_type}/{filename}"

        if s3_file_exists(S3_BUCKET, s3_key):
            logger.info(f"Pulando arquivo já existente: {s3_key}")
            continue

        logger.info(f"Baixando: {url}")
        download_to_local(url, local_path)

        logger.info(f"Enviando para S3: {s3_key}")
        upload_to_s3(local_path, S3_BUCKET, s3_key)

        os.remove(local_path)

    # Enviar log ao final
    logger.info("Fazendo upload do log para S3...")
    upload_log_to_s3(S3_BUCKET, S3_LOG_PREFIX, logger.log_path)

if __name__ == "__main__":
    main()
