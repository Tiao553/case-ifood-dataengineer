import boto3
import os
from botocore.exceptions import ClientError
from utils.logger_config import setup_logger

logger = setup_logger()
s3 = boto3.client("s3")

def s3_file_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"S3 já contém: {key}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise

def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    try:
        s3.upload_file(local_path, bucket, key)
        logger.info(f"Upload para S3 concluído: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Erro ao fazer upload para S3: {e}")

def upload_log_to_s3(bucket: str, log_prefix: str, log_path: str) -> None:
    log_filename = os.path.basename(log_path)
    s3_key = f"{log_prefix}{log_filename}"
    upload_to_s3(log_path, bucket, s3_key)
