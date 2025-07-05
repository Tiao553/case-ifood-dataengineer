import boto3
import os
from pyspark.sql.utils import AnalysisException
from utils.schema_control import apply_schema_with_cast

def get_processed_files(s3_client, bucket, prefix, logger):
    key = prefix + "_processed_files.txt"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return set(content.strip().split('\n')) if content else set()
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"Arquivo de controle não encontrado para {prefix}. Criando novo.")
        return set()

def append_to_processed(s3_client, bucket, prefix, filename, logger):
    key = prefix + "_processed_files.txt"

    try:
        # Tenta recuperar o conteúdo atual do arquivo
        response = s3_client.get_object(Bucket=bucket, Key=key)
        existing_content = response['Body'].read().decode("utf-8")
        processed = set(line.strip() for line in existing_content.splitlines() if line.strip())
    except s3_client.exceptions.NoSuchKey:
        # Arquivo ainda não existe
        processed = set()

    if filename not in processed:
        processed.add(filename)
        content = "\n".join(sorted(processed)) + "\n"  # Garante quebra de linha final
        s3_client.put_object(Bucket=bucket, Key=key, Body=content)
        logger.info(f"✅ Arquivo adicionado ao controle: {filename}")
    else:
        logger.info(f"🔁 Arquivo já estava registrado: {filename}")


def ingest_dataset(name, config, spark, logger):
    bucket = config["bucket"]
    prefix = config["prefix"]
    bronze_path = config["bronze_path"]
    schema = config.get("schema", None)

    logger.info(f"📥 Iniciando ingestão de: {name.upper()}")
    s3 = boto3.client("s3")
    processed_files = get_processed_files(s3, bucket, prefix, logger)

    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                
                if not key.endswith(".parquet") or key in processed_files or key.endswith("2023-01.parquet"):
                    logger.info(f"⏩ Ignorando: {key}")
                    continue

                file_path = f"s3a://{bucket}/{key}"
                logger.info(f"  -> Lendo: {file_path}")

                df = spark.read.parquet(file_path)
                output_path = os.path.join(bronze_path, name)

                if schema:
                    logger.info(f"  -> Aplicando schema para: {name}")
                    #df = spark.createDataFrame(df.rdd, schema=schema)
                    df = apply_schema_with_cast(df, schema, logger)

                df.write.format("delta").mode("append").save(output_path)
                logger.info(f"  ✅ Escrito em: {output_path}")

                append_to_processed(s3, bucket, prefix, key, logger)

    except AnalysisException as ae:
        logger.error(f"[ANALYSIS ERROR] {name}: {ae}")
    except Exception as e:
        logger.exception(f"[UNEXPECTED ERROR] {name}")
