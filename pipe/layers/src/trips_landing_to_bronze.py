from pyspark.sql import SparkSession
from utils.ingestor import ingest_dataset
from utils.logger import setup_logger

# Importa os schemas explícitos
from schema.yellow import yellow_schema
from schema.green import green_schema
from schema.fhv import fhv_schema
from schema.hvfhv import hvfhv_schema

logger = setup_logger()

# SparkSession com suporte a Delta
spark = SparkSession.builder \
    .appName("RawToBronzeIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuração dos datasets
datasets = {
    "yellow": {
        "bucket": "case-ifood-de-prd-landing-zone-593793061865",
        "prefix": "nyc_taxi/yellow/",
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/",
        "schema": yellow_schema
    },
    "green": {
        "bucket": "case-ifood-de-prd-landing-zone-593793061865",
        "prefix": "nyc_taxi/green/",
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/",
        "schema": green_schema
    },
    "fhv": {
        "bucket": "case-ifood-de-prd-landing-zone-593793061865",
        "prefix": "nyc_taxi/fhv/",
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/",
        "schema": fhv_schema
    },
    "hvfhv": {
        "bucket": "case-ifood-de-prd-landing-zone-593793061865",
        "prefix": "nyc_taxi/hvfhv/",
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/",
        "schema": hvfhv_schema
    }
}

# Execução
if __name__ == "__main__":
    for name, config in datasets.items():
        ingest_dataset(name, config, spark, logger)

    logger.info("✅ Ingestão finalizada.")
