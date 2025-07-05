# importando bibliotecas necessárias
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, upper

# Importando módulos necessários
from utils.logger import setup_logger
from utils.schema_control import apply_schema_with_cast
from transform.cleaner import standard_cleaning_pipeline
from transform.datetime_utils import add_ingestion_timestamp, add_ano_mes
from transform.deduplicator import deduplicate
from utils.delta_optimization import optimize_for_read_performance, coalesce_before_write
from transform.merge import merge_delta

# Importando schemas específicos
from schema.yellow import yellow_schema
from schema.green import green_schema
from schema.fhv import fhv_schema
from schema.hvfhv import hvfhv_schema

# ============================
# 1. CONFIGURAÇÃO INICIAL
# ============================

datasets = {
    "yellow": {
        "schema": yellow_schema,
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/yellow/",
        "silver_path": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/yellow/",
        "zorder_cols": ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"],
        "dedup_keys": ["vendorid", "tpep_pickup_datetime"],
        "pickup_col": "tpep_pickup_datetime"
    },
    "green": {
        "schema": green_schema,
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/green/",
        "silver_path": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/green/",
        "zorder_cols": ["lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID"],
        "dedup_keys": ["vendorid", "lpep_pickup_datetime"],
        "pickup_col": "lpep_pickup_datetime"
    },
    "fhv": {
        "schema": fhv_schema,
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/fhv/",
        "silver_path": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/fhv/",
        "zorder_cols": ["pickup_datetime", "PULocationID", "DOLocationID"],
        "dedup_keys": ["dispatching_base_num", "pickup_datetime"],
        "pickup_col": "pickup_datetime"
    },
    "hvfhv": {
        "schema": hvfhv_schema,
        "bronze_path": "s3a://case-ifood-de-prd-bronze-zone-593793061865/nyc_taxi/hvfhv/",
        "silver_path": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/hvfhv/",
        "zorder_cols": ["pickup_datetime", "PULocationID", "DOLocationID"],
        "dedup_keys": ["hvfhs_license_num", "pickup_datetime"],
        "pickup_col": "pickup_datetime"
    },
}

# ============================
# 2. PROCESSAMENTO
# ============================

def process_dataset(spark, dataset_name, config, logger):
    logger.info(f"\n🚕 Iniciando processamento do dataset: {dataset_name.upper()}")

    # Leitura do bronze
    df = spark.read.format("delta").load(config["bronze_path"])
    logger.info(f"📥 Leitura do bronze concluída: {config['bronze_path']}")

    # Validação e coerção de tipos
    df = apply_schema_with_cast(df, config["schema"], logger)

    # Renomeia colunas para minúsculo
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # Aplica lower em todas as colunas string
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, upper(col(field.name)))

    # Limpeza padronizada
    df = standard_cleaning_pipeline(df)

    # Adiciona colunas de auditoria temporal
    df = add_ingestion_timestamp(df)
    df = add_ano_mes(df, base_col=config["pickup_col"])

    # Remoção de duplicações
    df = deduplicate(df, partition_cols=config["dedup_keys"], order_col=config["pickup_col"])

    df = coalesce_before_write(df)

    # Escrita como merge no silver
    merge_delta(
        spark=spark,
        df_final=df,
        delta_path=config["silver_path"],
        business_key=config["dedup_keys"]
    )
    logger.info(f"💾 Merge concluído no silver: {config['silver_path']}")

    # Otimização Delta Lake (Z-ORDER + Vacuum + Statistics)
    optimize_for_read_performance(spark, config["silver_path"], config["zorder_cols"])
    logger.info(f"✅ Otimização finalizada para: {dataset_name.upper()}")


# ============================
# 3. EXECUÇÃO PRINCIPAL
# ============================

if __name__ == "__main__":
    builder = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger = setup_logger(name="bronze_to_silver", log_file="logs/bronze_to_silver.log")

    for dataset_name, config in datasets.items():
        try:
            process_dataset(spark, dataset_name, config, logger)
        except Exception as e:
            logger.error(f"❌ Erro no processamento de {dataset_name.upper()}: {str(e)}")
            
    spark.stop()