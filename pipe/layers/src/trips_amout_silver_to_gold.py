# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Importando módulos necessários
from transform.datetime_utils import add_ano_mes
from utils.logger import setup_logger
from utils.delta_optimization import optimize_and_vacuum_delta_table, coalesce_before_write

# Caminhos das tabelas Silver
paths = {
    "yellow": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/yellow/",
    "green": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/green/",
    "fhv": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/fhv/",
    "hvfhv": "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/hvfhv/"
}

# Caminhos das tabelas Gold e de agregações
GOLD_PATH = "s3a://case-ifood-de-prd-gold-zone-593793061865/nyc_taxi/trips_all/"
AGG_PATH = "s3a://case-ifood-de-prd-gold-zone-593793061865/nyc_taxi/agr_trips/"


def transform_yellow(df):
    """Transforma o DataFrame de corridas yellow para o formato Gold."""
    return df.selectExpr(
        "tpep_pickup_datetime as pickup_datetime",
        "tpep_dropoff_datetime as dropoff_datetime",
        "pulocationid as pulocationid",
        "dolocationid as dolocationid",
        "trip_distance", "cast(total_amount as double) as total_amount",
        "'yellow' as service_type",
        "'yellow' as provider"
    )

def transform_green(df):
    """Transforma o DataFrame de corridas green para o formato Gold."""
    return df.selectExpr(
        "lpep_pickup_datetime as pickup_datetime",
        "lpep_dropoff_datetime as dropoff_datetime",
        "pulocationid as pulocationid",
        "dolocationid as dolocationid",
        "trip_distance", "total_amount",
        "'green' as service_type",
        "'green' as provider"
    )

def transform_fhv(df):
    """Transforma o DataFrame de corridas FHV para o formato Gold."""
    return df.selectExpr(
        "pickup_datetime", 
        "dropOff_datetime as dropoff_datetime",
        "pulocationid as pulocationid",
        "dolocationid as dolocationid",
        "null as trip_distance", 
        "null as total_amount",
        "'fhv' as service_type",
        "'fhv' as provider"
    )

def transform_hvfhv(df):
    """Transforma o DataFrame de corridas HVFHV para o formato Gold."""
    df = df.withColumn(
        "provider",
        when(col("hvfhs_license_num") == "HV0002", "Juno")
        .when(col("hvfhs_license_num") == "HV0003", "Uber")
        .when(col("hvfhs_license_num") == "HV0004", "Via")
        .when(col("hvfhs_license_num") == "HV0005", "Lyft")
        .otherwise("Other")
    )
    return df.select(
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pulocationid").alias("pulocationid"),
        col("dolocationid").alias("dolocationid"),
        col("trip_miles").alias("trip_distance"),
        col("base_passenger_fare").alias("total_amount"),
        lit("hvfhv").alias("service_type"),
        col("provider")
    )

if __name__ == "__main__":
    logger = setup_logger("build_gold")
    logger.info("🚀 Iniciando construção da camada Gold NYC Taxi")

    try:
        spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("BuildGoldNYCTaxi")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        ).getOrCreate()

        df_yellow = transform_yellow(spark.read.format("delta").load(paths["yellow"]))
        df_green = transform_green(spark.read.format("delta").load(paths["green"]))
        df_fhv = transform_fhv(spark.read.format("delta").load(paths["fhv"]))
        df_hvfhv = transform_hvfhv(spark.read.format("delta").load(paths["hvfhv"]))

        df_union = ( 
            df_yellow
            .unionByName(df_green, allowMissingColumns=True)
            .unionByName(df_fhv, allowMissingColumns=True)
            .unionByName(df_hvfhv, allowMissingColumns=True)
        )

        df_union = add_ano_mes(df_union, base_col="pickup_datetime")

        df = coalesce_before_write(df_union,num_files=8)

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_PATH)
        )

        optimize_and_vacuum_delta_table(spark, GOLD_PATH, ["pickup_datetime","pulocationid","dolocationid","service_type"])

        logger.info("✅ Tabela trips union salva com sucesso.")
        logger.info("📊 Iniciando agregações analíticas")

        df_agg = (
            df_union
            .groupBy("ano_mes", "service_type", "provider","pulocationid","dolocationid") 
            .agg(
                F.count("*").alias("qtd_corridas"),
                F.countDistinct("pulocationid").alias("locais_embarque"),
                F.countDistinct("dolocationid").alias("locais_desembarque"),
                F.sum("trip_distance").alias("distancia_total"),
                F.sum("total_amount").alias("faturamento_total"),
                F.avg("trip_distance").alias("media_distancia"),
                F.avg("total_amount").alias("media_valor"),
                F.max("trip_distance").alias("max_distancia"),
                F.max("total_amount").alias("max_valor"),
                F.min("pickup_datetime").alias("min_data"),
                F.max("pickup_datetime").alias("max_data")
            )
        )

        (
            df_agg.write
            .format("delta")
            .mode("overwrite")
            .save(AGG_PATH)
        )

        optimize_and_vacuum_delta_table(spark, AGG_PATH, ["ano_mes", "service_type", "provider","pulocationid","dolocationid"])

        logger.info("✅ Agregações salvas com sucesso.")
        spark.stop()

    except Exception as e:
        logger.error(f"❌ Erro na execução da camada Gold: {str(e)}")
        raise
