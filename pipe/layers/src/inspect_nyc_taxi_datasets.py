from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col

# Importa os schemas explícitos
from schema.yellow import yellow_schema
from schema.green import green_schema
from schema.fhv import fhv_schema
from schema.hvfhv import hvfhv_schema

# Caminhos com seus schemas respectivos
sources = {
    "yellow": {
        "path": "s3a://case-ifood-de-prd-landing-zone-593793061865/nyc_taxi/yellow/",
        "schema": yellow_schema
    },
    "green": {
        "path": "s3a://case-ifood-de-prd-landing-zone-593793061865/nyc_taxi/green/",
        "schema": green_schema
    },
    "fhv": {
        "path": "s3a://case-ifood-de-prd-landing-zone-593793061865/nyc_taxi/fhv/",
        "schema": fhv_schema
    },
    "hvfhv": {
        "path": "s3a://case-ifood-de-prd-landing-zone-593793061865/nyc_taxi/hvfhv/",
        "schema": hvfhv_schema
    }
}

spark = SparkSession.builder \
    .appName("NYCTaxiInspect") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
    .getOrCreate()

for name, source in sources.items():
    path = source["path"]
    schema = source["schema"]

    print(f"\n\n===== [ {name.upper()} ] =====\n")

    try:
        # Etapa 1 - Ler tudo como string para evitar conflito com Parquet (INT32 vs INT64)
        df_raw = spark.read.parquet(path)

        # Etapa 2 - Converte todas as colunas para string primeiro
        for colname in df_raw.columns:
            df_raw = df_raw.withColumn(colname, col(colname).cast("string"))

        # Etapa 3 - Agora aplicamos o cast com segurança
        df_casted = df_raw
        for field in schema.fields:
            df_casted = df_casted.withColumn(field.name, col(field.name).cast(field.dataType))


        # Print do schema ajustado
        print("\n--- Schema ---")
        df_casted.printSchema()

        # Primeiro registro
        print("\n--- First Record ---")
        df_casted.show(1, vertical=True)

        # Estatísticas descritivas
        print("\n--- Describe ---")
        df_casted.describe().show(truncate=False)

    except AnalysisException as ae:
        print(f"[ERRO DE ANÁLISE] {name}: {ae}")
    except Exception as e:
        print(f"[ERRO GERAL] {name}: {e}")
