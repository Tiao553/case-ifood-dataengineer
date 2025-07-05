# src/validate_gold_analytics.py

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, hour, month, avg

OUTPUT_PATH = "/app/validation_results.txt"
AGG_PATH = "s3a://case-ifood-de-prd-gold-zone-593793061865/nyc_taxi/agr_trips/"
YELLOW_PATH = "s3a://case-ifood-de-prd-silver-zone-593793061865/nyc_taxi/yellow/"

if __name__ == "__main__":
    spark = configure_spark_with_delta_pip(
        SparkSession.builder
        .appName("ValidateGoldAnalytics")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ).getOrCreate()

    with open(OUTPUT_PATH, "w") as output:
        output.write("VALIDAÇÃO DAS MÉTRICAS ANALÍTICAS\n")
        output.write("===============================\n\n")

        # (2) Média de valor total recebido em um mês pelos yellow táxis
        df_agg = spark.read.format("delta").load(AGG_PATH)
        yellow_avg = df_agg.filter(col("service_type") == "yellow") \
            .select(avg("faturamento_total").alias("media_total_amount")) \
            .first()["media_total_amount"]

        print("[2] Média de total_amount dos Yellow Taxis:", yellow_avg)
        output.write(f"[2] Média de total_amount dos Yellow Taxis: {yellow_avg}\n")

        # (3) Média de passageiros por hora do dia para mês de maio
        df_yellow = spark.read.format("delta").load(YELLOW_PATH)

        df_maio = df_yellow.filter(month("tpep_pickup_datetime") == 5)

        if "passenger_count" in df_maio.columns:
            df_passageiros = df_maio.withColumn("hora", hour("tpep_pickup_datetime")) \
                .groupBy("hora").agg(avg("passenger_count").alias("media_passageiros")) \
                .orderBy("hora")

            df_passageiros.show(truncate=False)
            output.write("\n[3] Média de passageiros por hora do dia (maio):\n")
            for row in df_passageiros.collect():
                output.write(f"Hora {row['hora']}: média = {row['media_passageiros']:.2f}\n")
        else:
            print("Coluna passenger_count não disponível no dataset yellow.")
            output.write("[3] Coluna passenger_count não disponível no dataset yellow.\n")

    spark.stop()
