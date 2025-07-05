from pyspark.sql import DataFrame, SparkSession, functions as F
from delta.tables import DeltaTable

def coalesce_before_write(df: DataFrame, num_files: int = 1) -> DataFrame:
    """
    Reduz o número de partições físicas antes da escrita para evitar pequenos arquivos.
    """
    return df.coalesce(num_files)


def optimize_and_vacuum_delta_table(spark: SparkSession, delta_path: str, zorder_cols: list[str], retention_hours: int = 168):
    """
    Executa comandos de otimização Delta Lake: OPTIMIZE + ZORDER + VACUUM.

    Exemplo:
        zorder_cols = ["id_cliente", "data_inicio"]
    """
    z_cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY ({z_cols})")
    spark.sql(f"VACUUM delta.`{delta_path}` RETAIN {retention_hours} HOURS")


def analyze_delta_table(spark: SparkSession, delta_path: str):
    """
    Coleta estatísticas da tabela Delta para melhorar o plano de execução.
    """
    spark.sql(f"ANALYZE TABLE delta.`{delta_path}` COMPUTE STATISTICS")


def optimize_for_read_performance(spark: SparkSession, delta_path: str, zorder_cols: list[str]):
    """
    Executa ZORDER, VACUUM e ANALYZE na tabela Delta para performance ideal.
    """
    optimize_and_vacuum_delta_table(spark, delta_path, zorder_cols)
    analyze_delta_table(spark, delta_path)