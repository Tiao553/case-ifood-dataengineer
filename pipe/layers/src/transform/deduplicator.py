from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def deduplicate(df: DataFrame, partition_cols: list[str], order_col: str) -> DataFrame:
    """
    Remove duplicações no DataFrame, mantendo apenas o registro mais recente por grupo,
    com base em uma coluna de ordenação descendente.

    :param df: DataFrame de entrada.
    :param partition_cols: Lista de colunas usadas para identificar duplicações (chave natural).
    :param order_col: Coluna usada para ordenar os registros (ex.: data de atualização).
    :return: DataFrame deduplicado, contendo apenas o primeiro registro por grupo.
    :raises AnalysisException: se qualquer coluna especificada não existir no DataFrame.
    """
    # Validação das colunas
    missing_cols = [col for col in partition_cols + [order_col] if col not in df.columns]
    if missing_cols:
        raise AnalysisException(f"Colunas não encontradas no DataFrame: {missing_cols}")

    # Especificação da janela
    window_spec = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())

    # Aplicação da lógica de deduplicação
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1) \
             .drop("rn")


def drop_exact_duplicates(df: DataFrame, subset: list[str] = None) -> DataFrame:
    """
    Remove duplicações exatas com base em todas as colunas ou em um subconjunto especificado.

    :param df: DataFrame de entrada.
    :param subset: Lista de colunas a considerar na verificação de duplicações exatas.
                   Se None, considera todas as colunas.
    :return: DataFrame sem duplicações exatas.
    """
    return df.dropDuplicates(subset)


def deduplicate_by_max(df: DataFrame, partition_cols: list[str], target_col: str) -> DataFrame:
    """
    Remove duplicações mantendo o valor máximo de uma coluna específica por grupo.

    :param df: DataFrame de entrada.
    :param partition_cols: Lista de colunas para particionar.
    :param target_col: Coluna cujo valor máximo será mantido.
    :return: DataFrame com registros únicos por grupo com o valor máximo.
    :raises AnalysisException: se as colunas não existirem.
    """
    if target_col not in df.columns:
        raise AnalysisException(f"Coluna '{target_col}' não encontrada no DataFrame.")
    for col in partition_cols:
        if col not in df.columns:
            raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    window_spec = Window.partitionBy(*partition_cols).orderBy(F.col(target_col).desc())
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1) \
             .drop("rn")


def deduplicate_by_min(df: DataFrame, partition_cols: list[str], target_col: str) -> DataFrame:
    """
    Remove duplicações mantendo o valor mínimo de uma coluna específica por grupo.

    :param df: DataFrame de entrada.
    :param partition_cols: Lista de colunas para particionar.
    :param target_col: Coluna cujo valor mínimo será mantido.
    :return: DataFrame com registros únicos por grupo com o valor mínimo.
    :raises AnalysisException: se as colunas não existirem.
    """
    if target_col not in df.columns:
        raise AnalysisException(f"Coluna '{target_col}' não encontrada no DataFrame.")
    for col in partition_cols:
        if col not in df.columns:
            raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    window_spec = Window.partitionBy(*partition_cols).orderBy(F.col(target_col).asc())
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1) \
             .drop("rn")
