from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def add_ingestion_timestamp(df: DataFrame, col_name: str = "data_ingestao") -> DataFrame:
    """
    Adiciona uma coluna com o timestamp atual (UTC), representando o momento de ingestão dos dados.

    :param df: DataFrame de entrada.
    :param col_name: Nome da nova coluna a ser criada.
    :return: DataFrame com a coluna adicionada.
    """
    return df.withColumn(col_name, F.current_timestamp())


def add_update_timestamp(df: DataFrame, col_name: str = "data_atualizacao") -> DataFrame:
    """
    Adiciona uma coluna com o timestamp atual (UTC), representando o momento de atualização dos dados.

    :param df: DataFrame de entrada.
    :param col_name: Nome da nova coluna a ser criada.
    :return: DataFrame com a coluna adicionada.
    """
    return df.withColumn(col_name, F.current_timestamp())


def add_ano_mes(df: DataFrame, base_col: str = "data_base", new_col: str = "ano_mes") -> DataFrame:
    """
    Cria uma coluna contendo o ano e mês no formato 'yyyyMM' a partir de uma coluna de data.

    :param df: DataFrame de entrada.
    :param base_col: Nome da coluna com valores de data.
    :param new_col: Nome da nova coluna que conterá 'yyyyMM'.
    :return: DataFrame com a nova coluna.
    :raises AnalysisException: se a coluna base não existir no DataFrame.
    """
    if base_col not in df.columns:
        raise AnalysisException(f"Coluna '{base_col}' não encontrada no DataFrame.")
    return df.withColumn(new_col, F.date_format(F.col(base_col), "yyyyMM"))


def add_date_components(df: DataFrame, date_col: str = "data", prefix: str = "") -> DataFrame:
    """
    Adiciona colunas separadas para ano, mês e dia com base em uma coluna de data.

    :param df: DataFrame de entrada.
    :param date_col: Nome da coluna de data.
    :param prefix: Prefixo a ser adicionado às novas colunas (opcional).
    :return: DataFrame com colunas de componentes temporais.
    :raises AnalysisException: se a coluna base não existir no DataFrame.
    """
    if date_col not in df.columns:
        raise AnalysisException(f"Coluna '{date_col}' não encontrada no DataFrame.")

    return (
        df.withColumn(f"{prefix}ano", F.year(F.col(date_col)))
          .withColumn(f"{prefix}mes", F.month(F.col(date_col)))
          .withColumn(f"{prefix}dia", F.dayofmonth(F.col(date_col)))
    )


def convert_str_to_timestamp(df: DataFrame, col: str, fmt: str = "yyyy-MM-dd HH:mm:ss", new_col: str = None) -> DataFrame:
    """
    Converte uma coluna de string para tipo timestamp usando um formato especificado.

    :param df: DataFrame de entrada.
    :param col: Nome da coluna com valores de data em string.
    :param fmt: Formato da data/hora (padrão ISO completo).
    :param new_col: Nome da nova coluna resultante. Se None, sobrescreve a original.
    :return: DataFrame com a coluna convertida para timestamp.
    :raises AnalysisException: se a coluna não existir.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")
    
    target_col = new_col if new_col else col
    return df.withColumn(target_col, F.to_timestamp(F.col(col), fmt))


def convert_str_to_date(df: DataFrame, col: str, fmt: str = "yyyy-MM-dd", new_col: str = None) -> DataFrame:
    """
    Converte uma coluna de string para tipo date usando um formato especificado.

    :param df: DataFrame de entrada.
    :param col: Nome da coluna com valores de data em string.
    :param fmt: Formato da data (padrão ISO).
    :param new_col: Nome da nova coluna resultante. Se None, sobrescreve a original.
    :return: DataFrame com a coluna convertida para date.
    :raises AnalysisException: se a coluna não existir.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")
    
    target_col = new_col if new_col else col
    return df.withColumn(target_col, F.to_date(F.col(col), fmt))


def filter_by_date_range(df: DataFrame, date_col: str, start_date: str, end_date: str) -> DataFrame:
    """
    Filtra um DataFrame com base em um intervalo de datas (inclusive).

    :param df: DataFrame de entrada.
    :param date_col: Nome da coluna de data para filtro.
    :param start_date: Data inicial (formato 'yyyy-MM-dd').
    :param end_date: Data final (formato 'yyyy-MM-dd').
    :return: DataFrame filtrado pelas datas.
    :raises AnalysisException: se a coluna não existir.
    """
    if date_col not in df.columns:
        raise AnalysisException(f"Coluna '{date_col}' não encontrada no DataFrame.")
    
    return df.filter((F.col(date_col) >= F.lit(start_date)) & (F.col(date_col) <= F.lit(end_date)))
