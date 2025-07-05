from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.utils import AnalysisException


def clean_nulls(df: DataFrame, cols: list[str], dtype: str, default=None) -> DataFrame:
    """
    Substitui valores nulos em colunas específicas por valores padrão, de acordo com o tipo de dado.

    :param df: DataFrame de entrada.
    :param cols: Lista de colunas a serem tratadas.
    :param dtype: Tipo de dado como string ("string", "int", "double", "date", "timestamp").
    :param default: Valor padrão para substituição (opcional).
    :return: DataFrame com colunas tratadas.
    """
    # Valores padrão por tipo
    default_values = {
        "string": "desconhecido",
        "int": 0,
        "double": 0.0,
        "date": "1900-01-01",
        "timestamp": "1900-01-01 00:00:00"
    }

    default = default if default is not None else default_values.get(dtype)

    if default is None:
        raise ValueError(f"Tipo '{dtype}' não suportado ou valor default não fornecido.")

    for col in cols:
        if col not in df.columns:
            raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

        if dtype == "string":
            df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(default)).otherwise(F.col(col)))
        elif dtype in ["double", "int"]:
            df = df.withColumn(col, F.when(F.col(col).isNull(), F.lit(default)).otherwise(F.col(col).cast(dtype)))
        elif dtype == "date":
            df = df.withColumn(col, F.when(F.col(col).isNull(), F.to_date(F.lit(default))).otherwise(F.col(col)))
        elif dtype == "timestamp":
            df = df.withColumn(col, F.when(F.col(col).isNull(), F.to_timestamp(F.lit(default))).otherwise(F.col(col)))

    return df


def clean_all_by_type(df: DataFrame, dtype: str, default=None) -> DataFrame:
    """
    Aplica a limpeza de nulos a todas as colunas de um determinado tipo no DataFrame.

    :param df: DataFrame de entrada.
    :param dtype: Tipo de dado como string.
    :param default: Valor padrão (opcional).
    :return: DataFrame com colunas do tipo especificado tratadas.
    """
    type_mapping = {
        "string": StringType,
        "int": IntegerType,
        "double": DoubleType,
        "date": DateType,
        "timestamp": TimestampType
    }

    if dtype not in type_mapping:
        raise ValueError(f"Tipo '{dtype}' não é suportado.")

    cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == dtype]
    return clean_nulls(df, cols, dtype, default)


def apply_trim_to_strings(df: DataFrame) -> DataFrame:
    """
    Aplica trim (remoção de espaços em branco) a todas as colunas do tipo string.

    :param df: DataFrame de entrada.
    :return: DataFrame com strings normalizadas.
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def normalize_timestamps_to_utc(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Converte colunas de timestamp para UTC usando função Spark `from_utc_timestamp`.

    :param df: DataFrame de entrada.
    :param cols: Lista de colunas do tipo timestamp a serem convertidas.
    :return: DataFrame com timestamps normalizados para UTC.
    """
    for col in cols:
        if col not in df.columns:
            raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")
        df = df.withColumn(col, F.from_utc_timestamp(F.col(col), "UTC"))
    return df


def standard_cleaning_pipeline(df: DataFrame) -> DataFrame:
    """
    Executa um pipeline de limpeza padrão: aplica trim, substitui nulos e normaliza timestamps para UTC.

    :param df: DataFrame de entrada.
    :return: DataFrame limpo e padronizado.
    """
    df = apply_trim_to_strings(df)
    df = clean_all_by_type(df, "string")
    df = clean_all_by_type(df, "int")
    df = clean_all_by_type(df, "double")
    df = clean_all_by_type(df, "date")
    df = clean_all_by_type(df, "timestamp")
    timestamp_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)]
    df = normalize_timestamps_to_utc(df, timestamp_cols)
    return df
