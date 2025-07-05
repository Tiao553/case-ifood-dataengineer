from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def mask_cpf(df: DataFrame, col: str, new_col: str = None) -> DataFrame:
    """
    Mascaramento de CPF, ocultando os primeiros e últimos dígitos.
    Exemplo: 123.456.789-00 → ***.456.***-**

    :param df: DataFrame de entrada.
    :param col: Coluna com o CPF em formato string (somente números ou formatado).
    :param new_col: Nome da nova coluna com valor mascarado. Se None, cria coluna original + '_masked'.
    :return: DataFrame com coluna mascarada.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    target = new_col or f"{col}_masked"
    return df.withColumn(
        target,
        F.concat(
            F.lit("***."),
            F.substring(F.regexp_replace(F.col(col), r"\D", ""), 4, 3),
            F.lit(".***-**")
        )
    )


def mask_email(df: DataFrame, col: str, new_col: str = None) -> DataFrame:
    """
    Mascaramento simples de e-mail, mantendo os 3 primeiros caracteres e substituindo o restante por padrão fixo.

    :param df: DataFrame de entrada.
    :param col: Coluna com o e-mail.
    :param new_col: Nome da nova coluna com valor mascarado. Se None, cria coluna original + '_masked'.
    :return: DataFrame com coluna mascarada.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    target = new_col or f"{col}_masked"
    return df.withColumn(
        target,
        F.concat(
            F.substring(F.col(col), 1, 3),
            F.lit("***@***.com")
        )
    )


def mask_phone(df: DataFrame, col: str, new_col: str = None) -> DataFrame:
    """
    Mascaramento de número de telefone, mantendo apenas os últimos 4 dígitos.

    :param df: DataFrame de entrada.
    :param col: Coluna com o telefone.
    :param new_col: Nome da nova coluna com valor mascarado. Se None, cria coluna original + '_masked'.
    :return: DataFrame com coluna mascarada.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    target = new_col or f"{col}_masked"
    return df.withColumn(
        target,
        F.concat(F.lit("***-"), F.substring(F.col(col), -4, 4))
    )


def hash_column(df: DataFrame, col: str, new_col: str = None) -> DataFrame:
    """
    Gera o hash SHA-256 de uma coluna, útil para anonimização irreversível.

    :param df: DataFrame de entrada.
    :param col: Coluna a ser hasheada.
    :param new_col: Nome da nova coluna com hash. Se None, cria coluna original + '_hash'.
    :return: DataFrame com coluna de hash.
    """
    if col not in df.columns:
        raise AnalysisException(f"Coluna '{col}' não encontrada no DataFrame.")

    target = new_col or f"{col}_hash"
    return df.withColumn(target, F.sha2(F.col(col).cast("string"), 256))


def mask_all(df: DataFrame, cpf_cols: list[str] = None, email_cols: list[str] = None,
             phone_cols: list[str] = None, hash_cols: list[str] = None) -> DataFrame:
    """
    Aplica mascaramento e hash de forma abrangente para listas de colunas especificadas.

    :param df: DataFrame de entrada.
    :param cpf_cols: Lista de colunas contendo CPF.
    :param email_cols: Lista de colunas contendo e-mail.
    :param phone_cols: Lista de colunas contendo telefones.
    :param hash_cols: Lista de colunas a serem anonimizadas por hash.
    :return: DataFrame com colunas mascaradas e/ou anonimizadas.
    """
    cpf_cols = cpf_cols or []
    email_cols = email_cols or []
    phone_cols = phone_cols or []
    hash_cols = hash_cols or []

    for col in cpf_cols:
        df = mask_cpf(df, col)

    for col in email_cols:
        df = mask_email(df, col)

    for col in phone_cols:
        df = mask_phone(df, col)

    for col in hash_cols:
        df = hash_column(df, col)

    return df
