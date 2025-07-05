from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def generate_row_hash(df: DataFrame, cols: list[str], new_col: str = "row_hash") -> DataFrame:
    """
    Gera uma coluna de hash SHA-256 para cada linha do DataFrame com base na concatenação de colunas especificadas.

    :param df: DataFrame de entrada.
    :param cols: Lista de colunas a serem incluídas na geração do hash.
    :param new_col: Nome da nova coluna onde o hash será armazenado.
    :return: DataFrame com a nova coluna de hash.
    :raises AnalysisException: se alguma coluna especificada não existir no DataFrame.
    """
    # Validação de existência das colunas
    missing_cols = [col for col in cols if col not in df.columns]
    if missing_cols:
        raise AnalysisException(f"Colunas não encontradas no DataFrame: {missing_cols}")

    # Concatenação com separador padronizado e aplicação de hash SHA-256
    return df.withColumn(new_col, F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in cols]), 256))


def generate_dataset_hash(df: DataFrame, cols: list[str], hash_col: str = "row_hash") -> str:
    """
    Gera um hash SHA-256 global do DataFrame com base na agregação ordenada dos hashes de linha.

    :param df: DataFrame de entrada.
    :param cols: Colunas usadas para gerar o hash de linha.
    :param hash_col: Nome temporário da coluna de hash de linha.
    :return: Hash SHA-256 representando todo o conjunto de dados.
    """
    # Gera hashes por linha
    df_hashed = generate_row_hash(df, cols, new_col=hash_col)

    # Agrega todos os hashes em uma string ordenada e aplica um novo hash global
    ordered_concat = df_hashed.orderBy(cols).agg(
        F.sha2(F.concat_ws("", F.collect_list(F.col(hash_col))), 256).alias("dataset_hash")
    )

    return ordered_concat.collect()[0]["dataset_hash"]
