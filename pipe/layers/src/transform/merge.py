from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_delta(
    spark: SparkSession,
    df_final: DataFrame,
    delta_path: str,
    business_key: list[str],
    current_flag_col: str = "registro_ativo"
) -> None:
    """
    Realiza o merge SCD Tipo 2 em uma tabela Delta utilizando a operação MERGE INTO,
    com base em uma ou mais chaves de negócio.

    Esta função aplica:
    - Atualização de registros que sofreram alterações (pelo match de chaves);
    - Inserção de registros novos que não existem na tabela de destino;
    - Preservação dos registros inalterados.

    Exemplo de merge condition gerada (para business_key = ["id_cliente", "documento"]):

        target.id_cliente = source.id_cliente AND target.documento = source.documento

    :param spark: Sessão Spark ativa.
    :param df_final: DataFrame resultante da lógica SCD2 (com colunas data_inicio, data_fim, registro_ativo).
    :param delta_path: Caminho da tabela Delta no armazenamento (ex.: path no HDFS, S3 ou DBFS).
    :param business_key: Lista de colunas que identificam unicamente o registro.
    :param current_flag_col: Nome da coluna de flag de ativo (padrão: "registro_ativo").
    """
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)

        # Condição de correspondência para o merge
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in business_key])

        delta_table.alias("target").merge(
            source=df_final.alias("source"),
            condition=merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    else:
        # Primeira carga: grava como Delta
        df_final.write.format("delta").mode("overwrite").save(delta_path)
