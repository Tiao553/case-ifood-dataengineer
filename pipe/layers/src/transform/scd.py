from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


def scd_type_2(
    historico_df: DataFrame,
    atual_df: DataFrame,
    business_key: list[str],
    tracked_cols: list[str],
    effective_date_col: str = "data_inicio",
    end_date_col: str = "data_fim",
    current_flag_col: str = "registro_ativo",
    end_date_default: str = "2400-12-31",
    current_flag_active: int = 1,
    current_flag_inactive: int = 0,
    processing_date: str = None
) -> DataFrame:
    """
    Implementa Slowly Changing Dimension Tipo 2 (SCD Type 2) com data de fim futura e flag inteiro.
    
    :param historico_df: DataFrame com dados históricos da dimensão.
    :param atual_df: DataFrame com dados atuais da dimensão.
    :param business_key: Lista de colunas identificadoras únicas da entidade de negócio.
    :param tracked_cols: Lista de colunas cujas mudanças devem ser rastreadas.
    :param effective_date_col: Nome da coluna de início de vigência.
    :param end_date_col: Nome da coluna de fim de vigência.
    :param current_flag_col: Nome da coluna que indica se o registro está ativo (1) ou inativo (0).
    :param end_date_default: Data de fim padrão para registros ativos. Ex.: "2400-12-31".
    :param current_flag_active: Valor inteiro para marcar registros ativos.
    :param current_flag_inactive: Valor inteiro para marcar registros inativos.
    :param processing_date: Data de processamento no formato ISO (yyyy-MM-dd). Se None, usa current_date().
    :return: DataFrame com histórico atualizado segundo a lógica de SCD Tipo 2.
    """

    # Validação de colunas
    for col in business_key + tracked_cols:
        if col not in atual_df.columns or col not in historico_df.columns:
            raise AnalysisException(f"Coluna '{col}' não encontrada em ambos os DataFrames.")

    # Data de processamento
    data_proc = F.lit(processing_date) if processing_date else F.current_date()
    end_date_val = F.lit(end_date_default).cast("date")

    # Filtra apenas registros ativos no histórico
    historico_ativos = historico_df.filter(F.col(current_flag_col) == F.lit(current_flag_active))

    # Define condição de junção pelas chaves de negócio
    join_cond = [F.coalesce(historico_ativos[c], F.lit(None)) == F.coalesce(atual_df[c], F.lit(None)) for c in business_key]
    joined_df = atual_df.alias("atual").join(
        historico_ativos.alias("hist"),
        on=join_cond,
        how="left"
    )

    # Detecta mudanças nos atributos rastreados
    change_condition = F.lit(False)
    for col in tracked_cols:
        change_condition = change_condition | (
            F.coalesce(F.col(f"atual.{col}"), F.lit(None)) != F.coalesce(F.col(f"hist.{col}"), F.lit(None))
        )

    # Registros novos ou modificados: inserção
    novos_registros = joined_df.filter(change_condition | F.col(f"hist.{business_key[0]}").isNull()) \
        .select("atual.*") \
        .withColumn(effective_date_col, data_proc) \
        .withColumn(end_date_col, end_date_val) \
        .withColumn(current_flag_col, F.lit(current_flag_active))

    # Registros antigos a serem encerrados: atualização
    encerramentos = joined_df.filter(change_condition & F.col(f"hist.{business_key[0]}").isNotNull()) \
        .select("hist.*") \
        .withColumn(end_date_col, data_proc) \
        .withColumn(current_flag_col, F.lit(current_flag_inactive))

    # Registros históricos não alterados e registros já inativos permanecem
    historico_inalterado = historico_df.filter(F.col(current_flag_col) == F.lit(current_flag_inactive))

    # Combina o histórico final
    historico_final = historico_inalterado \
        .unionByName(encerramentos) \
        .unionByName(novos_registros)

    return historico_final