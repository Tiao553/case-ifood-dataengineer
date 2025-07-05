from pyspark.sql.functions import col
from pyspark.sql.types import *

def apply_schema_with_cast(df, schema, logger):
    logger.info("🛠️ Aplicando coerção de tipos com base no schema definido...")

    for field in schema.fields:
        col_name = field.name
        col_type = field.dataType.simpleString()

        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, col(col_name).cast(col_type))
            except Exception as e:
                logger.warning(f"⚠️ Falha ao converter coluna {col_name} para {col_type}: {str(e)}")
        else:
            logger.warning(f"⚠️ Coluna {col_name} não encontrada no dataframe. Será ignorada.")

    return df.select([field.name for field in schema.fields])
