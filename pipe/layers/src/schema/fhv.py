from pyspark.sql.types import *

fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", LongType(), True),
    StructField("DOlocationID", LongType(), True),
    StructField("SR_Flag", LongType(), True),
    StructField("Affiliated_base_number", StringType(), True)
])
