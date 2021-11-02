from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


def transform_data(df: DataFrame):
    # process values
    df = df.dropDuplicates(subset=["col16"]) \
        .fillna({'Col1': 0, 'Col2': 0, 'col3': 0})

    # cast types
    df = df.withColumn("Col1", col("Col1").cast(IntegerType())) \
        .withColumn("col3", col("col3").cast(IntegerType())) \
        .withColumn("Col2", col("Col2").cast(DoubleType())) \
        .withColumn("col10", col("col10").cast(DoubleType())) \
        .withColumnRenamed("col11", "col11") \
        .withColumnRenamed("col12", "col12") \
        .withColumnRenamed("col13", "col13") \
        .withColumnRenamed("col14", "col14") \
        .withColumnRenamed("col15", "col15") \
        .withColumnRenamed("col16", "col16")

    return df
