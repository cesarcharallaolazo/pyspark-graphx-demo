from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import utils
from utils.constant import *


def read_migration_file_df(spark: SparkSession, info, ) -> DataFrame:
    df = spark.read.option("sep", "|") \
        .csv(info[migration_raw_path] + f'TestData.txt',
             header=True)

    return df
