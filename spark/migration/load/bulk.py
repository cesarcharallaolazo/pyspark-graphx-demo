from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils.settings import pg_configuration


def bulk_data(df: DataFrame, target_table):
    df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url",
                f"jdbc:postgresql://{pg_configuration['host']}:{pg_configuration['port']}"
                f"/{pg_configuration['database']}") \
        .option("user", pg_configuration['user']) \
        .option("password", pg_configuration['password']) \
        .option("dbtable", target_table) \
        .option("numPartitions", 10) \
        .option("pushDownPredicate", "false") \
        .mode("overwrite") \
        .save()
