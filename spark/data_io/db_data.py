from pyspark.sql import SparkSession
from utils.settings import pg_configuration, sfOptions


def get_postgress_query(spark: SparkSession, query):
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url",
                f"jdbc:postgresql://{pg_configuration['host']}:{pg_configuration['port']}"
                f"/{pg_configuration['database']}") \
        .option("user", pg_configuration['user']) \
        .option("password", pg_configuration['password']) \
        .option("query", query) \
        .load()

    return jdbcDF


def get_snowflake_query(spark: SparkSession, query):
    df = spark.read.format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("query", query) \
        .option("autopushdown", "off") \
        .load()

    return df
