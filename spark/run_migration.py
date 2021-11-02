from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.utils import success_step_msg, failed_step_msg
from utils.constant import *
from data_io.load import df_writer, get_data
from migration.transform import transform
from migration.load import bulk
from data_io.db_data import get_snowflake_query, get_postgress_query
from graphs import dummy
from graphframes import *
import argparse


def parse_cli_args():
    """
    Parse cli arguments
    returns a dictionary of arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--root_path', action='store', dest=root_path, type=str,
                        help='Store', default=None)

    parser.add_argument('--checkpoint_path', action='store', dest=checkpoint_path, type=str,
                        help='Store', default=None)

    parser.add_argument('--app_env', action='store', dest=app_env, type=str,
                        help='Store', default=None)

    parser.add_argument('--version', action='store', dest=version, type=str,
                        help='Store', default=None)

    parser.add_argument('--org', action='store', dest=org, type=str,
                        help='Store', default=None)

    known_args, unknown_args = parser.parse_known_args()
    known_args_dict = vars(known_args)
    return known_args_dict


if __name__ == '__main__':
    args = parse_cli_args()

    # demo
    args[migration_raw_path] = f"{args[root_path]}/raw/"
    args[migration_raw_parquet_path] = f"{args[root_path]}/raw_converted/"
    args[migration_abt_path] = f"{args[root_path]}/abt_fraud/"
    args[migration_json_path] = f"{args[root_path]}/raw_json/"
    args[migration_raw_from_db] = f"{args[root_path]}/raw_from_db/"

    # Start Spark Environment
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # Enable Pushdown in a Session
    spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils \
        .enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

    # Checkpointing tuning strategy
    sc = SparkContext.getOrCreate()
    sc.setCheckpointDir(args[checkpoint_path])

    dummy.test(spark)
    dummy.databricks_demo(spark)
