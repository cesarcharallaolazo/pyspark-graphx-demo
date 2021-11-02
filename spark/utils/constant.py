from pyspark.sql.types import *
from pyspark.sql.functions import *

root_path = "root_path"
checkpoint_path = "checkpoint_path"
org = "org"
app_env = "app_env"
version = "version"

# migration
migration_raw_path = "migration_raw_path"
migration_raw_parquet_path = "migration_raw_parquet_path"
migration_abt_path = "migration_abt_path"
migration_json_path = "migration_json_path"
migration_raw_from_db = "migration_raw_from_db"

SLACK_MESSAGES = {
    "init_pyspark_process":
        {
            "process_number": "1",
            "process_title": "Init PySpark",
            "process_description": "Starting PySpark process in Spark-Job ...",
            "status_ok": "OK",
            "status_failed": "FAILED",
            "error_description_title": "Description: ",
            "error_description": "Review Logs from Spark-Job"
        },
    "pyspark_migration_process":
        {
            "process_number": "2",
            "process_title": "Transform/BulkLoad",
            "process_description": "Load Snowflake TO --> PostgresDB",
            "status_ok": "OK",
            "status_failed": "FAILED",
            "error_description_title": "Description: ",
            "error_description": "Review Logs from the Spark-Job & Verify Spark Failure"
        }
}
