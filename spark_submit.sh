make build &&\
cd target &&\

#aws s3 cp . s3://my-bucket/SPARK_RESOURCES/spark_job/ --recursive
#aws emr add-steps --cluster-id j-XXXXX --steps Type=CUSTOM_JAR,Name='Spark_Job_Test',Jar='command-runner.jar',ActionOnFailure=CANCEL_AND_WAIT,Args='[spark-submit,--deploy-mode,client,--master,yarn,--py-files,s3://my-bucket/SPARK_RESOURCES/spark_job/spark.zip,s3://my-bucket/SPARK_RESOURCES/spark_job/run_migration.py,--root_path,s3://my-bucket/job_data/,--checkpoint_path,s3://my-bucket/SPARK_RESOURCES/spark_job/checkpoints/]'
#--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 --repositories https://repos.spark-packages.org

spark_submit_cmd="spark-submit --jars /home/jovyan/SparkProjects/Project1/jars/graphframes_graphframes-0.8.1-spark3.0-s_2.12.jar,/home/jovyan/SparkProjects/Project1/jars/org.slf4j_slf4j-api-1.7.16.jar,/home/jovyan/SparkProjects/Project1/jars/postgresql-42.2.23.jre7.jar,/home/jovyan/SparkProjects/Project1/jars/snowflake-ingest-sdk-0.10.3.jar,/home/jovyan/SparkProjects/Project1/jars/snowflake-jdbc-3.13.6.jar,/home/jovyan/SparkProjects/Project1/jars/spark-snowflake_2.12-2.9.1-spark_3.0.jar --master local[*] --deploy-mode client --py-files spark.zip,/home/jovyan/SparkProjects/Project1/jars/graphframes_graphframes-0.8.1-spark3.0-s_2.12.jar,/home/jovyan/SparkProjects/Project1/jars/org.slf4j_slf4j-api-1.7.16.jar run_migration.py --root_path /home/jovyan/SparkProjects/Project1/source_data --checkpoint_path /home/jovyan/SparkProjects/Project1/source_data/checkpoints/"
eval $spark_submit_cmd
