from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(dag_id="dags_pyspark_minio_test",
         tags=['spark'],
         start_date=datetime(2025, 1, 16),
         catchup=False,
         schedule_interval=None) as dag:

    make_source = SparkSubmitOperator(
        task_id="make_source_task",
        application="/opt/airflow/pyspark/minio_source.py",
        conn_id="spark_local",
        name="make_source_task",
        conf={
            "spark.master": "local[*]",
            "spark.jars": "/opt/airflow/pyspark/jar/aws-java-sdk-bundle-1.12.497.jar,"
                          "/opt/airflow/pyspark/jar/hadoop-aws-3.3.2.jar,"
                          "/opt/airflow/pyspark/jar/hadoop-common-3.3.2.jar",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
    )

    make_source