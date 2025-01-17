from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(dag_id="dags_pyspark_minio_test",
         tags=['spark'],
         catchup=False) as dag :

    # Preprocessing task
    make_source = SparkSubmitOperator(
        task_id="make_source_task",
        application="/opt/airflow/pyspark/minio_source.py",  # PySpark script 경로
        conn_id="spark_local",  # Spark 연결 ID
        name="make_source_task",
        conf={
        "spark.master": "local[*]",
        "spark.jars": "/opt/airflow/pyspark/jar/aws-java-sdk-bundle-1.12.497.jar,"
                      "/opt/airflow/pyspark/jar/hadoop-aws-3.3.2.jar,"
                      "/opt/airflow/pyspark/jar/hadoop-common-3.3.2.jar"
        }
    )

    make_source