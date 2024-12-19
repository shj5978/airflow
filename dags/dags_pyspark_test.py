from datetime import datetime
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date' : datetime(2022,9,16)
}


with DAG(dag_id="dags_pyspark_test",
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag :

    # Preprocessing task
    preprocess = SparkSubmitOperator(
        task_id="preprocess",
        application="/opt/airflow/pyspark/preprocessing.py",  # PySpark script 경로
        conn_id="spark_local",  # Spark 연결 ID
        name="preprocess_task",
        master="local[*]"  # Spark master 설정
    )

    # Analytics task
    analytics = SparkSubmitOperator(
        task_id="analytics",
        application="/opt/airflow/pyspark/analytics.py",  # PySpark script 경로
        conn_id="spark_local",  # Spark 연결 ID
        name="analytics_task",
        master="local[*]"  # Spark master 설정
    )

    preprocess >> analytics