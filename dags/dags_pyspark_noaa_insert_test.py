from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date' : datetime(2022,9,16)
}


with DAG(dag_id="dags_pyspark_noaa_insert_test",
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag :

    # insert task
    insert = SparkSubmitOperator(
        task_id="insert",
        application="/opt/airflow/pyspark/noaa_insert.py",  # PySpark script 경로
        conn_id="spark_local",  # Spark 연결 ID
        name="insert_task",
        conf={
            "spark.master": "local[*]",
            "spark.executorEnv.MPLCONFIGDIR": "/opt/airflow/.matplotlib_cache",
            "spark.jars": "/opt/airflow/pyspark/jar/postgresql-42.6.0.jar"
        }  # 환경 설정
    )

    insert
