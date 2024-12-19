from datetime import datetime
from email.mime import application
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



from datetime import datetime

default_args = {
    'start_date' : datetime(2022,9,16)
}


with DAG(dag_id="dags_pyspark_test",
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag :
    
    # preprocess
    preprocess = SparkSubmitOperator(
        application = "/opt/airflow/pyspark/preprocessing.py",
        task_id = "preprocess",
        conn_id = "spark_local"
    )

    # analytics
    analytics = SparkSubmitOperator(
        application = "/opt/airflow/pyspark/analytics.py",
        task_id = "analytics",
        conn_id = "spark_local"
    )

    preprocess >> analytics