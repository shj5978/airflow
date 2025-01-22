from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="dags_boto3_minio_test",
         tags=['boto3'],
         start_date=datetime(2025, 1, 16),
         catchup=False,
         schedule_interval=None) as dag:

    make_source = BashOperator(
        task_id="make_source_task",
        bash_command="python3 /opt/airflow/pyspark/boto3_source.py",
    )

    upload_minio_source = BashOperator(
        task_id="upload_minio_source_task",
        bash_command="python3 /opt/airflow/pyspark/minio_upload.py",
    )

    # upload_boto3_source = BashOperator(
    #     task_id="upload_boto3_source_task",
    #     bash_command="python3 /opt/airflow/pyspark/boto3_upload.py",
    # )

    make_source >> upload_minio_source