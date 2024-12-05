import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["shj5978", "test1"],
) as dag:
    
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whomi",
    )
    # [END howto_operator_bash]

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2