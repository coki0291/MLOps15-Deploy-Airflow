from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


import os
import pytz

TZ = os.getenv('TZ')
timezone = pytz.timezone(TZ)

default_args = {
    'owner': 'datapath',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='first_dag_v2',
    default_args=default_args,
    description="descripcion del proceso",
    start_date=datetime(2026, 2, 11, 13, tzinfo=timezone),
    schedule="@once"
) as dag:
    
    task1 = BashOperator(
        task_id="task_1",
        bash_command="echo hello world - task 1 [$TZ]"
    )

    task2 = BashOperator(
        task_id="task_2",
        bash_command="echo hello world - task 2"
    )

    task3 = BashOperator(
        task_id="task_3",
        bash_command="echo hello world - task 3"
    )

    task1 >> [task2, task3]