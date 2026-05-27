from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.models import Variable

import os
import pytz

TZ = os.getenv('TZ')
timezone = pytz.timezone(TZ)

VAR1 = Variable.get("AIRFLOW_VAR_1")

VAR2 = Variable.get("AIRFLOW_VAR_2", deserialize_json=True)
VAR2_STRING = VAR2.get('data').get('string')



default_args = {
    'owner': 'datapath',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='first_dag_v4',
    default_args=default_args,
    description="descripcion del proceso",
    start_date=datetime(2026, 2, 11, 13, tzinfo=timezone),
    schedule="@once",
    params={"name":"Datapath", "proyect":"Airflow"}
) as dag:

    task1 = BashOperator(
        task_id="task_1",
        bash_command=f"echo hello world - task 1 [{VAR1}]"
    )

    task2 = BashOperator(
        task_id="task_2",
        bash_command="echo hello world - task 2 [$name]",
        env={
            "name": '{{ dag_run.conf["name"] }}'
        }
    )

    task3 = BashOperator(
        task_id="task_3",
        bash_command="echo hello world - task 3 [$AIRFLOW_HOME]"
    )

    task1 >> [task2, task3]