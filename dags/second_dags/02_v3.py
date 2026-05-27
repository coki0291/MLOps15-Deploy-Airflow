from datetime import datetime, timedelta
from airflow.decorators import dag, task


from common.add_task import task_virtualenv

@task.docker(image='datapath:latest')
def task_docker():

    import pandas as pd
    import sys

    print("python version")
    print(sys.version)




@dag(
    dag_id="02_v3",
    start_date=datetime(2024, 1, 1),
    schedule=None
)
def mydag():
    task_virtualenv()
    task_docker()

firsts_dag = mydag()
