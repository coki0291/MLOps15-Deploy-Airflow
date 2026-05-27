from datetime import datetime, timedelta
from airflow.decorators import dag, task

from common.add_task import task_training_model


@dag(
    dag_id="02_v4",
    start_date=datetime(2024, 1, 1),
    schedule=None
)
def mydag():
    task_training_model()

firsts_dag = mydag()
