from datetime import datetime, timedelta
from airflow.decorators import dag


from common.add_task import task_virtualenv


@dag(
    dag_id="02_v2",
    start_date=datetime(2024, 1, 1),
    schedule=None
)
def mydag():
    task_virtualenv()

firsts_dag = mydag()
