from datetime import datetime, timedelta
from airflow.decorators import dag


from common.add_task import suma_task


@dag(
    dag_id="02_v1",
    start_date=datetime(2024, 1, 1),
    schedule=None
)
def mydag():
    start = suma_task(1, 2)
    for i in range(3):
        start = suma_task(start, i+1)

firsts_dag = mydag()
