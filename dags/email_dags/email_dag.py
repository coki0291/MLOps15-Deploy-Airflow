import sys
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.utils.email import send_email_smtp
from common.add_task import task_training_model

def success_email(context):
    task_instance = context["task_instance"]
    task_status = "Success"

    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"""
    The task {task_instance.task_id} completed with status: {task_status}

    The task execution date is {context["execution_date"]}

    Log URL: {task_instance.log_url}
    """

    to_email = "slherrera91@gmail.com"

    send_email_smtp(
        to=to_email,
        subject=subject,
        html_content=body
    )

def failure_email(context):
    task_instance = context["task_instance"]
    task_status = "Failure"

    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"""
    The task {task_instance.task_id} completed with status: {task_status}

    The task execution date is {context["execution_date"]}

    Log URL: {task_instance.log_url}
    """

    to_email = "slherrera91@gmail.com"

    send_email_smtp(
        to=to_email,
        subject=subject,
        html_content=body
    )

default_args = {
    'owner': 'datapath',
    'email': ['tu_correo@dominio.com'], # <--- ¡ESTO ES CLAVE! Puede ser uno o varios mails
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False
}

@dag(
    dag_id="email_task",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    on_failure_callback = lambda context: failure_email(context),
    on_success_callback = lambda context: success_email(context)
)
def mydag():
    task_training_model()

first_dag = mydag()
