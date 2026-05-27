from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email_smtp


TO_EMAIL = "slherrera91@gmail.com"


def notify_email(context, status):
    task_instance = context["task_instance"]
    subject = f"Airflow | Entrenamiento de Modelo - {status}"
    body = f"""
    <h3>Notificación de Entrenamiento de Modelo ML</h3>
    <table border="1" cellpadding="6" cellspacing="0">
        <tr><td><b>Estado</b></td><td>{status}</td></tr>
        <tr><td><b>DAG</b></td><td>{task_instance.dag_id}</td></tr>
        <tr><td><b>Tarea</b></td><td>{task_instance.task_id}</td></tr>
        <tr><td><b>Fecha de ejecución</b></td><td>{context["execution_date"]}</td></tr>
        <tr><td><b>Log</b></td><td><a href="{task_instance.log_url}">{task_instance.log_url}</a></td></tr>
    </table>
    """
    send_email_smtp(to=TO_EMAIL, subject=subject, html_content=body)


@task.virtualenv(
    task_id="train_model",
    requirements=["pandas", "numpy", "joblib", "scikit-learn"],
    system_site_packages=False,
)
def train_model():
    import os
    import pandas as pd
    import joblib
    from sklearn.linear_model import Lasso

    X_TRAIN = pd.read_csv("/opt/airflow/dags/data/inputs/xtrain.csv")
    Y_TRAIN = pd.read_csv("/opt/airflow/dags/data/inputs/ytrain.csv")
    FEATURES = pd.read_csv("/opt/airflow/dags/data/inputs/selected_features.csv")

    features = FEATURES["0"].to_list()
    X_TRAIN = X_TRAIN[features]

    model = Lasso(alpha=0.001, random_state=0)
    model.fit(X_TRAIN, Y_TRAIN)

    model_dir = "/opt/airflow/dags/data/model"
    os.makedirs(model_dir, exist_ok=True)
    joblib.dump(model, f"{model_dir}/model.joblib")

    print(f"Modelo entrenado con {X_TRAIN.shape[0]} filas y {X_TRAIN.shape[1]} features.")
    print("Modelo guardado en /opt/airflow/dags/data/model/model.joblib")


default_args = {
    "owner": "datapath",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="training_model_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=lambda ctx: notify_email(ctx, "Exitoso ✔"),
    on_failure_callback=lambda ctx: notify_email(ctx, "Fallido ✘"),
)
def training_dag():
    train_model()


training_dag()
