from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email_smtp


TO_EMAIL = "slherrera91@gmail.com"


def notify_email(context, status):
    task_instance = context["task_instance"]
    subject = f"Airflow | Predicción de Modelo - {status}"
    body = f"""
    <h3>Notificación de Predicción de Modelo ML</h3>
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
    task_id="process_data",
    requirements=["pandas", "numpy"],
    system_site_packages=False,
)
def process_data() -> dict:
    """Task 1: Carga y procesa los datos de entrada seleccionando las features del modelo."""
    import pandas as pd

    X = pd.read_csv("/opt/airflow/dags/data/inputs/xtrain.csv")
    features_df = pd.read_csv("/opt/airflow/dags/data/inputs/selected_features.csv")
    features = features_df["0"].to_list()

    X_selected = X[features]

    print(f"Datos cargados: {X_selected.shape[0]} filas, {X_selected.shape[1]} features.")
    print(f"Features seleccionadas: {features}")

    return {
        "data": X_selected.to_dict("list"),
        "features": features,
        "n_rows": X_selected.shape[0],
    }


@task.virtualenv(
    task_id="predict",
    requirements=["pandas", "numpy", "joblib", "scikit-learn"],
    system_site_packages=False,
)
def predict(processed_data: dict) -> list:
    """Task 2: Carga el modelo entrenado y ejecuta predicciones sobre los datos procesados."""
    import numpy as np
    import pandas as pd
    import joblib

    features = processed_data["features"]
    X = pd.DataFrame(processed_data["data"])[features]

    model = joblib.load("/opt/airflow/dags/data/model/model.joblib")
    predictions = model.predict(X)

    prices = np.exp(predictions)

    print(f"Predicciones realizadas: {len(predictions)} registros")
    print(f"Precio promedio predicho: ${prices.mean():,.2f}")
    print(f"Precio mínimo predicho:   ${prices.min():,.2f}")
    print(f"Precio máximo predicho:   ${prices.max():,.2f}")

    return predictions.tolist()


default_args = {
    "owner": "datapath",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="prediction_model_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=lambda ctx: notify_email(ctx, "Exitoso ✔"),
    on_failure_callback=lambda ctx: notify_email(ctx, "Fallido ✘"),
)
def prediction_dag():
    processed = process_data()
    predict(processed)


prediction_dag()
