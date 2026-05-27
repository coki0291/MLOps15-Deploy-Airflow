from airflow.decorators import task

@task
def suma_task(x,y):
    print(f"Task arg: x={x}, y={y}")
    return x+y


@task.virtualenv(
    task_id="virtual_task", 
    requirements=["pandas", "numpy"],
    system_site_packages=False
)
def task_virtualenv():
    import pandas as pd
    import sys

    print("Python version")
    print(sys.version)





@task.virtualenv(
    task_id="virtual_task", 
    requirements=["pandas", "numpy", "joblib", "scikit-learn"],
    system_site_packages=False
)
def task_training_model():
    import sys
    from sklearn.linear_model import Lasso
    import pandas as pd
    import joblib

    PATH_COMMON = '../'
    sys.path.append(PATH_COMMON)

    X_TRAIN = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv')
    Y_TRAIN = pd.read_csv('/opt/airflow/dags/data/inputs/ytrain.csv')


    FEATURES = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')

    features = FEATURES['0'].to_list()  

    X_TRAIN = X_TRAIN[features]

    lin_model = Lasso(alpha=0.001, random_state=0)
    lin_model.fit(X_TRAIN, Y_TRAIN)

    joblib.dump(lin_model, '/opt/airflow/dags/data/model/model.joblib')

    print("modelo guardado")



