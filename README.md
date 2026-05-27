# MLOps15-Deploy-Airflow

Asignación Módulo 4 — Creación de DAGs en Airflow para Entrenamiento y Predicción de Modelos de Machine Learning.

---

## Estructura del proyecto

```
dags/
├── assignment/
│   ├── training_dag.py       # Parte 1: DAG de entrenamiento
│   └── prediction_dag.py     # Parte 2: DAG de predicción
├── common/
│   └── add_task.py           # Tarea virtualenv reutilizable (sesiones)
├── data/
│   └── inputs/
│       ├── xtrain.csv        # Features de entrenamiento
│       ├── ytrain.csv        # Target (SalePrice en escala log)
│       └── selected_features.csv  # Features seleccionadas para el modelo
├── email_dags/
│   └── email_dag.py          # DAG de email (sesiones)
├── first_dag/                # DAGs de la sesión 1
└── second_dags/              # DAGs de la sesión 2
docker-compose.yaml           # Infraestructura Airflow con SMTP configurado
```

---

## Parte 1: DAG de Entrenamiento

**Archivo:** [`dags/assignment/training_dag.py`](dags/assignment/training_dag.py)

- Entrena un modelo de regresión **Lasso** sobre el dataset de precios de casas (Ames Housing)
- Carga los datos desde archivos CSV locales (`xtrain.csv`, `ytrain.csv`, `selected_features.csv`)
- Guarda el modelo entrenado en `dags/data/model/model.joblib`
- **Notificación por email** al finalizar:
  - Correo de éxito si el entrenamiento completó correctamente
  - Correo de fallo si hubo algún error
  - El correo incluye: estado de la tarea, nombre del DAG, fecha de ejecución y link al log

**DAG ID:** `training_model_dag`

---

## Parte 2: DAG de Predicción

**Archivo:** [`dags/assignment/prediction_dag.py`](dags/assignment/prediction_dag.py)

Contiene dos tareas en secuencia:

| Task | Descripción |
|---|---|
| `process_data` | Carga `xtrain.csv`, selecciona las features del modelo y devuelve los datos procesados vía XCom |
| `predict` | Recibe los datos procesados, carga `model.joblib` y ejecuta las predicciones de precios |

- **Notificación por email** al finalizar (éxito o fallo) con fecha de ejecución y estado

**DAG ID:** `prediction_model_dag`

---

## Configuración SMTP (envío de emails)

Configurado en `docker-compose.yaml` con Gmail:

- Host: `smtp.gmail.com`
- Puerto: `587` con STARTTLS
- Destinatario: `slherrera91@gmail.com`

---

## Cómo ejecutar

1. Levantar los contenedores:
   ```bash
   docker compose up -d
   ```

2. Ingresar a la UI: [http://localhost:8081](http://localhost:8081) (usuario: `airflow`, contraseña: `airflow`)

3. Ejecutar primero `training_model_dag` (genera el modelo)

4. Ejecutar luego `prediction_model_dag` (usa el modelo para predecir)
