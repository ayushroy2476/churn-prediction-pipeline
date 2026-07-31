"""
Orchestrates the daily churn-prediction pipeline:

    ingest raw data -> dbt build -> dbt test -> score customers

Drop this file into your Airflow `dags/` folder. It assumes:
    - dbt project lives at /opt/airflow/dbt_project (mount it as a volume)
    - scripts/ live at /opt/airflow/scripts (mount it as a volume)
    - data/ lives at /opt/airflow/data (mount it as a volume)
    - Environment variables (GCP_PROJECT_ID, BQ_RAW_DATASET, etc.) are
      set on the Airflow worker/container.

Note: model training (train_model.py) is intentionally NOT part of this
daily DAG. Retraining happens on its own periodic cadence (weekly/monthly),
run manually or as a separate DAG -- serving and training don't need to
move at the same speed.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="churn_prediction_pipeline",
    description="Ingest Olist data, transform with dbt, score repeat-purchase likelihood",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "bigquery", "ml"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /opt/airflow/scripts/ingest_to_bigquery.py --data-dir /opt/airflow/data/raw",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt_project && dbt build",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    score_customers = BashOperator(
        task_id="score_customers",
        bash_command="python /opt/airflow/scripts/score_and_upload.py",
    )

    ingest >> dbt_build >> dbt_test >> score_customers
