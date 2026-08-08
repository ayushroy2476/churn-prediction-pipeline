"""
Orchestrates the daily churn-prediction pipeline:

    ingest raw data -> dbt build (run+test) -> score customers

Drop this file into your Airflow `dags/` folder. Assumes:
    - dbt project lives at /opt/airflow/dbt_project (mounted volume)
    - scripts/ live at /opt/airflow/scripts (mounted volume)
    - data/ lives at /opt/airflow/data (mounted volume)
    - dbt runs inside its own virtualenv at /opt/airflow/dbt_venv --
      dbt-core's pinned dependencies collide with Airflow's, so it's kept
      isolated rather than installed into the worker's own environment
    - an Airflow Variable `dbt_target` is registered (e.g. "prod"). If this
      project only ever runs against one target, feel free to just hardcode
      --target directly instead of going through a Variable.
    - `slack_alerts_webhook` is an optional Airflow Variable -- failures
      just go unnotified without it, fine for local/dev
    - GCP_PROJECT_ID, BQ_RAW_DATASET etc. are set on the worker

Model training (train_model.py) is intentionally NOT part of this daily
DAG. Retraining runs on its own weekly/monthly cadence, separately --
serving and training don't need to move at the same speed.

Note: the Olist dataset is a static historical Kaggle download, not a
daily-arriving feed (see README) -- so unlike a real production pipeline,
there's no "wait for today's extract to land" step here. `ingest_raw_data`
just re-loads the same data/raw/ files every run.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_VENV_ACTIVATE = "source /opt/airflow/dbt_venv/bin/activate"


def notify_slack_on_failure(context):
    """Post a failure alert to #data-eng-alerts. Silently no-ops if the
    webhook Variable isn't set, so it's safe to leave on in dev."""
    from airflow.models import Variable

    webhook = Variable.get("slack_alerts_webhook", default_var=None)
    if not webhook:
        return

    import requests

    ti = context["task_instance"]
    requests.post(
        webhook,
        json={
            "text": (
                f":red_circle: `{ti.task_id}` failed in `{ti.dag_id}` "
                f"(run {context['run_id']})\n<{ti.log_url}|logs>"
            )
        },
        timeout=10,
    )


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="churn_prediction_pipeline",
    description="Ingest Olist data, transform with dbt, score repeat-purchase likelihood",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["dbt", "bigquery", "ml"],
    doc_md=__doc__,
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            "python /opt/airflow/scripts/ingest_to_bigquery.py "
            "--data-dir /opt/airflow/data/raw"
        ),
        doc_md="Loads the Olist CSVs into the BQ raw dataset. Same static files every run -- there's no per-day extract to point at.",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            DBT_VENV_ACTIVATE + " && cd /opt/airflow/dbt_project && "
            "dbt build --target {{ var.value.dbt_target }} --fail-fast"
        ),
        doc_md=(
            "`dbt build` already tests each model right after building it, "
            "in dependency order -- a separate `dbt test` step afterward is "
            "redundant, and a `dbt run` + `dbt test` split would let bad "
            "data propagate through the whole graph before anything gets "
            "checked. `--fail-fast` stops at the first failure."
        ),
    )

    score_customers = BashOperator(
        task_id="score_customers",
        bash_command="python /opt/airflow/scripts/score_and_upload.py --date {{ ds }}",
        execution_timeout=timedelta(minutes=20),
        doc_md=(
            "Scores today's customer snapshot and writes to the serving "
            "table. Reads the model artifact published by the separate "
            "weekly training DAG -- never trains one itself. Requires "
            "score_and_upload.py to accept --date (see scripts/ fixes)."
        ),
    )

    ingest >> dbt_build >> score_customers