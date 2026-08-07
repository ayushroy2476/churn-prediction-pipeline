"""
Scores all customers in the feature table with the trained model and
writes the predictions back to BigQuery so they can be consumed by a
dashboard or downstream application.

Usage:
    python scripts/score_and_upload.py [--date YYYY-MM-DD]

`--date` is the logical run date (matches Airflow's `{{ ds }}`). It's
written to the output table as `score_date` so reruns/backfills tag rows
with the date they represent, not the wall-clock time the job happened to
execute -- and defaults to today (UTC) when run by hand.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NUMERIC_FEATURES = [
    "first_order_value",
    "first_order_num_items",
    "first_order_freight_value",
    "first_order_delivery_delay_days",
    "first_order_review_score",
]
CATEGORICAL_FEATURES = ["customer_state", "first_order_product_category"]

# Resolved relative to this file, not the caller's working directory --
# Airflow's BashOperator doesn't `cd` before running this script, so a
# bare relative path here would depend on whatever CWD the worker
# happened to have. Assumes models_artifacts/ sits next to scripts/ at
# the project root; adjust if your layout differs.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = PROJECT_ROOT / "models_artifacts" / "repeat_purchase_model.joblib"
FILL_VALUES_PATH = PROJECT_ROOT / "models_artifacts" / "feature_fill_values.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date(),
        help="Logical run date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    return parser.parse_args()


def load_features(client: bigquery.Client, project_id: str, marts_dataset: str) -> pd.DataFrame:
    query = f"""
        select *
        from `{project_id}.{marts_dataset}.feature_customer_ml`
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def load_fill_values() -> dict | None:
    """Fill values computed once at training time (see train_model.py),
    so scoring imputes with the same statistics the model was trained on.
    Falls back to per-batch stats if the sidecar file isn't there yet, but
    logs a warning -- per-batch imputation means a customer's score can
    drift day to day purely because *other* customers in that day's batch
    changed, which is a real training/serving skew risk."""
    if not FILL_VALUES_PATH.exists():
        logger.warning(
            "%s not found -- falling back to imputing from today's batch "
            "instead of training-time statistics. See train_model.py.",
            FILL_VALUES_PATH,
        )
        return None
    with open(FILL_VALUES_PATH) as f:
        return json.load(f)


def apply_imputation(df: pd.DataFrame, fill_values: dict | None) -> pd.DataFrame:
    for col in NUMERIC_FEATURES:
        fill = fill_values[col] if fill_values else df[col].median()
        df[col] = df[col].fillna(fill)
    for col in CATEGORICAL_FEATURES:
        fill = fill_values[col] if fill_values else "unknown"
        df[col] = df[col].fillna(fill)
    return df


def main() -> None:
    args = parse_args()

    if "GCP_PROJECT_ID" not in os.environ:
        raise SystemExit(
            "GCP_PROJECT_ID is not set. Configure it on the Airflow worker "
            "or in .env before running this script."
        )
    project_id = os.environ["GCP_PROJECT_ID"]
    marts_dataset = os.environ.get("BQ_MARTS_DATASET", "olist_marts")
    predictions_dataset = os.environ.get("BQ_PREDICTIONS_DATASET", marts_dataset)

    client = bigquery.Client(project=project_id)

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"No model found at {MODEL_PATH}. Run scripts/train_model.py first."
        )
    pipeline = joblib.load(MODEL_PATH)
    fill_values = load_fill_values()

    df = load_features(client, project_id, marts_dataset)
    logger.info("Loaded %d rows from feature_customer_ml", len(df))

    if df.empty:
        raise RuntimeError(
            "feature_customer_ml returned 0 rows -- refusing to score and "
            "truncate the predictions table with empty data. Check the "
            "upstream dbt models before rerunning."
        )

    df = apply_imputation(df, fill_values)

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    df["repeat_purchase_probability"] = pipeline.predict_proba(X)[:, 1]
    df["score_date"] = args.date
    df["scored_at"] = datetime.now(timezone.utc)

    output_cols = [
        "customer_unique_id",
        "first_order_id",
        "customer_state",
        "repeat_purchase_probability",
        "score_date",
        "scored_at",
    ]
    predictions = df[output_cols]

    table_id = f"{project_id}.{predictions_dataset}.customer_repeat_purchase_scores"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(predictions, table_id, job_config=job_config)
    job.result()

    logger.info(
        "Wrote %d scored customers to %s (score_date=%s)",
        len(predictions),
        table_id,
        args.date,
    )


if __name__ == "__main__":
    main()
