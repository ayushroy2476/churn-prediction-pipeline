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
# Airflow's BashOperator doesn't `cd` before running this script.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = PROJECT_ROOT / "models_artifacts" / "repeat_purchase_model.joblib"


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
    return client.query(query).to_dataframe(create_bqstorage_client=True)


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

    df = load_features(client, project_id, marts_dataset)
    logger.info("Loaded %d rows from feature_customer_ml", len(df))

    if df.empty:
        raise RuntimeError(
            "feature_customer_ml returned 0 rows -- refusing to score and "
            "truncate the predictions table with empty data. Check the "
            "upstream dbt models before rerunning."
        )

    # No manual imputation here, on purpose. The pipeline's own imputer --
    # fit on the training fold in train_model.py, serialized into the
    # .joblib artifact -- handles missing values the same way it did during
    # training. Pre-filling here with different values, like an earlier
    # version of this script did, would just override that.
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