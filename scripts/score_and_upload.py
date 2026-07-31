"""
Scores all customers in the feature table with the trained model and
writes the predictions back to BigQuery so they can be consumed by a
dashboard or downstream application.

Usage:
    python scripts/score_and_upload.py
"""

import logging
import os
from datetime import datetime, timezone
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

MODEL_PATH = Path("models_artifacts/repeat_purchase_model.joblib")


def load_features(client: bigquery.Client, project_id: str, marts_dataset: str) -> pd.DataFrame:
    query = f"""
        select *
        from `{project_id}.{marts_dataset}.feature_customer_ml`
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def main() -> None:
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
    for col in NUMERIC_FEATURES:
        df[col] = df[col].fillna(df[col].median())
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].fillna("unknown")

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    df["repeat_purchase_probability"] = pipeline.predict_proba(X)[:, 1]
    df["scored_at"] = datetime.now(timezone.utc)

    output_cols = [
        "customer_unique_id",
        "first_order_id",
        "customer_state",
        "repeat_purchase_probability",
        "scored_at",
    ]
    predictions = df[output_cols]

    table_id = f"{project_id}.{predictions_dataset}.customer_repeat_purchase_scores"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(predictions, table_id, job_config=job_config)
    job.result()

    logger.info("Wrote %d scored customers to %s", len(predictions), table_id)


if __name__ == "__main__":
    main()
