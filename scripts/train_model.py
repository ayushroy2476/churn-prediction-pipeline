"""
Trains a repeat-purchase classifier on the dbt-built feature table
(<project>.olist_marts.feature_customer_ml) and saves the model artifact
locally.

Usage:
    python scripts/train_model.py
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    classification_report,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

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
TARGET = "repeat_within_90d"

# Resolved relative to this file, not the caller's working directory --
# same reasoning as the identical fix in score_and_upload.py.
MODEL_DIR = Path(__file__).resolve().parent.parent / "models_artifacts"
MODEL_DIR.mkdir(exist_ok=True)
MODEL_PATH = MODEL_DIR / "repeat_purchase_model.joblib"
METADATA_PATH = MODEL_DIR / "model_metadata.json"


def load_features() -> pd.DataFrame:
    project_id = os.environ["GCP_PROJECT_ID"]
    marts_dataset = os.environ.get("BQ_MARTS_DATASET", "olist_marts")
    client = bigquery.Client(project=project_id)
    query = f"""
        select *
        from `{project_id}.{marts_dataset}.feature_customer_ml`
        where {TARGET} is not null
    """
    logger.info("Pulling feature table from BigQuery...")
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def build_pipeline(scale_pos_weight: float = 1.0) -> Pipeline:
    # Imputation lives inside the pipeline and gets fit only on the
    # training fold. Fitting it on the full dataset before splitting (train
    # + test together), like the previous version did, leaks test-set
    # information into the values used to fill missing training data --
    # quietly inflates the reported metrics. This also means
    # score_and_upload.py no longer needs its own imputation step: whatever
    # this pipeline learns gets serialized into the .joblib artifact and
    # reapplied automatically, consistently, at scoring time.
    numeric_transformer = SimpleImputer(strategy="median")
    categorical_transformer = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="constant", fill_value="unknown")),
            ("encode", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES),
        ]
    )
    model = XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        eval_metric="logloss",
        scale_pos_weight=scale_pos_weight,
        random_state=42,
    )
    return Pipeline(steps=[("preprocess", preprocessor), ("model", model)])


def main() -> None:
    if "GCP_PROJECT_ID" not in os.environ:
        raise SystemExit(
            "GCP_PROJECT_ID is not set. Configure it in .env before running "
            "this script."
        )

    df = load_features()
    logger.info("Loaded %d customer rows with a known label", len(df))

    if df.empty:
        raise RuntimeError(
            "feature_customer_ml returned 0 labeled rows -- nothing to "
            "train on. Check the upstream dbt models."
        )

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    scale_pos_weight = neg / pos
    logger.info(
        "Class balance in training set: %d negative / %d positive (scale_pos_weight=%.2f)",
        neg, pos, scale_pos_weight,
    )

    pipeline = build_pipeline(scale_pos_weight=scale_pos_weight)
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_proba)
    logger.info("Test ROC-AUC: %.4f", auc)
    logger.info("\n%s", classification_report(y_test, y_pred, zero_division=0))

    # Precision/recall across a spread of thresholds -- use this to
    # actually tune LOW_THRESHOLD / HIGH_THRESHOLD in
    # dashboard/lib/scoring_service.py, which are placeholder guesses right
    # now (0.35 / 0.68), not backtested against anything.
    logger.info("Precision/recall at candidate probability thresholds:")
    for threshold in (0.3, 0.4, 0.5, 0.6, 0.7):
        preds_at_threshold = (y_proba >= threshold).astype(int)
        precision = precision_score(y_test, preds_at_threshold, zero_division=0)
        recall = recall_score(y_test, preds_at_threshold, zero_division=0)
        logger.info("  >=%.1f: precision=%.3f recall=%.3f", threshold, precision, recall)

    joblib.dump(pipeline, MODEL_PATH)
    logger.info("Saved model to %s", MODEL_PATH)

    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_rows": len(df),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "roc_auc": round(float(auc), 4),
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
    }
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info("Saved training metadata to %s", METADATA_PATH)


if __name__ == "__main__":
    main()
    