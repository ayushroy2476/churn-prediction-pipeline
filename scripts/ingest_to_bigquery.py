"""
Loads the raw Olist CSV files into BigQuery.

Usage:
    python scripts/ingest_to_bigquery.py --data-dir data/raw

Requires:
    - GCP_PROJECT_ID env var
    - BQ_RAW_DATASET env var (default: olist_raw)
    - GOOGLE_APPLICATION_CREDENTIALS pointing at a service account key
      with BigQuery Data Editor + Job User roles
"""

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Maps the raw Kaggle filenames to the table names we want in BigQuery.
FILE_TABLE_MAP = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
    "olist_geolocation_dataset.csv": "geolocation",
}


def get_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    return bigquery.Client(project=project_id)


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "US") -> None:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset %s already exists", dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset, exists_ok=True)
        logger.info("Created dataset %s", dataset_id)


def load_csv_to_bq(
    client: bigquery.Client,
    csv_path: Path,
    dataset_id: str,
    table_name: str,
) -> None:
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    df = pd.read_csv(csv_path)
    logger.info("Loading %s (%d rows) -> %s", csv_path.name, len(df), table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for the load job to finish

    table = client.get_table(table_id)
    logger.info("Loaded %d rows into %s", table.num_rows, table_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Olist CSVs into BigQuery")
    parser.add_argument(
        "--data-dir",
        default="data/raw",
        help="Directory containing the extracted Olist CSV files",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    dataset_id = os.environ.get("BQ_RAW_DATASET", "olist_raw")

    client = get_client()
    ensure_dataset(client, dataset_id)

    missing = []
    for filename, table_name in FILE_TABLE_MAP.items():
        csv_path = data_dir / filename
        if not csv_path.exists():
            missing.append(filename)
            continue
        load_csv_to_bq(client, csv_path, dataset_id, table_name)

    if missing:
        logger.warning(
            "Skipped %d missing file(s): %s. Double check your Kaggle download.",
            len(missing),
            ", ".join(missing),
        )

    logger.info("Ingestion complete.")


if __name__ == "__main__":
    main()
