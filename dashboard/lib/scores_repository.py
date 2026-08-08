"""
Data access for the repeat-purchase scores table. This is the only module
that should know BigQuery exists -- everything downstream of
`load_predictions` just works with a plain DataFrame.
"""

import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery

TABLE_NAME = "customer_repeat_purchase_scores"


@st.cache_data(ttl=3600, show_spinner="Loading predictions from BigQuery...")
def load_predictions() -> pd.DataFrame:
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get(
        "BQ_PREDICTIONS_DATASET", os.environ.get("BQ_MARTS_DATASET", "olist_marts")
    )
    client = bigquery.Client(project=project_id)
    query = f"select * from `{project_id}.{dataset}.{TABLE_NAME}`"
    # `select *` is fine for now, but once the mart schema is settled, trim
    # this to the columns the dashboard actually uses -- BigQuery bills by
    # columns scanned, so there's no reason to pay for feature columns
    # nobody's looking at here.
    return client.query(query).to_dataframe(create_bqstorage_client=True)