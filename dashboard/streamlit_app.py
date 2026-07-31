"""
Simple Streamlit dashboard for the repeat-purchase prediction pipeline.

Run with:
    streamlit run dashboard/streamlit_app.py
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

st.set_page_config(page_title="Repeat-Purchase Dashboard", layout="wide")


@st.cache_data(ttl=3600)
def load_predictions() -> pd.DataFrame:
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get(
        "BQ_PREDICTIONS_DATASET", os.environ.get("BQ_MARTS_DATASET", "olist_marts")
    )
    client = bigquery.Client(project=project_id)
    query = f"""
        select *
        from `{project_id}.{dataset}.customer_repeat_purchase_scores`
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


st.title("Customer Repeat-Purchase Dashboard")

df = load_predictions()

col1, col2, col3 = st.columns(3)
col1.metric("Customers scored", f"{len(df):,}")
col2.metric("Avg. repeat probability", f"{df['repeat_purchase_probability'].mean():.1%}")
col3.metric(
    "High-likelihood customers (>50%)",
    f"{(df['repeat_purchase_probability'] > 0.5).sum():,}",
)

st.subheader("Repeat-purchase probability distribution")
fig = px.histogram(df, x="repeat_purchase_probability", nbins=30)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Average probability by state")
by_state = (
    df.groupby("customer_state")["repeat_purchase_probability"]
    .mean()
    .reset_index()
    .sort_values("repeat_purchase_probability", ascending=False)
)
fig2 = px.bar(by_state, x="customer_state", y="repeat_purchase_probability")
st.plotly_chart(fig2, use_container_width=True)

st.subheader("Customer-level scores")
st.dataframe(
    df.sort_values("repeat_purchase_probability", ascending=False).head(200),
    use_container_width=True,
)
