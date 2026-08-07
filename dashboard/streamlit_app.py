"""
Streamlit dashboard for the repeat-purchase prediction pipeline.

Reads the scored-customer table produced by the `score_customers` task of
the daily `churn_prediction_pipeline` Airflow DAG, and gives the CRM /
marketing team a way to explore, filter, and export the results.

Run with:
    streamlit run dashboard/streamlit_app.py

Required env vars (see .env.example):
    GCP_PROJECT_ID          -- BigQuery project
    BQ_PREDICTIONS_DATASET  -- dataset holding customer_repeat_purchase_scores
                                (falls back to BQ_MARTS_DATASET, then "olist_marts")
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

st.set_page_config(page_title="Repeat-Purchase Dashboard", layout="wide")

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
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def risk_tier(p: float) -> str:
    if p >= 0.66:
        return "High (>=66%)"
    if p >= 0.33:
        return "Medium (33-66%)"
    return "Low (<33%)"


st.title("Customer Repeat-Purchase Dashboard")

if "GCP_PROJECT_ID" not in os.environ:
    st.error(
        "Missing required environment variable GCP_PROJECT_ID. Set it in "
        "your `.env` file (see .env.example) or the shell before running "
        "Streamlit."
    )
    st.stop()

try:
    df = load_predictions()
except Exception as e:
    st.error(f"Couldn't load predictions from BigQuery: {e}")
    st.stop()

if df.empty:
    st.warning(
        "No scored customers found. Has today's `churn_prediction_pipeline` "
        "DAG run finished yet?"
    )
    st.stop()

df["risk_tier"] = df["repeat_purchase_probability"].apply(risk_tier)

# ------------------------------------------------------------------ header

header_left, header_right = st.columns([4, 1])
with header_left:
    if "score_date" in df.columns:
        as_of = pd.to_datetime(df["score_date"]).max().date()
        st.caption(f"Data as of {as_of}")
    st.caption(
        "Estimated likelihood a customer places another order, from the "
        "repeat-purchase model. These are estimates, not guarantees -- use "
        "them to prioritize outreach, not as a sole targeting rule."
    )
with header_right:
    if st.button("Refresh data", use_container_width=True):
        load_predictions.clear()
        st.rerun()

# ----------------------------------------------------------------- filters. 

st.sidebar.header("Filters")

states = sorted(df["customer_state"].dropna().unique().tolist())
selected_states = st.sidebar.multiselect("Customer state", states, default=states)

prob_min, prob_max = st.sidebar.slider(
    "Repeat-purchase probability range",
    min_value=0.0,
    max_value=1.0,
    value=(0.0, 1.0),
    step=0.01,
)

filtered = df[
    df["customer_state"].isin(selected_states)
    & df["repeat_purchase_probability"].between(prob_min, prob_max)
]

if filtered.empty:
    st.warning("No customers match the current filters.")
    st.stop()

# ------------------------------------------------------------------- KPIs

col1, col2, col3, col4 = st.columns(4)
col1.metric("Customers scored", f"{len(filtered):,}")
col2.metric(
    "Avg. repeat probability", f"{filtered['repeat_purchase_probability'].mean():.1%}"
)
col3.metric(
    "High-likelihood (>=66%)",
    f"{(filtered['repeat_purchase_probability'] >= 0.66).sum():,}",
)
col4.metric(
    "Low-likelihood (<33%)",
    f"{(filtered['repeat_purchase_probability'] < 0.33).sum():,}", 
)

overview_tab, explorer_tab = st.tabs(["Overview", "Customer explorer"])

with overview_tab:
    st.subheader("Repeat-purchase probability distribution")
    fig = px.histogram(filtered, x="repeat_purchase_probability", nbins=30)
    fig.update_layout(xaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.subheader("Average probability by state")
        by_state = (
            filtered.groupby("customer_state")["repeat_purchase_probability"]
            .mean()
            .reset_index()
            .sort_values("repeat_purchase_probability", ascending=False)
        )
        fig2 = px.bar(by_state, x="customer_state", y="repeat_purchase_probability")
        fig2.update_layout(yaxis_tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

    with right:
        st.subheader("Customers by risk tier")
        tier_order = ["Low (<33%)", "Medium (33-66%)", "High (>=66%)"]
        tier_counts = (
            filtered["risk_tier"]
            .value_counts()
            .reindex(tier_order)
            .fillna(0)
            .reset_index()
        )
        tier_counts.columns = ["risk_tier", "customers"]
        fig3 = px.bar(tier_counts, x="risk_tier", y="customers")
        st.plotly_chart(fig3, use_container_width=True)

with explorer_tab:
    st.subheader("Customer-level scores")

    if "customer_id" in filtered.columns:
        search = st.text_input("Search by customer ID")
        table = (
            filtered[
                filtered["customer_id"].astype(str).str.contains(search, case=False)
            ]
            if search
            else filtered
        )
    else:
        table = filtered

    st.caption(f"{len(table):,} customers shown")
    st.dataframe(
        table.sort_values("repeat_purchase_probability", ascending=False),
        use_container_width=True,
        column_config={
            "repeat_purchase_probability": st.column_config.ProgressColumn(
                "Repeat-purchase probability",
                min_value=0.0,
                max_value=1.0,
            )
        },
    )

    st.download_button(
        "Download shown rows as CSV",
        data=table.to_csv(index=False).encode("utf-8"),
        file_name="repeat_purchase_scores.csv",
        mime="text/csv",
    )