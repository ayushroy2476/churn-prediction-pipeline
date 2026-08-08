"""
Streamlit dashboard for the repeat-purchase prediction pipeline.

Reads the scored-customer table produced by the `score_customers` task of
the daily `churn_prediction_pipeline` Airflow DAG, and gives the CRM /
marketing team a way to explore, filter, and export the results.

Run from the project root with:
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

from lib.scores_repository import load_predictions
from lib.scoring_service import HIGH_THRESHOLD, LOW_THRESHOLD, TIER_ORDER, likelihood_tier
from lib.styling import ACCENT, TIER_COLORS, inject_custom_css

load_dotenv()

st.set_page_config(
    page_title="Olist \u00b7 Repeat-Purchase Dashboard",
    page_icon="\U0001F6D2",
    layout="wide",
)
inject_custom_css()

st.caption("OLIST DATA PLATFORM")
st.title("Repeat-Purchase Dashboard")

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

df["likelihood_tier"] = df["repeat_purchase_probability"].apply(likelihood_tier)

meta_col, refresh_col = st.columns([5, 1])
with meta_col:
    if "score_date" in df.columns:
        as_of = pd.to_datetime(df["score_date"]).max().date()
        st.caption(f"Data as of {as_of}")
with refresh_col:
    st.write("")  # cheap vertical nudge so the button lines up with the caption
    if st.button("Refresh data", use_container_width=True):
        load_predictions.clear()
        st.rerun()

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

col1, col2, col3, col4 = st.columns(4)
col1.metric("Customers scored", f"{len(filtered):,}")
col2.metric(
    "Avg. repeat probability",
    f"{filtered['repeat_purchase_probability'].mean():.1%}",
    help="Model estimate, not a guarantee -- use it to prioritize outreach, "
    "not as a sole targeting rule.",
)
col3.metric(
    f"High-likelihood (>={HIGH_THRESHOLD:.0%})",
    f"{(filtered['repeat_purchase_probability'] >= HIGH_THRESHOLD).sum():,}",
)
col4.metric(
    f"Low-likelihood (<{LOW_THRESHOLD:.0%})",
    f"{(filtered['repeat_purchase_probability'] < LOW_THRESHOLD).sum():,}",
)

overview_tab, explorer_tab = st.tabs(["Overview", "Customer explorer"])

with overview_tab:
    st.subheader("Repeat-purchase probability distribution")
    fig = px.histogram(
        filtered,
        x="repeat_purchase_probability",
        nbins=30,
        color_discrete_sequence=[ACCENT],
    )
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
        fig2 = px.bar(
            by_state,
            x="customer_state",
            y="repeat_purchase_probability",
            color_discrete_sequence=[ACCENT],
        )
        fig2.update_layout(yaxis_tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

    with right:
        st.subheader("Customers by likelihood tier")
        tier_counts = (
            filtered["likelihood_tier"]
            .value_counts()
            .reindex(TIER_ORDER)
            .fillna(0)
            .reset_index()
        )
        tier_counts.columns = ["likelihood_tier", "customers"]
        fig3 = px.bar(
            tier_counts,
            x="likelihood_tier",
            y="customers",
            color="likelihood_tier",
            color_discrete_sequence=TIER_COLORS,
            category_orders={"likelihood_tier": TIER_ORDER},
        )
        fig3.update_layout(showlegend=False)
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

st.divider()
st.caption(
    "Refreshes daily via the `churn_prediction_pipeline` Airflow DAG \u00b7 "
    "owned by data-eng \u00b7 questions in #data-eng"
)