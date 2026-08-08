"""
Visual polish: hides Streamlit's default chrome and applies the project's
accent color. Reads style.css from ../assets so the CSS can be tweaked
without touching application code.
"""

from pathlib import Path

import streamlit as st

ACCENT = "#0F9D8C"  # placeholder teal -- swap for real brand colors
TIER_COLORS = ["#C9E4DF", "#5FBFAF", ACCENT]  # light -> accent as likelihood goes up

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"


def inject_custom_css() -> None:
    css_path = ASSETS_DIR / "style.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)