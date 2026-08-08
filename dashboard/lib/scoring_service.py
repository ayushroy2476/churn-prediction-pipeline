"""
Business logic for turning a raw model probability into a labeled tier.

Kept free of Streamlit, pandas, and BigQuery imports on purpose -- this is
the one module in the project that's pure and deterministic, which is
exactly what makes it worth actually unit testing
(see tests/test_scoring_service.py) instead of only eyeballing it in the
browser.
"""

LOW_THRESHOLD = 0.35
HIGH_THRESHOLD = 0.68
# Rough cuts, not tuned against a real precision/recall tradeoff yet --
# revisit once there's enough scored history to backtest against actual
# repeat-purchase outcomes.

TIER_LOW = f"Low (<{LOW_THRESHOLD:.0%})"
TIER_MID = f"Medium ({LOW_THRESHOLD:.0%}-{HIGH_THRESHOLD:.0%})"
TIER_HIGH = f"High (>={HIGH_THRESHOLD:.0%})"
TIER_ORDER = [TIER_LOW, TIER_MID, TIER_HIGH]


def likelihood_tier(probability: float) -> str:
    """Bucket a repeat-purchase probability into a human-readable tier."""
    if probability >= HIGH_THRESHOLD:
        return TIER_HIGH
    if probability >= LOW_THRESHOLD:
        return TIER_MID
    return TIER_LOW