"""Autotrader Scrape Monitor - Live dashboard for the background scraper."""

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Autotrader Scrape Monitor", page_icon="🚗", layout="wide")

CSV_PATH = Path("C:/Users/adam/Downloads/autotrader_dealers.csv")
PROGRESS_PATH = Path("C:/Users/adam/Downloads/scrape_progress.json")


def _load_progress() -> dict | None:
    """Load the progress JSON written by run_autotrader_scrape.py."""
    if not PROGRESS_PATH.exists():
        return None
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _load_csv() -> pd.DataFrame | None:
    """Load the CSV of scraped dealers."""
    if not CSV_PATH.exists():
        return None
    try:
        df = pd.read_csv(CSV_PATH, dtype=str)
        if df.empty:
            return None
        return df
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Autotrader Scrape Monitor")

progress = _load_progress()
df = _load_csv()

# ---------------------------------------------------------------------------
# Status card + progress bar
# ---------------------------------------------------------------------------
if progress:
    status = progress.get("status", "unknown")
    total = progress.get("total", 0)
    processed = progress.get("processed", 0)

    if status == "running":
        st.info(f"Scrape is **running** -- {processed:,} / {total:,} processed")
    elif status == "complete":
        st.success(f"Scrape **complete** -- {processed:,} / {total:,} processed")
    else:
        st.warning(f"Status: {status}")

    if total > 0:
        st.progress(min(processed / total, 1.0))

    # Metrics row
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Saved", f"{progress.get('saved', 0):,}")
    c2.metric("Failed", f"{progress.get('failed', 0):,}")
    c3.metric("Skipped", f"{progress.get('skipped', 0):,}")
    c4.metric("Rate", f"{progress.get('rate_per_min', 0):.0f}/min")
    c5.metric("Elapsed", f"{progress.get('elapsed_min', 0):.1f} min")
    c6.metric("ETA", f"{progress.get('eta_min', 0):.0f} min")

    st.caption(f"Last updated: {progress.get('last_updated', 'N/A')}")
    st.divider()
else:
    st.warning(
        "No scrape progress found. Start the scraper with:\n\n"
        "```\ncd C:/Users/adam/dealership-intel\n"
        "python run_autotrader_scrape.py\n```"
    )

# ---------------------------------------------------------------------------
# CSV data sections
# ---------------------------------------------------------------------------
if df is not None:
    row_count = len(df)
    st.header(f"Scraped Dealers ({row_count:,} rows)")

    # State breakdown
    if "state" in df.columns:
        st.subheader("By State")
        state_counts = (
            df["state"]
            .fillna("Unknown")
            .value_counts()
            .reset_index()
        )
        state_counts.columns = ["State", "Count"]

        col_chart, col_table = st.columns([2, 1])
        with col_chart:
            st.bar_chart(state_counts.set_index("State").head(25))
        with col_table:
            st.dataframe(state_counts, use_container_width=True, height=400)

    # Recent results
    st.subheader("Recent Results (last 50)")
    display_cols = [
        "name", "city", "state", "phone",
        "website_url", "rating", "review_count", "inventory_count",
    ]
    available = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available].tail(50).iloc[::-1], use_container_width=True)

    # Full CSV download
    st.divider()
    csv_bytes = CSV_PATH.read_bytes()
    st.download_button(
        "Download Full CSV",
        csv_bytes,
        file_name="autotrader_dealers.csv",
        mime="text/csv",
        use_container_width=True,
    )
else:
    if progress is None:
        st.info("No CSV data yet. The scraper hasn't been run.")

# ---------------------------------------------------------------------------
# Auto-refresh while scrape is running
# ---------------------------------------------------------------------------
if progress and progress.get("status") == "running":
    time.sleep(5)
    st.rerun()
