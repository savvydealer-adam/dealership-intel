"""Enrich tab - Process dealers from xlsx through crawl + enrichment pipeline."""

import logging
import time
from pathlib import Path

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Enrich - DealershipIntel", page_icon="🔍", layout="wide")
st.title("Enrich Dealers (xlsx)")
st.markdown("Upload a dealer list (xlsx/csv) and enrich with website crawling + Apollo fallback.")


def _get_browser_manager():
    from config.settings import get_settings
    from crawlers.browser import BrowserManager

    settings = get_settings()
    return BrowserManager(
        headless=settings.browser_headless,
        max_pages=settings.browser_max_pages,
        chromium_path=settings.chromium_path,
    )


def _get_apollo_service():
    from services.apollo_api import ApolloAPIService

    api_key = st.session_state.get("apollo_api_key")
    if not api_key:
        from config.settings import get_settings

        api_key = get_settings().apollo_api_key
    if api_key:
        return ApolloAPIService(api_key)
    return None


def _get_db_service():
    from services.database_service import DatabaseService

    db_url = st.session_state.get("database_url")
    if not db_url:
        from config.settings import get_settings

        db_url = get_settings().database_url
    if db_url:
        try:
            return DatabaseService(db_url)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
    return None


# --- File Upload ---
st.header("1. Load Dealer List")

uploaded_file = st.file_uploader("Upload xlsx or csv file:", type=["xlsx", "xls", "csv"])

if uploaded_file:
    # Save to temp location
    temp_path = Path(f"/tmp/dealership_intel_{uploaded_file.name}")
    temp_path.write_bytes(uploaded_file.getvalue())

    try:
        from pipeline.enrichment_pipeline import EnrichmentPipeline

        enrichment = EnrichmentPipeline()
        dealers_df = enrichment.load_dealers(str(temp_path))
        st.session_state.enrichment_df = dealers_df
        st.session_state.enrichment_path = str(temp_path)
        st.success(f"Loaded {len(dealers_df)} dealers.")
    except Exception as e:
        st.error(f"Failed to load file: {e}")

if "enrichment_df" in st.session_state:
    dealers_df = st.session_state.enrichment_df

    # Show preview
    st.dataframe(dealers_df.head(10), use_container_width=True)

    # Show summaries
    enrichment = EnrichmentPipeline()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("By Provider")
        provider_counts = enrichment.get_provider_summary(dealers_df)
        for provider, count in sorted(provider_counts.items(), key=lambda x: -x[1])[:15]:
            st.text(f"  {provider}: {count}")

    with col2:
        st.subheader("By State")
        state_counts = enrichment.get_state_summary(dealers_df)
        for state, count in sorted(state_counts.items(), key=lambda x: -x[1])[:15]:
            st.text(f"  {state}: {count}")

    # --- Filters ---
    st.header("2. Configure Processing")

    fcol1, fcol2, fcol3 = st.columns(3)

    with fcol1:
        provider_options = ["All"] + sorted(provider_counts.keys())
        provider_filter = st.selectbox("Filter by provider:", provider_options)
        if provider_filter == "All":
            provider_filter = None

    with fcol2:
        state_options = ["All"] + sorted(state_counts.keys())
        state_filter = st.selectbox("Filter by state:", state_options)
        if state_filter == "All":
            state_filter = None

    with fcol3:
        max_dealers = st.number_input("Max dealers (0 = all):", min_value=0, value=0, step=10)
        if max_dealers == 0:
            max_dealers = None

    ocol1, ocol2, ocol3 = st.columns(3)

    with ocol1:
        delay_seconds = st.number_input(
            "Delay between requests (s):", min_value=0.5, max_value=10.0, value=2.0, step=0.5
        )

    with ocol2:
        skip_existing = st.checkbox("Skip already-enriched dealers", value=True)

    with ocol3:
        enable_crawling = st.checkbox("Enable website crawling", value=True)

    run_name = st.text_input("Run name:", value=f"Enrichment - {time.strftime('%Y-%m-%d %H:%M')}")

    # --- Execute ---
    st.header("3. Run Enrichment")

    if st.button("Start Enrichment", type="primary", use_container_width=True):
        db = _get_db_service()
        apollo = _get_apollo_service()
        browser_manager = _get_browser_manager() if enable_crawling else None

        if not apollo and not enable_crawling:
            st.error("Either Apollo API or website crawling must be enabled.")
        else:
            from pipeline.enrichment_pipeline import EnrichmentPipeline

            enrichment = EnrichmentPipeline(
                db_service=db,
                browser_manager=browser_manager,
                apollo_service=apollo,
            )

            progress_bar = st.progress(0)
            status_text = st.empty()
            results_container = st.empty()

            def on_progress(current: int, total: int, message: str):
                progress_bar.progress(current / total if total > 0 else 0)
                status_text.text(f"[{current}/{total}] {message}")

            with st.spinner("Enriching dealers..."):
                try:
                    results = enrichment.process_batch(
                        dealers_df,
                        provider_filter=provider_filter,
                        state_filter=state_filter,
                        max_dealers=max_dealers,
                        skip_existing=skip_existing,
                        delay_seconds=delay_seconds,
                        run_name=run_name,
                        on_progress=on_progress,
                    )
                except Exception as e:
                    st.error(f"Enrichment failed: {e}")
                    results = []

            if results:
                results_df = pd.DataFrame(results)
                st.session_state.enrichment_results = results_df

                status_text.text("Enrichment completed!")

                # Summary metrics
                successful = sum(1 for r in results if r.get("status") == "Success")
                partial = sum(1 for r in results if r.get("status") == "Partial")
                failed = sum(1 for r in results if r.get("status") not in ("Success", "Partial"))
                contacts = sum(len(r.get("contacts", [])) for r in results)

                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Total", len(results))
                m2.metric("Successful", successful)
                m3.metric("Partial", partial)
                m4.metric("Failed", failed)
                m5.metric("Contacts Found", contacts)

                results_container.dataframe(results_df, use_container_width=True)

                # Export button
                st.header("4. Export Results")
                export_format = st.radio("Export format:", ["xlsx", "csv"], horizontal=True)
                export_name = f"enriched_dealers_{time.strftime('%Y%m%d_%H%M')}.{export_format}"

                if st.button("Export Results"):
                    try:
                        export_path = f"/tmp/{export_name}"
                        enrichment.export_results(results, export_path)

                        with open(export_path, "rb") as f:
                            st.download_button(
                                label=f"Download {export_name}",
                                data=f.read(),
                                file_name=export_name,
                                mime="application/octet-stream",
                            )
                        st.success(f"Ready to download: {export_name}")
                    except Exception as e:
                        st.error(f"Export failed: {e}")
            else:
                st.warning("No results were generated.")

            # Clean up browser
            if browser_manager:
                import asyncio

                try:
                    asyncio.run(browser_manager.close())
                except Exception:
                    pass
