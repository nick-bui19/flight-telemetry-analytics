"""Real-Time Flight Telemetry Dashboard."""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
DEMO_MODE = os.environ.get("DEMO_MODE", "snapshot")  # "snapshot" | "postgres"

_PG_HOST     = os.environ.get("POSTGRES_HOST", "localhost")
_PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")
_PG_DB       = os.environ.get("POSTGRES_DB", "flight_data")
_PG_USER     = os.environ.get("POSTGRES_USER", "nickbui")
_PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dummy")

REFRESH_INTERVAL = 15  # seconds — matches live map cache TTL


def _conn():
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}",
        pool_pre_ping=True,
    )
    return engine.connect()


def _load_table(name: str, _snapshot_dir: Path | None = None) -> pd.DataFrame:
    """Load a table from parquet snapshot or live Postgres depending on DEMO_MODE.

    _snapshot_dir is for testing only; pass tmp_path to override default location.
    """
    if os.environ.get("DEMO_MODE", "snapshot") == "snapshot":
        snap_dir = _snapshot_dir or (Path(__file__).parent / "data")
        return pd.read_parquet(snap_dir / f"{name}.parquet")
    with _conn() as conn:
        import sqlalchemy
        return pd.read_sql_table(name, conn)


# ── Queries ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=15)
def get_live_map_data() -> pd.DataFrame:
    try:
        df = _load_table("realtime_flight_state")
        df = df[df["lat"].notna() & df["lon"].notna()]
        # Time filter only in postgres mode — snapshot returns all rows
        if os.environ.get("DEMO_MODE", "snapshot") != "snapshot":
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(minutes=5)
            df = df[pd.to_datetime(df["updated_at_utc"], utc=True) >= cutoff]
        return df[["icao24", "callsign", "lat", "lon", "altitude_m", "velocity_mps", "heading_deg", "updated_at_utc"]]
    except Exception as exc:
        st.error(f"DB error (live map): {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_leaderboards() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        df = _load_table("realtime_flight_state")
        df_speed = df[["icao24", "callsign", "velocity_mps"]].sort_values("velocity_mps", ascending=False, na_position="last").head(10)
        df_alt   = df[["icao24", "callsign", "altitude_m"]].sort_values("altitude_m", ascending=False, na_position="last").head(10)
        df_vrate = df[["icao24", "callsign", "vertical_rate_mps"]].sort_values("vertical_rate_mps", ascending=False, na_position="last").head(10)
        return df_speed, df_alt, df_vrate
    except Exception as exc:
        st.error(f"DB error (leaderboards): {exc}")
        empty = pd.DataFrame()
        return empty, empty, empty


@st.cache_data(ttl=30)
def get_heatmap_data() -> pd.DataFrame:
    try:
        df = _load_table("realtime_airspace_grid_5m")
        latest = df["window_end"].max()
        return df[df["window_end"] == latest][["grid_cell", "aircraft_count", "avg_altitude_m"]]
    except Exception as exc:
        st.error(f"DB error (heatmap): {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_quality_data() -> pd.DataFrame:
    try:
        df = _load_table("telemetry_quality_5m")
        return df.sort_values("window_end", ascending=False).head(12)
    except Exception as exc:
        st.error(f"DB error (quality): {exc}")
        return pd.DataFrame()


# ── Dashboard ─────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Flight Telemetry Dashboard",
        page_icon="✈",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("✈ Real-Time Flight Telemetry Dashboard")
    st.caption("Live monitoring of flight positions, speeds, and airspace density")

    # Sidebar
    st.sidebar.header("Controls")
    auto_refresh = st.sidebar.checkbox("Auto-refresh (15s)", value=True)

    # Fetch early — cached, so safe to call once here and reuse below
    map_df = get_live_map_data()

    # ── Top KPIs ──────────────────────────────────────────────────────────────
    kpi_left, kpi_right = st.columns(2)
    with kpi_left:
        st.metric("Tracked aircraft", len(map_df))
    with kpi_right:
        if not map_df.empty and "updated_at_utc" in map_df.columns:
            last_ts = pd.to_datetime(map_df["updated_at_utc"], utc=True).max()
            st.metric("Last data update", last_ts.strftime("%H:%M:%S UTC"))
        else:
            st.metric("Last data update", datetime.now().strftime("%H:%M:%S UTC"))

    st.divider()

    # ── Section 1: Live Map ───────────────────────────────────────────────────
    st.subheader("Live Flight Map")

    if map_df.empty:
        st.info("No active flights in the last 5 minutes. Ensure the pipeline is running.")
    else:
        fig_map = px.scatter_geo(
            map_df,
            lat="lat",
            lon="lon",
            hover_name="callsign",
            hover_data={"icao24": True, "altitude_m": True, "velocity_mps": True, "heading_deg": True},
            color="altitude_m",
            color_continuous_scale="Viridis",
            labels={"altitude_m": "Altitude (m)"},
            scope="usa",
            title="Active Flights (last 5 min)",
            height=450,
        )
        fig_map.update_traces(marker=dict(size=8))
        st.plotly_chart(fig_map, use_container_width=True)

    st.divider()

    # ── Section 2: Leaderboards ───────────────────────────────────────────────
    st.subheader("Leaderboards")
    df_speed, df_alt, df_vrate = get_leaderboards()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Fastest (m/s)**")
        if not df_speed.empty:
            st.dataframe(df_speed, use_container_width=True, hide_index=True)
        else:
            st.info("No data")

    with col2:
        st.markdown("**Highest altitude (m)**")
        if not df_alt.empty:
            st.dataframe(df_alt, use_container_width=True, hide_index=True)
        else:
            st.info("No data")

    with col3:
        st.markdown("**Top climb rate (m/s)**")
        if not df_vrate.empty:
            st.dataframe(df_vrate, use_container_width=True, hide_index=True)
        else:
            st.info("No data")

    st.divider()

    # ── Section 3: Airspace Density Heatmap ──────────────────────────────────
    st.subheader("Airspace Density (latest 5-min window)")
    heatmap_df = get_heatmap_data()

    if heatmap_df.empty:
        st.info("No airspace grid data yet. Spark aggregations may still be initializing.")
    else:
        try:
            heatmap_df[["grid_lat", "grid_lon"]] = heatmap_df["grid_cell"].str.split("_", expand=True).astype(float)
            fig_heat = px.scatter_geo(
                heatmap_df,
                lat="grid_lat",
                lon="grid_lon",
                size="aircraft_count",
                color="aircraft_count",
                color_continuous_scale="Reds",
                hover_data={"grid_cell": True, "avg_altitude_m": True},
                scope="usa",
                title="Aircraft density by grid cell",
                height=400,
            )
            st.plotly_chart(fig_heat, use_container_width=True)
        except Exception:
            st.dataframe(heatmap_df, use_container_width=True)

    st.divider()

    # ── Section 4: Data Quality Panel ────────────────────────────────────────
    st.subheader("Telemetry Quality (5-min windows)")
    quality_df = get_quality_data()

    if quality_df.empty:
        st.info("No quality metrics yet.")
    else:
        latest = quality_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total messages", int(latest.get("total_messages", 0)))
        c2.metric("With position", int(latest.get("messages_with_position", 0)))
        c3.metric("Missing position", int(latest.get("messages_missing_position", 0)))
        avg_lag = latest.get("avg_event_lag_seconds")
        c4.metric("Avg event lag", f"{avg_lag:.1f}s" if avg_lag is not None else "—")

        with st.expander("Quality history"):
            st.dataframe(quality_df, use_container_width=True, hide_index=True)

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    if auto_refresh:
        time.sleep(REFRESH_INTERVAL)
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
