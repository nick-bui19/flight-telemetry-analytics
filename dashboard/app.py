"""Real-Time Flight Telemetry Dashboard."""

from __future__ import annotations

import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
_PG_HOST     = os.environ.get("POSTGRES_HOST", "localhost")
_PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")
_PG_DB       = os.environ.get("POSTGRES_DB", "flight_data")
_PG_USER     = os.environ.get("POSTGRES_USER", "nickbui")
_PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dummy")

_ENGINE = sqlalchemy.create_engine(
    f"postgresql+psycopg2://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}",
    pool_pre_ping=True,
)

REFRESH_INTERVAL = 15  # seconds — matches live map cache TTL


def _conn():
    return _ENGINE.connect()


# ── Queries (all SQL-side aggregation, all with LIMIT) ────────────────────────

@st.cache_data(ttl=15)
def get_live_map_data() -> pd.DataFrame:
    sql = """
        SELECT icao24, callsign, lat, lon, altitude_m, velocity_mps, heading_deg, updated_at_utc
        FROM realtime_flight_state
        WHERE updated_at_utc >= NOW() - INTERVAL '5 minutes'
          AND lat IS NOT NULL AND lon IS NOT NULL
    """
    try:
        with _conn() as conn:
            return pd.read_sql_query(sql, conn)
    except Exception as exc:
        st.error(f"DB error (live map): {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_leaderboards() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sql_speed = """
        SELECT icao24, callsign, velocity_mps
        FROM realtime_flight_state
        ORDER BY velocity_mps DESC NULLS LAST
        LIMIT 10
    """
    sql_alt = """
        SELECT icao24, callsign, altitude_m
        FROM realtime_flight_state
        ORDER BY altitude_m DESC NULLS LAST
        LIMIT 10
    """
    sql_vrate = """
        SELECT icao24, callsign, vertical_rate_mps
        FROM realtime_flight_state
        ORDER BY vertical_rate_mps DESC NULLS LAST
        LIMIT 10
    """
    try:
        with _conn() as conn:
            df_speed = pd.read_sql_query(sql_speed, conn)
            df_alt   = pd.read_sql_query(sql_alt,   conn)
            df_vrate = pd.read_sql_query(sql_vrate, conn)
        return df_speed, df_alt, df_vrate
    except Exception as exc:
        st.error(f"DB error (leaderboards): {exc}")
        empty = pd.DataFrame()
        return empty, empty, empty


@st.cache_data(ttl=30)
def get_heatmap_data() -> pd.DataFrame:
    sql = """
        SELECT grid_cell, aircraft_count, avg_altitude_m
        FROM realtime_airspace_grid_5m
        WHERE window_end = (SELECT MAX(window_end) FROM realtime_airspace_grid_5m)
    """
    try:
        with _conn() as conn:
            return pd.read_sql_query(sql, conn)
    except Exception as exc:
        st.error(f"DB error (heatmap): {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_quality_data() -> pd.DataFrame:
    sql = """
        SELECT * FROM telemetry_quality_5m
        ORDER BY window_end DESC
        LIMIT 12
    """
    try:
        with _conn() as conn:
            return pd.read_sql_query(sql, conn)
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
    st.sidebar.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")

    # ── Section 1: Live Map ───────────────────────────────────────────────────
    st.subheader("Live Flight Map")
    map_df = get_live_map_data()

    if map_df.empty:
        st.info("No active flights in the last 5 minutes. Ensure the pipeline is running.")
    else:
        st.caption(f"{len(map_df)} aircraft tracked")
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
        # Parse grid_cell "lat_lon" → numeric columns for scatter
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
        # Summary metrics from most recent window
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
